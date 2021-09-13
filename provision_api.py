import traceback
from flask import Flask, jsonify, make_response, request, Response
from flask_compress import Compress
import psycopg2
import pandas as pd, numpy as np
import argparse
import json
import requests
import time
import itertools
import os, threading
from multiprocessing import Pipe
from multiprocessing.connection import Connection
from typing import Any, Literal, Tuple, List, Dict, Optional, Union, NamedTuple

import calculate_services_cnt
import experimental_aggregation
from thread_pool import ThreadPool

class NonASCIIJSONEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        kwargs['ensure_ascii'] = False
        super().__init__(**kwargs)

class Properties:
    def __init__(
            self, provision_db_addr: str, provision_db_port: int, provision_db_name: str, provision_db_user: str, provision_db_pass: str,
            houses_db_addr: str, houses_db_port: int, houses_db_name: str, houses_db_user: str, houses_db_pass: str,
            api_port: int, transport_model_api_endpoint: str):
        self.provision_db_addr = provision_db_addr
        self.provision_db_port = provision_db_port
        self.provision_db_name = provision_db_name
        self.provision_db_user = provision_db_user
        self.provision_db_pass = provision_db_pass
        self.houses_db_addr = houses_db_addr
        self.houses_db_port = houses_db_port
        self.houses_db_name = houses_db_name
        self.houses_db_user = houses_db_user
        self.houses_db_pass = houses_db_pass
        self.api_port = api_port
        self.transport_model_api_endpoint = transport_model_api_endpoint
        self._houses_conn: Optional[psycopg2.extensions.connection] = None
        self._provision_conn: Optional[psycopg2.extensions.connection] = None
    @property
    def provision_conn_string(self) -> str:
        return f'host={self.provision_db_addr} port={self.provision_db_port} dbname={self.provision_db_name}' \
                f' user={self.provision_db_user} password={self.provision_db_pass}'
    @property
    def houses_conn_string(self) -> str:
        return f'host={self.houses_db_addr} port={self.houses_db_port} dbname={self.houses_db_name}' \
                f' user={self.houses_db_user} password={self.houses_db_pass}'
    @property
    def houses_conn(self) -> psycopg2.extensions.connection:
        if self._houses_conn is None or self._houses_conn.closed:
            self._houses_conn = psycopg2.connect(self.houses_conn_string)
        return self._houses_conn
            
    @property
    def provision_conn(self) -> psycopg2.extensions.connection:
        if self._provision_conn is None or self._provision_conn.closed:
            self._provision_conn = psycopg2.connect(self.provision_conn_string)
        return self._provision_conn

    def close(self):
        if self.houses_conn is not None:
            self._houses_conn.close()
        if self._provision_conn is not None:
            self._provision_conn.close()

class Avaliability:
    def get_walking(self, lat: float, lan: float, t: int, pipe: Optional[Connection] = None) -> str:
        if t == 0:
            ans = json.dumps({
                    'type': 'Polygon',
                    'coordinates': []
            })
            if pipe is not None:
                pipe.send(ans)
            return ans
        with properties.provision_conn.cursor() as cur:
            cur.execute(f'SELECT ST_AsGeoJSON(geometry) FROM walking WHERE latitude = {lat} AND longitude = {lan} AND time = {t}')
            res = cur.fetchall()
            if len(res) != 0:
                if pipe is not None:
                    pipe.send(res[0][0])
                return res[0][0]
            try:
                print(f'downloading walking for {lat}, {lan}, {t}')
                ans = json.dumps(
                    requests.get(f'https://galton.urbica.co/api/foot/?lng={lat}&lat={lan}&radius=5&cellSize=0.1&intervals={t}', timeout=15).json()['features'][0]['geometry']
                )
            except Exception:
                ans = json.dumps({'type': 'Polygon', 'coordinates': []})
                properties.provision_conn.rollback()
                if pipe is not None:
                    pipe.send(ans)
                return ans

            try:
                cur.execute(f"INSERT INTO walking (latitude, longitude, time, geometry) VALUES ({lat}, {lan}, {t}, ST_SetSRID(ST_GeomFromGeoJSON('{ans}'::text), 4326)) ON CONFLICT DO NOTHING")
                properties.provision_conn.commit()
            except Exception:
                properties.provision_conn.rollback()
                
        if pipe is not None:
            pipe.send(ans)
        return ans


    def get_transport(self, lat: float, lan: float, t: int, pipe: Optional[Connection] = None) -> str:
        if t == 0:
            res = json.dumps({
                'type': 'Polygon',
                'coordinates': []
            })
            if pipe is not None:
                pipe.send(res)
            return res
        with properties.provision_conn.cursor() as cur:
            cur.execute(f'SELECT ST_AsGeoJSON(geometry) FROM transport WHERE latitude = {lat} AND longitude = {lan} AND time = {t}')
            res = cur.fetchall()
            if len(res) != 0:
                if pipe is not None:
                    pipe.send(res[0][0])
                return res[0][0]
            if t >= 60:
                cur.execute(f'SELECT ST_AsGeoJSON(geometry) FROM transport WHERE time = {t} LIMIT 1')
                res = cur.fetchall()
                if len(res) != 0:
                    if pipe is not None:
                        pipe.send(res[0][0])
                    return res[0][0]
            print(f'downloading transport for {lat}, {lan}, {t}')
            try:
                data = requests.post(f'{properties.transport_model_api_endpoint}', timeout=15, json=
                    {
                        'source': [lan, lat],
                        'cost': t * 60,
                        'day_time': 46800,
                        'mode_type': 'pt_cost'
                    }
                ).json()
                if len(data['features']) == 0:
                    ans = json.dumps({'type': 'Polygon', 'coordinates': []})
                else:
                    cur.execute('SELECT ST_AsGeoJSON(ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')', data['features'])) + ']))')
                    ans = cur.fetchall()[0][0]
            except Exception:
                ans = json.dumps({'type': 'Polygon', 'coordinates': []})
                properties.provision_conn.rollback()
                if pipe is not None:
                    pipe.send(ans)
                return ans

            try:
                cur.execute('INSERT INTO transport (latitude, longitude, time, geometry) VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326)) ON CONFLICT DO NOTHING', (lat, lan, t, ans))
                properties.provision_conn.commit()
            except Exception:
                properties.provision_conn.rollback()
            if pipe is not None:
                pipe.send(ans)
        return ans

    def get_car(self, lat: float, lan: float, t: int, pipe: Optional[Connection] = None) -> str:
        if t == 0:
            res = json.dumps({
                'type': 'Polygon',
                'coordinates': []
            })
            if pipe is not None:
                pipe.send(res)
            return res
        with properties.provision_conn.cursor() as cur:
            cur.execute(f'SELECT ST_AsGeoJSON(geometry) FROM car WHERE latitude = {lat} AND longitude = {lan} AND time = {t}')
            res = cur.fetchall()
            if len(res) != 0:
                if pipe is not None:
                    pipe.send(res[0][0])
                return res[0][0]
            if t >= 60:
                cur.execute(f'SELECT ST_AsGeoJSON(geometry) FROM car WHERE time = {t} LIMIT 1')
                res = cur.fetchall()
                if len(res) != 0:
                    if pipe is not None:
                        pipe.send(res[0][0])
                    return res[0][0]
            print(f'downloading car for {lat}, {lan}, {t}')
            try:
                data = requests.post(f'{properties.transport_model_api_endpoint}', timeout=15, json=
                    {
                        'source': [lan, lat],
                        'cost': t * 60,
                        'day_time': 46800,
                        'mode_type': 'car_cost'
                    }
                ).json()
                if len(data['features']) == 0:
                    ans = json.dumps({'type': 'Polygon', 'coordinates': []})
                else:
                    cur.execute('SELECT ST_AsGeoJSON(ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')', data['features'])) + ']))')
                    ans = cur.fetchall()[0][0]
            except Exception:
                ans = json.dumps({'type': 'Polygon', 'coordinates': []})
                properties.provision_conn.rollback()
                if pipe is not None:
                    pipe.send(ans)
                return ans

            try:
                cur.execute('INSERT INTO car (latitude, longitude, time, geometry) VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326)) ON CONFLICT DO NOTHING', (lat, lan, t, ans))
                properties.provision_conn.commit()
            except Exception:
                properties.provision_conn.rollback()
        if pipe is not None:
            pipe.send(ans)
        return ans
    
    def ensure_ready(self, lat: float, lan: float, time_walking: int, time_transport: int, time_car: int) -> Tuple[str, str, str]:
        pipes = [Pipe() for _ in range(3)]
        threads = list(map(lambda func_and_t: threading.Thread(target=lambda: func_and_t[0](lat, lan, func_and_t[1], func_and_t[2])),
                ((self.get_walking, time_walking, pipes[0][0]), (self.get_transport, time_transport, pipes[1][0]), (self.get_car, time_car, pipes[2][0]))))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        
        return tuple([pipe[1].recv() for pipe in pipes]) # type: ignore

properties: Properties
avaliability: Avaliability

needs: pd.DataFrame
all_houses: pd.DataFrame
infrastructure: pd.DataFrame
services_buildings: pd.DataFrame
blocks: pd.DataFrame
city_hierarchy: pd.DataFrame

Listings = NamedTuple('Listings', [
    ('city_functions', pd.DataFrame),
    ('living_situations', pd.DataFrame),
    ('social_groups', pd.DataFrame)
])
listings: Listings

provision_districts: pd.DataFrame
provision_municipalities: pd.DataFrame
provision_blocks: pd.DataFrame

def compute_atomic_provision(social_group: str, living_situation: str, service_type: str, coords: Tuple[float, float],
        provision_conn: psycopg2.extensions.connection, houses_conn: psycopg2.extensions.connection, **kwargs) -> Dict[str, Any]:
    walking_time_cost: int
    transport_time_cost: int
    personal_transport_time_cost: int
    intensity: float
    significance: float
    walking_time_cost, transport_time_cost, personal_transport_time_cost, intensity, significance = \
            experimental_aggregation.get_needs(needs, infrastructure, social_group, living_situation, service_type)
    if walking_time_cost == 0 and transport_time_cost == 0 and personal_transport_time_cost == 0 and intensity == 0 and significance == 0:
        print(f'No data found for needs (social_group = {social_group}, living_situation = {living_situation}, service_type = {service_type})')
        raise Exception(f'No data found for needs (social_group = {social_group}, living_situation = {living_situation}, service_type = {service_type})')

    if 'walking_time_cost' in kwargs:
        walking_time_cost = int(kwargs['walking_time_cost'])
    if 'transport_time_cost' in kwargs:
        transport_time_cost = int(kwargs['transport_time_cost'])
    if 'personal_transport_time_cost' in kwargs:
        personal_transport_time_cost = int(kwargs['personal_transport_time_cost'])
    if 'intensity' in kwargs:
        intensity = int(kwargs['intensity'])
    if 'significance' in kwargs:
        significance = int(kwargs['significance'])
    walking_availability = float(kwargs.get('walking_availability', 1))
    public_transport_availability_multiplier = float(kwargs.get('public_transport_availability_multiplier', 1))
    personal_transport_availability_multiplier = float(kwargs.get('personal_transport_availability_multiplier', 0))
    max_target_s = float(kwargs.get('max_target_s', 30.0))
    target_s_divider = float(kwargs.get('target_s_divider', 6))
    coeff_multiplier = float(kwargs.get('coeff_multiplier', 5))

    if walking_time_cost == 0 and transport_time_cost == 0 and (personal_transport_time_cost == 0 or personal_transport_availability_multiplier == 0):
        return {
            'walking_geometry': json.dumps({'type': 'Polygon', 'coordinates': []}),
            'transport_geometry': json.dumps({'type': 'Polygon', 'coordinates': []}),
            'car_geometry': json.dumps({'type': 'Polygon', 'coordinates': []}),
            'service_types': dict(),
            'provision_result': 0.0,
            'parameters': {
                'walking_time_cost': walking_time_cost,
                'transport_time_cost': transport_time_cost,
                'personal_transport_time_cost': personal_transport_time_cost,
                'intensity': intensity,
                'significance': significance
            }
        }

    # Walking

    # walking_geometry = avaliability.get_walking(*coords, walking_time_cost)
    walking_geometry, transport_geometry, car_geometry = avaliability.ensure_ready(*coords, walking_time_cost, transport_time_cost, personal_transport_time_cost)

    df_target_servs = services_buildings[
            services_buildings['service_type_id'].isin(calculate_services_cnt.count_service(coords, service_type, walking_time_cost, 'walking', provision_conn, houses_conn))
            &
            (services_buildings['service_type'] == service_type)
    ]
    df_target_servs = df_target_servs.join(pd.Series(['walking'] * df_target_servs.shape[0], name='availability_type', dtype=str, index=df_target_servs.index))

    # public transport

    # transport_geometry = avaliability.get_transport(*coords, transport_time_cost)

    transport_servs = services_buildings[
            services_buildings['service_type_id'].isin(calculate_services_cnt.count_service(coords, service_type, transport_time_cost, 'transport', provision_conn, houses_conn))
            &
            (services_buildings['service_type'] == service_type)
    ]
    transport_servs = transport_servs.set_index('service_type_d').drop(df_target_servs['service_type_id'], errors='ignore').reset_index()
    transport_servs = transport_servs.join(pd.Series(['public_transport'] * transport_servs.shape[0], name='availability_type', dtype=str, index=transport_servs.index))

    df_target_servs = df_target_servs.append(transport_servs, ignore_index=True)
    del transport_servs

    # perosonal_transport (car)

    # car_geometry = avaliability.get_car(*coords, personal_transport_time_cost)
    
    car_servs = services_buildings[
            services_buildings['service_type_id'].isin(calculate_services_cnt.count_service(coords, service_type, personal_transport_time_cost, 'car', provision_conn, houses_conn))
            &
            (services_buildings['service_type'] == service_type)
    ]
    car_servs = car_servs.set_index('service_type_id').drop(df_target_servs['service_type_id'], errors='ignore').reset_index()
    car_servs = car_servs.join(pd.Series(['personal_transport'] * car_servs.shape[0], name='availability_type', dtype=str, index=car_servs.index))
    df_target_servs = df_target_servs.append(car_servs, ignore_index=True)
    del car_servs

    # Выполнить расчет атомарной обеспеченности
    # Задать начальное значение обеспеченности
    target_O = 0.0

    # Расчет выполняется при наличии точек оказания услуг на полигоне доступности, иначе обеспеченность - 0
    if not df_target_servs.empty:

        # Рассчитать доступность D услуг из целевого дома для целевой социальной группы
        # Если услуга расположена в пределах требуемой пешей доступности (на полигоне пешей доступности), то D = 1.
        # Если услуга расположена вне пешей доступности, но удовлетворяет требованиям транспортной доступности,
        # то D = 1/I (I - интенсивность использования типа услуги целевой социальной группой).

        # Рассчитать доступность D услуг
        df_target_servs['availability'] = np.where(df_target_servs['availability_type'] == 'walking', walking_availability,
                np.where(df_target_servs['availability_type'] == 'public_transport',
                        round(1 / intensity * public_transport_availability_multiplier, 2), round(1 / intensity * personal_transport_availability_multiplier, 2)))

        # Вычислить мощность S предложения по целевому типу услуги для целевой группы
        target_S = (df_target_servs['power'] * df_target_servs['availability']).sum()

        # Если рассчитанная мощность S > max_target_s, то S принимается равной max_target_s
        if target_S > max_target_s:
            target_S = max_target_s

        # Вычислить значение обеспеченности О
        if significance == 0.5:
            target_O = target_S / target_s_divider
        else:
            coeff = abs(significance - 0.5) * coeff_multiplier
            if significance > 0.5:
                target_O = ((target_S / target_s_divider) ** (coeff + 1)) / (5 ** coeff)
            else:
                target_O = 5 - ((5 - target_S / target_s_divider) ** (coeff + 1)) / (5 ** coeff)

    target_O = round(target_O, 2)

    return {
        'walking_geometry': json.loads(walking_geometry) if walking_geometry is not None else json.dumps({'type': 'Polygon', 'coordinates': []}),
        'transport_geometry': json.loads(transport_geometry) if transport_geometry is not None else json.dumps({'type': 'Polygon', 'coordinates': []}),
        'car_geometry': json.loads(car_geometry) if car_geometry is not None else json.dumps({'type': 'Polygon', 'coordinates': []}),
        'service_types': list(df_target_servs.transpose().to_dict().values()),
        'provision_result': target_O,
        'parameters': {
            'walking_time_cost': walking_time_cost,
            'transport_time_cost': transport_time_cost,
            'personal_transport_time_cost': personal_transport_time_cost,
            'intensity': intensity,
            'significance': significance
        }
    }


def get_aggregation(where: Union[str, Tuple[float, float]], where_type: str, social_group: Optional[str],
        living_situation: Optional[str], service_type: Optional[str], provision_conn: psycopg2.extensions.connection,
        houses_conn: psycopg2.extensions.connection, update: bool = False) -> Dict[str, Union[float, str]]:
    given_vaules = (social_group, living_situation, service_type)
    found_id: Optional[int] = None
    where_column = 'district' if where_type == 'districts' else 'municipality'

    with provision_conn.cursor() as cur_provision:
        if where_type in ('districts', 'municipalities'):
            cur_provision.execute(f'SELECT id, avg_intensity, avg_significance, avg_provision, time_done FROM aggregation_{where_column}'
                    ' WHERE social_group_id ' + ('= (SELECT id FROM social_groups WHERE name = %s)' if social_group is not None else 'is %s') +
                    ' AND living_situation_id ' + ('= (SELECT id FROM living_situations WHERE name = %s)' if living_situation is not None else 'is %s') +
                    ' AND service_type_id ' + ('= (SELECT id FROM service_types WHERE name = %s)' if service_type is not None else 'is %s') +
                    f' AND {where_column}_id = (SELECT id FROM {where_type} WHERE full_name = %s)',
                    (social_group, living_situation, service_type, where))
            cur_data = cur_provision.fetchall()
            if len(cur_data) != 0:
                id, intensity, significance, provision, done =  cur_data[0]
                if not update:
                    return {
                        'provision': provision,
                        'intensity': intensity,
                        'significance': significance,
                        'time_done': done
                    }
                else:
                    found_id = id
        elif where_type == 'house':
            cur_provision.execute('SELECT id, avg_intensity, avg_significance, avg_provision, time_done FROM aggregation_house'
                    ' WHERE social_group_id ' + ('= (SELECT id FROM social_groups WHERE name = %s)' if social_group is not None else 'is %s') +
                    ' AND living_situation_id ' + ('= (SELECT id FROM living_situations WHERE name = %s)' if living_situation is not None else 'is %s') +
                    ' AND service_type_id ' + ('= (SELECT id FROM service_types WHERE name = %s)' if service_type is not None else 'is %s') +
                    ' AND latitude = %s AND longitude = %s',
                    (social_group, living_situation, service_type, *where))
            cur_data = cur_provision.fetchall()
            if len(cur_data) != 0:
                id, intensity, significance, provision, done = cur_data[0]
                if not update:
                    return {
                        'provision': provision,
                        'intensity': intensity,
                        'significance': significance,
                        'time_done': done
                    }
                else:
                    found_id = id
        elif where_type == 'total':
            raise Exception('This method is not available for now')
        else:
            raise Exception(f'Unknown aggregation type: "{where_type}"')

    del cur_data

    if social_group is None:
        soc_groups = get_social_groups(service_type, living_situation, to_list=True)
    else:
        soc_groups = [social_group]
    
    if living_situation is None:
        situations = get_living_situations(social_group, service_type, to_list=True)
    else:
        situations = [living_situation]

    service_types: List[str]
    if service_type is None:
        service_types = get_service_types(social_group, living_situation, to_list=True)
    else:
        service_types = [service_type]

    houses: List[Tuple[float, float]]
    if where_type in ('municipalities', 'districts'):
        houses = list(map(lambda x: (x[1]['latitude'], x[1]['longitude']), all_houses[all_houses[where_column] == where].iterrows()))
    else:
        houses = [where] # type: ignore

    cnt_houses = 0
    provision_houses = 0.0
    intensity_houses = 0.0
    significance_houses = 0.0
    
    for house in houses:
        cnt_groups = 0
        provision_group = 0.0
        intensity_group = 0.0
        significance_group = 0.0
        groups_provision = dict()
        for social_group in soc_groups:
            social_group_needs = needs[needs['social_group'] == social_group]
            cnt_functions = 0
            provision_function = 0.0
            intensity_function = 0.0
            significance_function = 0.0
            for service_type in service_types:
                service_type_needs = social_group_needs[social_group_needs['service_type'] == service_type]
                if service_type_needs.shape[0] == 0 or (len(soc_groups) != 1 and len(service_types) != 1 and service_type_needs['significance'].max() <= 0.5):
                    continue
                cnt_atomic = 0
                provision_atomic = 0.0
                intensity_atomic = 0.0
                significance_atomic = 0.0
                for living_situation in situations:
                    living_situation_needs = service_type_needs[service_type_needs['living_situation'] == living_situation]
                    if living_situation_needs.shape[0] == 0 or living_situation_needs.iloc[0]['walking'] == 0 and living_situation_needs.iloc[0]['transport'] == 0:
                        continue
                    try:
                        prov = compute_atomic_provision(social_group, living_situation, service_type, house, provision_conn, houses_conn)
                        provision_atomic += prov['provision_result']
                        intensity_atomic += prov['parameters']['intensity']
                        significance_atomic += prov['parameters']['significance']
                        cnt_atomic += 1
                    except Exception as ex:
                        print(f'Exception occured: {ex}')
                        traceback.print_exc()
                        pass
                if cnt_atomic != 0:
                    provision_function += provision_atomic / cnt_atomic
                    intensity_function += intensity_atomic / cnt_atomic
                    significance_function += significance_atomic / cnt_atomic
                    cnt_functions += 1
            if cnt_functions != 0:
                provision_group += provision_function / cnt_functions
                intensity_group += intensity_function / cnt_functions
                significance_group += intensity_function / cnt_functions
                groups_provision[social_group] = (provision_function / cnt_functions, intensity_function / cnt_functions, intensity_function / cnt_functions)
                cnt_groups += 1

        with houses_conn.cursor() as cur_houses:
            cur_houses.execute('SELECT sum(ss.number) FROM social_structure ss'
                    ' INNER JOIN social_groups sg ON ss.social_group_id = sg.id'
                    ' WHERE house_id IN (SELECT residential_object_id FROM houses WHERE ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float = %s'
                    ' AND ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float = %s)', (house[0], house[1]))
            res = cur_houses.fetchall()
            if len(soc_groups) != 1:
                if len(res) != 0 and res[0][0] is not None:
                    cnt_functions = res[0][0]
                    provision_group = 0
                    intensity_group = 0
                    significance_group = 0
                    for social_group in groups_provision.keys():
                        cur_houses.execute('SELECT ss.number FROM social_structure ss'
                                ' INNER JOIN social_groups sg ON ss.social_group_id = sg.id'
                                ' WHERE house_id IN (SELECT residential_object_id FROM houses WHERE ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float = %s'
                                ' AND ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float = %s) AND sg.name = %s UNION select 0', (house[0], house[1], social_group))
                        number = cur_houses.fetchall()[0][0]
                        provision_group += groups_provision[social_group][0] * number / cnt_functions
                        intensity_group += groups_provision[social_group][1] * number / cnt_functions
                        significance_group += groups_provision[social_group][2] * number / cnt_functions
                elif len(groups_provision) != 0:
                    provision_group = sum(map(lambda x: x[0], groups_provision.values())) / len(groups_provision)
                    intensity_group = sum(map(lambda x: x[1], groups_provision.values())) / len(groups_provision)
                    significance_group = sum(map(lambda x: x[2], groups_provision.values())) / len(groups_provision)
                else:
                    provision_group = 0
                    intensity_group = 0
                    significance_group = 0
            elif cnt_groups != 0:
                provision_group /= cnt_groups
                intensity_group /= cnt_groups
                significance_group /= cnt_groups
        provision_houses += provision_group
        intensity_houses += intensity_group
        significance_houses += intensity_group
        cnt_houses += 1
                
    if cnt_houses != 0:
        provision_houses = round(provision_houses / cnt_houses, 2)
        intensity_houses = round(intensity_houses / cnt_houses, 2)
        significance_houses = round(significance_houses / cnt_houses)
    done_time: Any = time.localtime()
    done_time = f'{done_time.tm_year}-{done_time.tm_mon}-{done_time.tm_mday} {done_time.tm_hour}:{done_time.tm_min}:{done_time.tm_sec}'

    try:
        with provision_conn.cursor() as cur_provision:
            if where_type in ('districts', 'municipalities'):
                if found_id is None:
                    cur_provision.execute(f'INSERT INTO aggregation_{where_column} (social_group_id, living_situation_id, service_type_id, {where_column}_id, avg_intensity, avg_significance, avg_provision, time_done)'
                            ' VALUES ((SELECT id FROM social_groups WHERE name = %s), (SELECT id FROM living_situations WHERE name = %s), (SELECT id FROM service_types WHERE name = %s),'
                            f' (SELECT id FROM {where_type} WHERE full_name = %s), %s, %s, %s, %s)',
                            (*given_vaules, where, intensity_houses, significance_houses, provision_houses, done_time))
                else:
                    cur_provision.execute(f'UPDATE aggregation_{where_column} SET avg_intensity = %s, avg_significance = %s, avg_provision = %s, time_done = %s WHERE id = %s',
                            (intensity_houses, significance_houses, provision_houses, done_time, found_id))
            else:
                if found_id is None:
                    cur_provision.execute('INSERT INTO aggregation_house (social_group_id, living_situation_id, service_type_id, latitude, longitude, avg_intensity, avg_significance, avg_provision, time_done)'
                            'VALUES ((SELECT id FROM social_groups WHERE name = %s), (SELECT id FROM living_situations WHERE name = %s), (SELECT id FROM service_types WHERE name = %s),'
                            ' %s, %s, %s, %s, %s, %s)',
                            (*given_vaules, *where, intensity_houses, significance_houses, provision_houses, done_time))
                else:
                    cur_provision.execute('UPDATE aggregation_house SET intensity = %s, avg_significance = %s, avg_provision = %s, time_done = %s WHERE id = %s', 
                            (intensity_houses, significance_houses, provision_houses, done_time, found_id))
            provision_conn.commit()
    except Exception:
        provision_conn.rollback()
        raise
    return {
        'provision': provision_houses,
        'intensity': intensity_houses,
        'significance': significance_houses,
        'time_done': done_time
    }

def aggregate_district(district: str, social_group: str, living_situation: str, service_type: str,
        provision_conn: psycopg2.extensions.connection, houses_conn:psycopg2.extensions.connection) -> None:
    print(f'Aggregating social_group({social_group}) + living_situation({living_situation}) + service_type({service_type}) + district({district})')
    start = time.time()
    res = get_aggregation(district, 'districts', social_group, living_situation, service_type, provision_conn, houses_conn, False)
    print(f'Finished    social_group({social_group}) + living_situation({living_situation}) + service_type({service_type}) + district({district})'
            f' in {time.time() - start:6.2f} seconds (total_value = {res["provision"]:.2f})')

def aggregate_municipality(municipality: str, social_group: str, living_situation: str, service_type: str,
        provision_conn: psycopg2.extensions.connection, houses_conn:psycopg2.extensions.connection) -> None:
    print(f'Aggregating social_group({social_group}) + living_situation({living_situation}) + service_type({service_type}) + municipality({municipality})')
    start = time.time()
    res = get_aggregation(municipality, 'municipalities', social_group, living_situation, service_type, provision_conn, houses_conn, False)
    print(f'Finished    social_group({social_group}) + living_situation({living_situation}) + service_type({service_type}) + municipality({municipality})'
            f' in {time.time() - start:6.2f} seconds (total_value = {res["provision"]:.2f})')

def update_aggregation(district_or_municipality: str, including_municipalities: bool = False) -> None:
    full_start = time.time()
    if district_or_municipality in city_hierarchy['district_full_name'].unique():
        district = True
    else:
        district = False
    tp = ThreadPool(8, [lambda: ('houses_conn', psycopg2.connect(properties.houses_conn_string)),
            lambda: ('provision_conn', psycopg2.connect(properties.provision_conn_string))], max_size=10)
    try:
        for social_group in get_social_groups(to_list=True) + [None]: # type: ignore
            for service_type in get_service_types(social_group, to_list=True) + [None]: # type: ignore
                for living_situation in get_living_situations(social_group, service_type, to_list=True) + [None]: # type: ignore
                    if district:
                        try:
                            tp.execute(aggregate_district, (district_or_municipality, social_group, living_situation, service_type))
                        except Exception as ex:
                            traceback.print_exc()
                            print(f'Exception occured! {ex}')
                        if including_municipalities:
                            for municipality in all_houses[all_houses['district'] == district]['municipality'].unique():
                                try:
                                    tp.execute(aggregate_municipality, (municipality, social_group, living_situation, service_type))
                                except Exception as ex:
                                    traceback.print_exc()
                                    print(f'Exception occured! {ex}')
                    else:
                        try:
                            tp.execute(aggregate_municipality, (district_or_municipality, social_group, living_situation, service_type))
                        except Exception as ex:
                            traceback.print_exc()
                            print(f'Exception occured! {ex}')

    except KeyboardInterrupt:
        print('Interrupted by user')
        tp.stop()
        tp.join()
    finally:
        print(f'Finished updating all agregations in {time.time() - full_start:.2f} seconds')

def update_global_data() -> None:
    global all_houses
    global needs
    global infrastructure
    global listings
    global services_buildings
    global blocks
    global city_hierarchy
    global provision_districts
    global provision_municipalities
    global provision_blocks
    with properties.houses_conn.cursor() as cur:
        cur.execute('SELECT DISTINCT dist.full_name, muni.full_name, ROUND(ST_X(ST_Centroid(h.geometry))::numeric, 3)::float as latitude,'
                '   ROUND(ST_Y(ST_Centroid(h.geometry))::numeric, 3)::float as longitude'
                ' FROM houses h'
                '   JOIN districts dist ON dist.id = h.district_id'
                '   JOIN municipalities muni on muni.id = h.municipality_id')
        all_houses = pd.DataFrame(cur.fetchall(), columns=('district', 'municipality', 'latitude', 'longitude'))

        cur.execute('SELECT i.id, i.name, f.id, f.name, s.id, s.name FROM city_functions f JOIN infrastructure_types i ON i.id = f.infrastructure_type_id'
                ' JOIN service_types s ON s.city_function_id = f.id ORDER BY i.name, f.name, s.name;')
        infrastructure = pd.DataFrame(cur.fetchall(), columns=('infrastructure_id', 'infrastructure', 'function_id', 'function', 'service_type_id', 'service_type'))

        cur.execute('SELECT s.name, l.name, f.name, n.walking, n.public_transport, n.personal_transport, n.intensity FROM needs n'
                ' JOIN social_groups s ON s.id = n.social_group_id'
                ' JOIN living_situations l ON l.id = n.living_situation_id'
                ' JOIN service_types f ON f.id = n.service_type_id'
                ' ORDER BY s.name, l.name, f.name')
        needs = pd.DataFrame(cur.fetchall(), columns=('social_group', 'living_situation', 'service_type', 'walking', 'transport', 'car', 'intensity'))
        cur.execute('SELECT s.name, st.name, v.significance FROM values v'
                ' JOIN social_groups s ON s.id = v.social_group_id'
                ' JOIN city_functions f ON f.id = v.city_function_id'
                ' JOIN service_types st ON st.city_function_id = f.id')
        tmp = pd.DataFrame(cur.fetchall(), columns=('social_group', 'service_type', 'significance'))
        needs = needs.merge(tmp, on=['social_group', 'service_type'], how='inner')

        cur.execute('SELECT p.id, b.address, f.name, ST_AsGeoJSON(ST_Centroid(p.geometry)), f.capacity, st.name FROM buildings b'
                ' JOIN physical_objects p ON b.physical_object_id = p.id'
                ' JOIN phys_objs_fun_objs pf ON p.id = pf.phys_obj_id'
                ' JOIN functional_objects f ON f.id = pf.fun_obj_id'
                ' JOIN service_types st on f.service_type_id = st.id'
                ' ORDER BY p.id')
        services_buildings = pd.DataFrame(cur.fetchall(), columns=('service_type_id', 'address', 'service_name', 'location', 'power', 'service_type'))
        services_buildings['location'] = pd.Series(
            map(lambda geojson: (round(float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), 4), round(float(geojson[geojson.rfind(',') + 1:-2]), 4)),
                services_buildings['location'])
        )
        cur.execute('SELECT m.id, m.full_name, m.short_name, m.population, d.id, d.full_name, d.short_name, d.population FROM municipalities m'
                ' JOIN districts d on d.id = m.district_id ORDER BY d.full_name, m.full_name')
        city_hierarchy = pd.DataFrame(cur.fetchall(), columns=('municipality_id', 'municipality_full_name', 'municipality_short_name',
                'municipality_population', 'district_id', 'district_full_name', 'district_short_name', 'district_population'))

        cur.execute('SELECT b.id, b.population, m.full_name as municipality, d.full_name as district FROM'
            ' blocks b JOIN municipalities m ON m.id = b.municipality_id JOIN districts d ON d.id = m.district_id ORDER BY 4, 3, 1')
        blocks = pd.DataFrame(cur.fetchall(), columns=('id', 'population', 'municipality', 'district')).set_index('id')
        blocks['population'] = blocks['population'].replace({np.nan: None})

        cur.execute('SELECT loc.full_name, s.name, houses.count, eval.count, eval.service_load_mean, eval.service_load_sum,'
                '   houses.provision_mean, eval.evaluation_mean, eval.reserve_resources_mean, eval.reserve_resources_sum,'
                '   houses.reserve_resources_mean, houses.reserve_resources_sum'
                ' FROM provision.services_districts eval'
                '   JOIN provision.houses_districts houses on houses.service_type_id = eval.service_type_id and houses.district_id = eval.district_id'
                '   JOIN districts loc on eval.district_id = loc.id'
                '   JOIN service_types s on eval.service_type_id = s.id'
                ' ORDER BY 1, 2'
        )
        provision_districts = pd.DataFrame(cur.fetchall(), columns=('district', 'service_type', 'houses_count', 'services_count', 'services_load_mean', 'services_load_sum',
                'houses_provision', 'services_evaluation', 'services_reserve_mean', 'services_reserve_sum', 'houses_reserve_mean', 'houses_reserve_sum'))

        cur.execute('SELECT loc.full_name, s.name, houses.count, eval.count, eval.service_load_mean, eval.service_load_sum,'
                '   houses.provision_mean, eval.evaluation_mean, eval.reserve_resources_mean, eval.reserve_resources_sum,'
                '   houses.reserve_resources_mean, houses.reserve_resources_sum'
                ' FROM provision.services_municipalities eval'
                '   JOIN provision.houses_municipalities houses on houses.service_type_id = eval.service_type_id and houses.municipality_id = eval.municipality_id'
                '   JOIN municipalities loc on eval.municipality_id = loc.id'
                '   JOIN service_types s on eval.service_type_id = s.id'
                ' ORDER BY 1, 2'
        )
        provision_municipalities = pd.DataFrame(cur.fetchall(), columns=('municipality', 'service_type', 'houses_count', 'services_count', 'services_load_mean',
                'services_load_sum', 'houses_provision', 'services_evaluation', 'services_reserve_mean', 'services_reserve_sum', 'houses_reserve_mean', 'houses_reserve_sum'))

        cur.execute('SELECT eval.block_id, s.name, houses.count, eval.count, eval.service_load_mean, eval.service_load_sum,'
                '   houses.provision_mean, eval.evaluation_mean, eval.reserve_resources_mean, eval.reserve_resources_sum,'
                '   houses.reserve_resources_mean, houses.reserve_resources_sum'
                ' FROM provision.services_blocks eval'
                '   JOIN provision.houses_blocks houses on houses.service_type_id = eval.service_type_id and houses.block_id = eval.block_id'
                '   JOIN service_types s on eval.service_type_id = s.id'
                ' ORDER BY 1, 2'
        )
        provision_blocks = pd.DataFrame(cur.fetchall(), columns=('block', 'service_type', 'houses_count', 'services_count', 'services_load_mean', 'services_load_sum',
                'houses_provision', 'services_evaluation', 'services_reserve_mean', 'services_reserve_sum', 'houses_reserve_mean', 'houses_reserve_sum'))

        cur.execute('SELECT id, name FROM city_functions ORDER BY name')
        city_functions = pd.DataFrame(cur.fetchall(), columns=('id', 'name'))
        cur.execute('SELECT id, name FROM living_situations ORDER BY name')
        living_situations = pd.DataFrame(cur.fetchall(), columns=('id', 'name'))
        cur.execute('SELECT id, name FROM social_groups ORDER BY name')
        social_groups = pd.DataFrame(cur.fetchall(), columns=('id', 'name'))
        listings = Listings(city_functions, living_situations, social_groups)
    # blocks['population'] = blocks['population'].fillna(-1).astype(int)
    with properties.provision_conn.cursor() as cur:
        cur.execute('SELECT s.block_id, ss.social_groups, s.services FROM blocks_soc_groups ss JOIN blocks_services s on s.block_id = ss.block_id')
        blocks = blocks.join(pd.DataFrame(cur.fetchall(), columns=('id', 'social_groups', 'services')).set_index('id'))


compress = Compress()

app = Flask(__name__)
compress.init_app(app)
app.json_encoder = NonASCIIJSONEncoder

@app.after_request
def after_request(response) -> Response:
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = '*'
    response.headers['Access-Control-Allow-Headers'] = '*'
    return response

@app.route('/api/reload_data/', methods=['POST'])
def reload_data() -> Response:
    update_global_data()
    return make_response('OK')

# Расчет обеспеченности для атомарной ситуации: обеспеченность одной социальной группы в одной жизненной ситуации
# одной городской функцией, относительно одного жилого дома.

# Для сервисов передаются следующие атрибуты:
# -  название сервиса(service_name)
# -  признак принадлежности изохрону пешеходной доступности (walking_dist, boolean)
# -  признак принадлежности изохронам транспортной доступности (transport_dist, boolean)
# -  мощность сервиса (power, со значениями от 1 до 10)

# Сервис возвращает числовую оценку обеспеченности целевой социальной группы в целевой жизненной ситуации сервисами,
# относящимися к целевой городской функции, в целевой точке (доме)
@app.route('/api/provision/atomic', methods=['GET'])
@app.route('/api/provision/atomic/', methods=['GET'])
def atomic_provision() -> Response:
    if not ('social_group' in request.args and 'living_situation' in request.args and 'service_type' in request.args and 'location' in request.args):
        return make_response(jsonify({'error': "Request must include all of the ('social_group', 'living_situation', 'service_type', 'location') arguments"}), 400)
    social_group: str = request.args['social_group'] # type: ignore
    if social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    living_situation: str = request.args['living_situation'] # type: ignore
    if living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    service_type: str = request.args['service_type'] # type: ignore
    if service_type.isnumeric():
        service_type = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in infrastructure['service_type_id'] else 'None'
    if not (social_group in get_social_groups(to_list=True) and living_situation in get_living_situations(to_list=True) \
                and service_type in get_service_types(to_list=True)):
        return make_response(jsonify({'error': f"At least one of the ('social_group', 'living_situation', 'service_type') is not in the list of avaliable"
                f' ({social_group in get_social_groups(to_list=True)}, {living_situation in get_living_situations(to_list=True)},'
                        f' {service_type in get_service_types(to_list=True)})'}), 400)
    coords: Tuple[int, int] = tuple(map(float, request.args['location'].split(','))) # type: ignore

    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': compute_atomic_provision(coords=coords, houses_conn=properties.houses_conn, provision_conn=properties.provision_conn, **request.args)
    }))

@app.route('/api/provision/aggregated', methods=['GET'])
@app.route('/api/provision/aggregated/', methods=['GET'])
def aggregated_provision() -> Response:
    social_group: Optional[str] = request.args.get('social_group')
    if social_group and social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    living_situation: Optional[str] = request.args.get('living_situation')
    if living_situation and living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    service_type: Optional[str] = request.args.get('service_type')
    if service_type and service_type.isnumeric():
        service_type = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in infrastructure['service_type_id'] else 'None'
    location: Optional[str] = request.args.get('location')
    if not ((social_group is None or social_group == 'all' or social_group in get_social_groups(to_list=True))
            and (living_situation is None or living_situation == 'all' or living_situation in get_living_situations(to_list=True))
            and (service_type is None or service_type == 'all' or service_type in get_service_types(to_list=True))):
        return make_response(jsonify({'error': "At least one of the ('social_group', 'living_situation', 'city_function') is not in the list of avaliable"}), 400)
    launch_aggregation = True if 'launch_aggregation' in request.args else False
    
    soc_groups: Union[List[str], List[Optional[str]]] = get_social_groups(to_list=True) if social_group == 'all' else [social_group] # type: ignore
    situations: Union[List[str], List[Optional[str]]] = get_living_situations(to_list=True) if living_situation == 'all' else [living_situation] # type: ignore
    service_types: Union[List[str], List[Optional[str]]] = get_service_types(to_list=True) if service_type == 'all' else [service_type] # type: ignore
    where: List[Union[str, Tuple[float, float]]]
    where_type: str
    if location is None:
        where = ['total']
        where_type = 'total'
        raise Exception("Getting total aggregaation is unsupported at the moment. You need to set 'location' parameter.")
    elif location.startswith('inside_'): # type: ignore
        name = location[7:] # type: ignore
        if name not in city_hierarchy['district_full_name'].unique():
            return make_response(jsonify({'error': f"'{name}'' should be a district, but it is not in the list"}), 400)
        where_type = 'municipalities'
        where = list(all_houses[all_houses['district'] == name]['municipality'].unique())
    elif location in city_hierarchy['district_full_name'].unique():
        where = [location]
        where_type = 'districts'
    elif location in city_hierarchy['municipality_full_name'].unique():
        where = [location]
        where_type = 'municipalities'
    else:
        try:
            where = [tuple(map(float, request.args.get('location', type=str).split(',')))] # type: ignore
            where_type = 'house'
        except:
            return make_response(jsonify({'error': f"Cannot find '{location}' in any of the 'districts' or 'municipalities', or parse as a house coordinates"}), 400)

    where_column = 'district' if where_type == 'districts' else 'house' if where_type == 'house' else 'municipality'
    res: List[Dict[str, Dict[str, Any]]] = list()
    with properties.provision_conn.cursor() as cur:
        for now_where in where:
            for now_soc_group in soc_groups:
                for now_situation in situations:
                    cur.execute(f'SELECT DISTINCT f.name FROM aggregation_{where_column} a' + 
                            (f' JOIN {where_type} w ON w.id = a.{where_column}_id' if where_type != 'house' else '') +
                            ' JOIN social_groups s ON s.id = a.social_group_id'
                            ' JOIN living_situations l ON l.id = a.living_situation_id'
                            ' JOIN city_functions f ON f.id = a.city_function_id' +
                            (' WHERE w.full_name = %s' if where_type != 'house' else ' WHERE a.latitude = %s AND a.longitude = %s') +
                            ' AND s.name = %s AND l.name = %s',
                            (now_where, now_soc_group, now_situation) if where_type != 'house' else (now_where[0], now_where[1], now_soc_group, now_situation))
                    ready_functions = list(map(lambda x: x[0], cur.fetchall()))
                    for now_service in service_types:
                        if now_service in ready_functions or launch_aggregation:
                            if now_service not in ready_functions:
                                print(f'location({location}) + social_group({now_soc_group}) + living_situation({now_situation}) + service_type({now_service}) is missing, aggregating')
                            res.append({
                                'params': {
                                    'location': now_where,
                                    'social_group': now_soc_group,
                                    'living_situation': now_situation,
                                    'city_function': now_service,
                                },
                                'result': get_aggregation(now_where, where_type, now_soc_group, now_situation, now_service, properties.provision_conn, properties.houses_conn)
                            })
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': social_group,
                'living_situation': living_situation,
                'service_type': service_type,
                'location': location,
                'location_type': where_type,
                'launch_aggregation': launch_aggregation
            },
            'provision': res
        }
    }))
   
@app.route('/api/provision/alternative', methods=['GET'])
@app.route('/api/provision/alternative/', methods=['GET'])
def alternative_aggregated_provision():
    social_group: Optional[str] = request.args.get('social_group')
    if social_group and social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    living_situation: Optional[str] = request.args.get('living_situation')
    if living_situation and living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    service_type: Optional[str] = request.args.get('service_type')
    if service_type and service_type.isnumeric():
        service_type = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in infrastructure['service_type_id'] else 'None'
    location: Optional[str] = request.args.get('location')
    if not ((social_group is None or social_group == 'all' or social_group in get_social_groups(to_list=True))
            and (living_situation is None or living_situation == 'all' or living_situation in get_living_situations(to_list=True))
            and (service_type is None or service_type == 'all' or service_type in infrastructure['service_type'].unique())):
        return make_response(jsonify({'error': "At least one of the ('social_group', 'living_situation', 'service_type') is not in the list of avaliable"}), 400)
    
    soc_groups: Union[List[str], List[Optional[str]]] = get_social_groups(to_list=True) if social_group == 'all' else [social_group] # type: ignore
    situations: Union[List[str], List[Optional[str]]] = get_living_situations(to_list=True) if living_situation == 'all' else [living_situation] # type: ignore
    service_types: Union[List[str], List[Optional[str]]] = get_service_types(to_list=True) if service_type == 'all' else [service_type] # type: ignore
    where: List[Union[int, str, Tuple[float, float]]]
    if location is None:
        where = ['city']
        raise NotImplementedError("Getting city aggregation is unsupported at the moment. You need to set 'location' parameter.")
    elif location.startswith('inside_'): # type: ignore
        name = location[7:] # type: ignore
        if name in city_hierarchy['district_full_name'].unique():
            where = list(all_houses[all_houses['district'] == name]['municipality'].unique())
        elif name in city_hierarchy['municipality_full_name'].unique():
            where = list(blocks[blocks['municipality'] == name].index)
        else:
            return make_response(jsonify({'error': f"'{name}' should be a district, but it is not in the list"}), 400)
    elif location in city_hierarchy['district_full_name'].unique() or location in city_hierarchy['municipality_full_name'].unique():
        where = [location]
    else:
        try:
            where = [tuple(map(float, request.args.get('location', type=str).split(',')))] # type: ignore
            return make_response(jsonify({'error': 'Alternative aggregation method does not support aggregation on house'}))
        except:
            return make_response(jsonify({'error': f"Cannot find '{location}' in any of the 'districts' or 'municipalities'"}), 400)

    res: List[Dict[str, Dict[str, Any]]] = list()
    for now_where in where:
        for now_soc_group in soc_groups:
            for now_situation in situations:
                for now_service in service_types:
                    res.append({
                        'parameters': {
                            'location': now_where,
                            'social_group': now_soc_group,
                            'living_situation': now_situation,
                            'service_type': now_service,
                        },
                        'result': experimental_aggregation.aggregate(needs, infrastructure, blocks, now_where,
                                now_soc_group, now_situation, now_service, 'return_debug_info' in request.args)
                    })
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': social_group,
                'living_situation': living_situation,
                'service_type': service_type,
                'location': location,
            },
            'provision': res
        }
    }))

@app.route('/api/provision/ready/houses', methods=['GET'])
@app.route('/api/provision/ready/houses/', methods=['GET'])
def ready_houses() -> Response:
    social_group: Optional[str] = request.args.get('social_group')
    if social_group and social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    living_situation: Optional[str] = request.args.get('living_situation')
    if living_situation and living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    city_function: Optional[str] = request.args.get('city_function')
    if city_function and city_function.isnumeric():
        city_function = listings.city_functions[listings.city_functions['id'] == int(city_function)]['name'].iloc[0] \
                if int(city_function) in listings.city_functions['id'] else 'None'
    house: Tuple[Optional[float], Optional[float]]
    if 'house' in request.args:
        house = tuple(map(float, request.args['house'].split(','))) # type: ignore
    else:
        house = (None, None)
    with properties.provision_conn.cursor() as cur:
        cur_str = 'SELECT soc.name, liv.name, fun.name, a.latitude, a.longitude, avg_provision' \
            ' FROM aggregation_house a JOIN living_situations liv ON liv.id = a.living_situation_id' \
            ' JOIN social_groups soc ON soc.id = a.social_group_id' \
            ' JOIN city_functions fun ON fun.id = a.city_function_id'
        wheres = []
        for column_name, value in (('soc.name', social_group), ('liv.name', living_situation), ('fun.name', city_function), ('latitude', house[0]), ('longitude', house[1])):
            if value is not None:
                wheres.append(f"{column_name} = '{value}'")
        if len(wheres) != 0:
            cur_str += ' WHERE ' + ' AND '.join(wheres)
        cur.execute(cur_str)
        ans = pd.DataFrame(cur.fetchall(), columns=('social_group', 'living_situation', 'city_function', 'latitude', 'longitude', 'provision'))
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'params': {
                'social_group': social_group,
            'living_situation': living_situation,
                'city_function': city_function,
                'house': f'{house[0]},{house[1]}' if house[0] is not None else None
            },
            'result': list((row[1].to_dict() for row in ans.iterrows()))
        }
    }))

@app.route('/api/provision/ready/districts', methods=['GET'])
@app.route('/api/provision/ready/districts/', methods=['GET'])
def ready_districts() -> Response:
    social_group: Optional[str] = request.args.get('social_group')
    if social_group and social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    living_situation: Optional[str] = request.args.get('living_situation')
    if living_situation and living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    city_function: Optional[str] = request.args.get('city_function')
    if city_function and city_function.isnumeric():
        city_function = listings.city_functions[listings.city_functions['id'] == int(city_function)]['name'].iloc[0] \
                if int(city_function) in listings.city_functions['id'] else 'None'
    district: Optional[str] = request.args.get('district', None, type=str)
    if district and district.isnumeric():
        district = city_hierarchy[city_hierarchy['district_id'] == int(district)]['district_full_name'] \
                if int(district) in city_hierarchy['district_id'] else 'None'
    with properties.provision_conn.cursor() as cur:
        cur_str = 'SELECT soc.name, liv.name, fun.name, dist.full_name, avg_provision' \
            ' FROM aggregation_district a JOIN living_situations liv ON liv.id = a.living_situation_id' \
            ' JOIN social_groups soc ON soc.id = a.social_group_id' \
            ' JOIN city_functions fun ON fun.id = a.city_function_id' \
            ' JOIN districts dist ON dist.id = a.district_id'
        wheres = []
        for column_name, value in (('soc.name', social_group), ('liv.name', living_situation), ('fun.name', city_function), ('dist.full_name', district)):
            if value is not None:
                wheres.append(f"{column_name} = '{value}'")
        if len(wheres) != 0:
            cur_str += ' WHERE ' + ' AND '.join(wheres)
        cur.execute(cur_str)
        ans = pd.DataFrame(cur.fetchall(), columns=('social_group', 'living_situation', 'city_function', 'district', 'provision'))
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'params': {
                'social_group': social_group,
                'living_situation': living_situation,
                'city_function': city_function,
                'district': district
            },
            'result': list((row[1].to_dict() for row in ans.iterrows()))
        }
    }))

@app.route('/api/provision/ready/municipalities', methods=['GET'])
@app.route('/api/provision/ready/municipalities/', methods=['GET'])
def ready_municipalities() -> Response:
    social_group: Optional[str] = request.args.get('social_group')
    if social_group and social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    living_situation: Optional[str] = request.args.get('living_situation')
    if living_situation and living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    city_function: Optional[str] = request.args.get('city_function')
    if city_function and city_function.isnumeric():
        city_function = listings.city_functions[listings.city_functions['id'] == int(city_function)]['name'].iloc[0] \
                if int(city_function) in listings.city_functions['id'] else 'None'
    municipality: Optional[str] = request.args.get('municipality', None, type=str)
    if municipality and municipality.isnumeric():
        municipality = city_hierarchy[city_hierarchy['municipality_id'] == int(municipality)]['municipality_full_name'] \
                if int(municipality) in city_hierarchy['municipality_id'] else 'None'
    with properties.provision_conn.cursor() as cur:
        cur_str = 'SELECT soc.name, liv.name, fun.name, muni.full_name, avg_provision' \
            ' FROM aggregation_municipality a JOIN living_situations liv ON liv.id = a.living_situation_id' \
            ' JOIN social_groups soc ON soc.id = a.social_group_id' \
            ' JOIN city_functions fun ON fun.id = a.city_function_id' \
            ' JOIN municipalities muni ON muni.id = a.municipality_id'
        wheres = []
        for column_name, value in (('soc.name', social_group), ('liv.name', living_situation), ('fun.name', city_function), ('muni.full_name', municipality)):
            if value is not None:
                wheres.append(f"{column_name} = '{value}'")
        if len(wheres) != 0:
            cur_str += ' WHERE ' + ' AND '.join(wheres)
        cur.execute(cur_str)
        ans = pd.DataFrame(cur.fetchall(), columns=('social_group', 'living_situation', 'city_function', 'municipality', 'provision'))
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'params': {
                'social_group': social_group,
                'living_situation': living_situation,
                'city_function': city_function,
                'municipality': municipality
            },
            'result': list((row[1].to_dict() for row in ans.iterrows()))
        }
    }))
    
@app.route('/api/houses', methods=['GET'])
@app.route('/api/houses/', methods=['GET'])
def houses_in_square() -> Response:
    if 'firstPoint' not in request.args or 'secondPoint' not in request.args:
        return make_response(jsonify({'error': "'firstPoint' and 'secondPoint' must be provided as query parameters"}), 400)
    point_1: Tuple[int, int] = tuple(map(float, request.args['firstPoint'].split(','))) # type: ignore
    point_2: Tuple[int, int] = tuple(map(float, request.args['secondPoint'].split(','))) # type: ignore
    with properties.houses_conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float, ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float FROM houses"
                " WHERE ST_WITHIN(geometry, ST_POLYGON(text('LINESTRING({lat1} {lng1}, {lat1} {lan2}, {lat2} {lan2}, {lat2} {lng1}, {lat1} {lng1})'), 4326))".format(
            lat1=point_1[0], lng1 = point_1[1], lat2 = point_2[0], lan2 = point_2[1]
        ))
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'params': {
                    'firstCoord': f'{point_1[0]},{point_1[1]}',
                    'secondCoord': f'{point_2[0]},{point_2[1]}'
                },
                'houses': list(cur.fetchall())
            }
        }
        ))

def get_social_groups(service_type: Optional[str] = None, living_situation: Optional[str] = None, to_list: bool = False) -> Union[List[str], pd.DataFrame]:
    if service_type and service_type.isnumeric():
        service_type = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in infrastructure['service_type_id'] else 'None'
    if living_situation and living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    res = needs[(needs['significance'] > 0) & (needs['intensity'] > 0)]
    res = res[res['social_group'].apply(lambda name: name[-1] == ')')] \
        .merge(listings.social_groups, left_on='social_group', right_on='name') \
        .sort_values('id') \
        .drop(['id', 'name'], axis=True)
    if service_type is None:
        res = res.drop(['service_type', 'significance'], axis=True)
    else:
        res = res[res['service_type'] == service_type].drop('service_type', axis=True)
    if living_situation is None:
        res = res.drop(['living_situation', 'intensity'], axis=True)
    else:
        res = res[res['living_situation'] == living_situation].drop('living_situation', axis=True)
    if to_list:
        return list(res['social_group'].unique())
    else:
        return res

@app.route('/api/relevance/social_groups', methods=['GET'])
@app.route('/api/relevance/social_groups/', methods=['GET'])
def relevant_social_groups() -> Response:
    res: pd.DataFrame = get_social_groups(request.args.get('service_type'), request.args.get('living_situation'))
    ids = list(listings.social_groups.set_index('name').loc[list(res['social_group'])]['id'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'service_type': request.args.get('service_type'),
                'living_situation': request.args.get('living_situation')
            },
            'social_groups': list(res.drop_duplicates().replace({np.nan: None}).transpose().to_dict().values()),
            'social_groups_ids': ids
        }
    }))

@app.route('/api/list/social_groups', methods=['GET'])
@app.route('/api/list/social_groups/', methods=['GET'])
def list_social_groups() -> Response:
    res: List[str] = get_social_groups(request.args.get('service_type'), request.args.get('living_situation'), to_list=True)
    ids = list(listings.social_groups.set_index('name').loc[list(res)]['id'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'service_type': request.args.get('service_type'),
                'living_situation': request.args.get('living_situation')
            },
            'social_groups': res,
            'social_groups_ids': ids
        }
    }))


def get_city_functions(social_group: Optional[str] = None, living_situation: Optional[str] = None, to_list: bool = False) -> Union[List[str], pd.DataFrame]:
    if social_group and social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    if living_situation and living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    res = needs[(needs['significance'] > 0) & (needs['intensity'] > 0) & needs['service_type'].isin(infrastructure['service_type'].dropna().unique())]
    if social_group is None:
        res = res.drop(['social_group', 'significance'], axis=True)
    else:
        res = res[res['social_group'] == social_group].drop('social_group', axis=True)
    if living_situation is None:
        res = res.drop(['living_situation', 'intensity'], axis=True)
    else:
        res = res[res['living_situation'] == living_situation].drop('living_situation', axis=True)
    if to_list:
        return list(infrastructure[infrastructure['service_type'].isin(res['service_type'].unique())]['function'].unique())
    else:
        return res.join(infrastructure[['function', 'service_type']].set_index('service_type'), on='service_type', how='inner')

@app.route('/api/relevance/city_functions', methods=['GET'])
@app.route('/api/relevance/city_functions/', methods=['GET'])
def relevant_city_functions() -> Response:
    res: pd.DataFrame = get_city_functions(request.args.get('social_group'), request.args.get('living_situation'))
    ids = list(listings.city_functions.set_index('name').loc[list(res['function'])]['id'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'living_situation': request.args.get('living_situation')
            },
            'city_functions': list(res.drop_duplicates().replace({np.nan: None}).transpose().to_dict().values()),
            'city_functions_ids': ids
        }
    }))

@app.route('/api/list/city_functions', methods=['GET'])
@app.route('/api/list/city_functions/', methods=['GET'])
def list_city_functions() -> Response:
    res: List[str] = sorted(get_city_functions(request.args.get('social_group'), request.args.get('living_situation'), to_list=True))
    ids = list(listings.city_functions.set_index('name').loc[list(res)]['id'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'living_situation': request.args.get('living_situation')
            },
            'city_functions': res,
            'city_functions_ids': ids
        }
    }))

def get_service_types(social_group: Optional[str] = None, living_situation: Optional[str] = None, to_list: bool = False) -> Union[List[str], pd.DataFrame]:
    if social_group and social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    if living_situation and living_situation.isnumeric():
        living_situation = listings.living_situations[listings.living_situations['id'] == int(living_situation)]['name'].iloc[0] \
                if int(living_situation) in listings.living_situations['id'] else 'None'
    res = needs[(needs['significance'] > 0) & (needs['intensity'] > 0) & needs['service_type'].isin(infrastructure['service_type'].dropna().unique())]
    if social_group is None:
        res = res.drop(['social_group', 'significance'], axis=True)
    else:
        res = res[res['social_group'] == social_group].drop('social_group', axis=True)
    if living_situation is None:
        res = res.drop(['living_situation', 'intensity'], axis=True)
    else:
        res = res[res['living_situation'] == living_situation].drop('living_situation', axis=True)
    if to_list:
        return list(res['service_type'].unique())
    else:
        return res

@app.route('/api/list/service_types', methods=['GET'])
@app.route('/api/list/service_types/', methods=['GET'])
def list_services() -> Response:
    res: List[str] = sorted(get_service_types(request.args.get('social_group'), request.args.get('living_situation'), to_list=True))
    ids = list(infrastructure[infrastructure['service_type'].isin(res)][['service_type_id', 'service_type']].drop_duplicates() \
            .sort_values('service_type')['service_type_id'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'living_situation': request.args.get('living_situation')
            },
            'service_types': res,
            'service_types_ids': ids
        }
    }))

def get_living_situations(social_group: Optional[str] = None, service_type: Optional[str] = None, to_list: bool = False) -> Union[List[str], pd.DataFrame]:
    if social_group and social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    if service_type and service_type.isnumeric():
        service_type = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in infrastructure['service_type_id'] else 'None'
    res = needs[(needs['significance'] > 0) & (needs['intensity'] > 0)]
    if social_group is not None and service_type is not None:
        res = res[(res['social_group'] == social_group) & (res['service_type'] == service_type)].drop(['service_type', 'social_group'], axis=True)
    elif social_group is not None:
        res = pd.DataFrame(res[res['social_group'] == social_group]['living_situation'].unique(), columns=('living_situation',))
    elif service_type is not None:
        res = pd.DataFrame(res[res['service_type'] == service_type]['living_situation'].unique(), columns=('living_situation',))
    else:
        res = pd.DataFrame(res['living_situation'].unique(), columns=('living_situation',))
    if to_list:
        return list(res['living_situation'].unique())
    else:
        return res

@app.route('/api/relevance/living_situations', methods=['GET'])
@app.route('/api/relevance/living_situations/', methods=['GET'])
def relevant_living_situations() -> Response:
    res: pd.DataFrame = get_living_situations(request.args.get('social_group'), request.args.get('service_type'))
    ids = list(listings.living_situations.set_index('name').loc[list(res['living_situation'])]['id'])
    significance: Optional[int] = None
    if 'significance' in res.columns:
        if res.shape[0] > 0:
            significance = next(iter(res['significance']))
        res = res.drop('significance', axis=True)
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'service_type': request.args.get('service_type'),
                'significance': significance
            },
            'living_situations': list(res.drop_duplicates().transpose().to_dict().values()),
            'living_situations_ids': ids
        }
    }))

@app.route('/api/list/living_situations', methods=['GET'])
@app.route('/api/list/living_situations/', methods=['GET'])
def list_living_situations() -> Response:
    res: List[str] = get_living_situations(request.args.get('social_group'), request.args.get('service_type'), to_list=True)
    ids = list(listings.living_situations.set_index('name').loc[list(res)]['id'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'params': {
                'social_group': request.args.get('social_group'),
                'service_type': request.args.get('service_type'),
            },
            'living_situations': res,
            'living_situations_ids': ids
        }
    }))

@app.route('/api/list/infrastructures', methods=['GET'])
@app.route('/api/list/infrastructures/', methods=['GET'])
def list_infrastructures() -> Response:
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'infrastructures': tuple([{
                'name': infra,
                'functions': tuple([{
                    'name': function,
                    'service_types': tuple([service_type for service_type in sorted(infrastructure[infrastructure['function'] == function].dropna()['service_type'])])
                } for function in sorted(infrastructure[infrastructure['infrastructure'] == infra].dropna()['function'].unique())])
            } for infra in sorted(infrastructure['infrastructure'].unique())])
        }
    }))

@app.route('/api/list/districts', methods=['GET'])
@app.route('/api/list/districts/', methods=['GET'])
def list_districts() -> Response:
    districts = city_hierarchy[['district_id', 'district_full_name', 'district_short_name']].drop_duplicates().sort_values('district_full_name')
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'districts': list(districts['district_full_name']),
            'districts_short_names': list(districts['district_short_name']),
            'districts_ids': list(districts['district_id'])
        }
    }))

@app.route('/api/list/municipalities', methods=['GET'])
@app.route('/api/list/municipalities/', methods=['GET'])
def list_municipalities() -> Response:
    municipalities = city_hierarchy[['municipality_id', 'municipality_full_name', 'municipality_short_name']].drop_duplicates().sort_values('municipality_full_name')
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'municipalities': list(municipalities['municipality_full_name']),
            'municipalities_short_names': list(municipalities['municipality_short_name']),
            'municipalities_ids': list(municipalities['municipality_id'])
        }
    }))

@app.route('/api/list/city_hierarchy', methods=['GET'])
@app.route('/api/list/city_hierarchy/', methods=['GET'])
def list_city_hierarchy() -> Response:
    local_hierarchy = city_hierarchy
    if 'location' in request.args:
        if request.args['location'] in city_hierarchy['district_full_name'].unique():
            local_hierarchy = local_hierarchy[local_hierarchy['district_full_name'] == request.args['location']]
        elif request.args['location'] in city_hierarchy['municipality_full_name'].unique():
            local_hierarchy = local_hierarchy[local_hierarchy['municipality_full_name'] == request.args['location']]
        elif request.args['location'].isnumeric():
            local_hierarchy = local_hierarchy[local_hierarchy['municipality_full_name'] == blocks.loc[int(request.args['location'])]['municipality']]
        else:
            return make_response(jsonify({'error': f"location '{request.args['location']}'' is not found in any of districts, municipalities or blocks"}), 400)
    
    districts = [{'id': id, 'full_name': full_name, 'short_name': short_name, 'population': population}
            for _, (id, full_name, short_name, population) in
                    local_hierarchy[['district_id', 'district_full_name', 'district_short_name', 'district_population']].drop_duplicates().iterrows()]
    for district in districts:
        district['municipalities'] = [{'id': id, 'full_name': full_name, 'short_name': short_name, 'population': population}
                for _, (id, full_name, short_name, population) in
                        local_hierarchy[local_hierarchy['district_id'] == district['id']][['municipality_id', 'municipality_full_name',
                                'municipality_short_name', 'municipality_population']].iterrows()]
    if 'include_blocks' in request.args:
        for district in districts:
            for municipality in district['municipalities']:
                municipality['blocks'] = [{'id': id, 'population': population} for id, population in
                        blocks[blocks['municipality'] == municipality['full_name']]['population'].items()]
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'districts': districts,
            'parameters': {
                'include_blocks': 'include_blocks' in request.args,
                'location': request.args.get('location')
            }
        }
    }))

@app.route('/', methods=['GET'])
@app.route('/api/', methods=['GET'])
def api_help() -> Response:
    return make_response(jsonify({
        'version': '2021-09-13',
        '_links': {
            'self': {
                'href': request.path
            },
            'atomic_provision': {
                'href': '/api/provision/atomic/{?social_group,living_situation,service_type,location}',
                'templated': True
            },
            'aggregated-provision': {
                'href': '/api/provision/aggregated/{?social_group,living_situation,service_type,location}',
                'templated': True
            },
            'alternative-aggregated-provision': {
                'href': '/api/provision/alternative/{?social_group,living_situation,service_type,location,return_debug_info}',
                'templated': True
            },
            'get-houses': {
                'href': '/api/houses/{?firstPoint,secondPoint}',
                'templated': True
            },
            'list-social_groups': {
                'href': '/api/list/social_groups/{?service_type,living_situation}',
                'templated': True
            },
            'list-living_situations': {
                'href': '/api/list/living_situations/{?social_group,service_type}',
                'templated': True
            },
            'list-city_functions': {
                'href': '/api/list/city_functions/{?social_group,living_situation}',
                'templated': True
            },
            'list-service_types': {
                'href': '/api/list/service_types/{?social_group,living_situation}',
                'templated': True
            },
            'list-city_hierarchy' :{
                'href': '/api/list/city_hierarchy/{?include_blocks,location}',
                'templated': True
            },
            'relevant-social_groups': {
                'href': '/api/relevance/social_groups/{?service_type,living_situation}',
                'templated': True
            },
            'relevant-living_situations': {
                'href': '/api/relevance/living_situations/{?social_group,service_type}',
                'templated': True
            },
            'relevant-city_functions': {
                'href': '/api/relevance/city_functions/{?social_group,living_situation}',
                'templated': True
            },
            'list-infrastructures': {
                'href': '/api/list/infrastructures/'
            },
            'list-districts': {
                'href': '/api/list/districts/'
            },
            'list-municipalities': {
                'href': '/api/list/municipalities/'
            },
            'ready_aggregations_houses': {
                'href': '/api/provision/ready/houses/{?social_group,living_situation,service_type,house}',
                'templated': True
            },
            'ready_aggregations_districts': {
                'href': '/api/provision/ready/districts/{?social_group,living_situation,service_type,district}',
                'templated': True
            },
            'ready_aggregations_municipalities': {
                'href': '/api/provision/ready/municipalities/{?social_group,living_situation,service_type,municipality}',
                'templated': True
            },
            'provision_v3_ready': {
                'href': '/api/provision_v3/ready/{?include_evaluation_scale}',
                'templated': True
            },
            'provision_v3_not_ready': {
                'href': '/api/provision_v3/not_ready/'
            },
            'provision_v3_services': {
                'href': '/api/provision_v3/services/{?service_type,location}',
                'templated': True
            },
            'provision_v3_service': {
                'href': '/api/provision_v3/service/{service_id}/',
                'templated': True
            },
            'provision_v3_houses' : {
                'href': '/api/provision_v3/houses{?service_type,location}',
                'templated': True
            },
            'provision_v3_house_service_types' : {
                'href': '/api/provision_v3/house/{house_id}/{?service_type}',
                'templated': True
            },
            'provision_v3_house_services': {
                'href': '/api/provision_v3/house_services/{house_id}/{?service_type}',
                'templated': True
            },
            'provision_v3_prosperity': {
                'href': '/api/provision_v3/prosperity/{?social_group,service_type,location}',
                'templated': True
            },
            'provision_v3_prosperity_districts': {
                'href': '/api/provision_v3/prosperity/districts/{?district,municipality,block,service_type,city_function,infrastructure,social_group,provision_only}',
                'templated': True
            },
            'provision_v3_prosperity_municipalities': {
                'href': '/api/provision_v3/prosperity/municipalities/{?district,municipality,block,service_type,city_function,infrastructure,social_group,provision_only}',
                'templated': True
            },
            'provision_v3_prosperity_blocks': {
                'href': '/api/provision_v3/prosperity/blocks/{?district,municipality,block,service_type,city_function,infrastructure,social_group,provision_only}',
                'templated': True
            }
        }
    }))

@app.route('/api/provision_v3/services', methods=['GET'])
@app.route('/api/provision_v3/services/', methods=['GET'])
def provision_v3_services() -> Response:
    service_type = request.args.get('service_type')
    if service_type and service_type.isnumeric():
        service_type = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in infrastructure['service_type_id'] else f'{service_type} (not found)'
    location = request.args.get('location')
    with properties.houses_conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(s.center), s.service_type, s.service_name, s.district_name, s.municipal_name, s.block_id, s.address,'
                '    v.houses_in_radius, v.people_in_radius, v.service_load, v.needed_capacity, v.reserve_resource, v.evaluation as provision,'
                '    v.functional_object_id as service_type_id'
                ' FROM all_services s JOIN provision.services v ON s.func_id = v.functional_object_id' + 
                (' WHERE s.service_type = %s' if 'service_type' in request.args else ''), ((service_type,) if 'service_type' in request.args else ()))
        df = pd.DataFrame(cur.fetchall(), columns=('center', 'service_type', 'service_name', 'district', 'municipality', 'block', 'address',
                'houses_in_access', 'people_in_access', 'service_load', 'needed_capacity', 'reserve_resource', 'provision', 'service_id'))
    df['center'] = df['center'].apply(json.loads)
    df['block'] = df['block'].replace({np.nan: None})
    df['address'] = df['address'].replace({np.nan: None})
    if location is not None:
        if location in city_hierarchy['district_full_name'].unique() or location in city_hierarchy['district_short_name'].unique():
            districts = city_hierarchy[['district_full_name', 'district_short_name']]
            df = df[df['district'] == districts[(districts['district_short_name'] == location) |\
                    (districts['district_full_name'] == location)]['district_short_name'].iloc[0]]
            df = df.drop('district', axis=True)
        elif location in city_hierarchy['municipality_full_name'].unique() or location in city_hierarchy['municipality_short_name'].unique():
            municipalities = city_hierarchy[['municipality_full_name', 'municipality_short_name']]
            df = df[df['municipality'] == municipalities[(municipalities['municipality_short_name'] == location) | \
                    (municipalities['municipality_full_name'] == location)]['municipality_short_name'].iloc[0]]
            df = df.drop(['district', 'municipality'], axis=True)
        else:
            location = f'Not found ({location})'
            df = df.drop(df.index)
    if 'service_type' in request.args:
        df = df.drop('service_type', axis=True)
    return make_response(jsonify({
        '_links': {
            'self': {'href': request.path},
            'service_info': {
                'href': '/api/provision_v3/service/{service_id}',
                'templated': True
            }
        },
        '_embedded': {
            'services': list(df.transpose().to_dict().values()),
            'parameters': {
                'service_type': service_type,
                'location': location
            }
        }
    }))

@app.route('/api/provision_v3/service/<int:service_id>', methods=['GET'])
@app.route('/api/provision_v3/service/<int:service_id>/', methods=['GET'])
def provision_v3_service_info(service_id: int) -> Response:
    service_info = dict()
    with properties.houses_conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(s.center), s.service_type, s.service_name, s.district_name, s.municipal_name, s.block_id, s.address,'
                '    v.houses_in_radius, v.people_in_radius, v.service_load, v.needed_capacity, v.reserve_resource, v.evaluation as provision'
                ' FROM all_services s JOIN provision.services v ON s.func_id = %s AND v.functional_object_id = %s', (service_id, service_id))
        res = cur.fetchone()
        if res is None:
            service_info['service_name'] = 'Not found'
        else:
            service_info['center'] = json.loads(res[0])
            service_info['service_type'] = res[1]
            service_info['service_name'] = res[2]
            service_info['district'] = res[3]
            service_info['municipality'] = res[4]
            service_info['block'] = res[5]
            service_info['address'] = res[6]
            service_info['houses_in_access'] = res[7]
            service_info['people_in_access'] = res[8]
            service_info['service_load'] = res[9]
            service_info['needed_capacity'] = res[10]
            service_info['reserve_resource'] = res[11]
            service_info['provision'] = res[12]
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'service': service_info,
            'parameters': {
                'service_id': service_id
            }
        }
    }), 404 if service_info['service_name'] == 'Not found' else 200)

@app.route('/api/provision_v3/houses', methods=['GET'])
@app.route('/api/provision_v3/houses/', methods=['GET'])
def provision_v3_houses() -> Response:
    service_type = request.args.get('service_type')
    if service_type and not service_type.isnumeric():
        service_type = int(infrastructure[infrastructure['service_type'] == service_type]['service_type_id'].iloc[0]) \
                if service_type in list(infrastructure['service_type']) else None
    elif service_type:
        service_type = int(service_type)
    location = request.args.get('location')
    location_tuple: Optional[Tuple[Literal['district', 'municipality'], int]] = None
    if location is not None:
        if location in city_hierarchy['district_full_name'].unique() or location in city_hierarchy['district_short_name'].unique():
            districts = city_hierarchy[['district_id', 'district_full_name', 'district_short_name']]
            location_tuple = 'district', int(districts[(districts['district_short_name'] == location) | \
                    (districts['district_full_name'] == location)]['district_id'].iloc[0])
        elif location in city_hierarchy['municipality_full_name'].unique() or location in city_hierarchy['municipality_short_name'].unique():
            municipalities = city_hierarchy[['municipality_id', 'municipality_full_name', 'municipality_short_name']]
            location_tuple = 'municipality', int(municipalities[(municipalities['municipality_short_name'] == location) | \
                    (municipalities['municipality_full_name'] == location)]['municipality_id'].iloc[0])
        else:
            location = f'{location} (not found)'
    if not location_tuple and not service_type:
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'houses': [],
                'parameters': {
                    'location': location,
                    'service_type': None
                },
                'error': "at least one of the 'service_type' and 'location' must be set in request"
            }
        }), 400)
    with properties.houses_conn.cursor() as cur:
        cur.execute('SELECT h.residential_object_id, h.address, ST_AsGeoJSON(h.center), d.full_name AS district, m.full_name AS municipality,'
                ' h.block_id, s.name as service_type, p.reserve_resource, p.provision FROM provision.houses p JOIN houses h ON p.house_id = h.id'
                ' JOIN districts d ON h.district_id = d.id JOIN municipalities m ON h.municipality_id = m.id'
                ' JOIN service_types s ON p.service_type_id = s.id' + 
                (' WHERE' if location_tuple or service_type else '') +
                (' d.id = %s ' if location_tuple and location_tuple[0] == 'district' else ' m.id = %s' \
                        if location_tuple and location_tuple[0] == 'municipality' else '') +
                ('AND' if location_tuple and service_type else '') +
                (' s.id = %s' if service_type else '') +
                ' ORDER BY p.service_type_id, h.id',
                (location_tuple[1], service_type) if location_tuple and service_type else (location_tuple[1],) \
                        if location_tuple else (service_type,) if service_type else tuple())
        houses = pd.DataFrame(cur.fetchall(),
                columns=('id', 'address', 'center', 'district', 'municipality', 'block', 'service_type', 'reserve_resource', 'provision')).set_index('id')
    houses['center'] = houses['center'].apply(lambda x: json.loads(x))
    gr = houses[['service_type', 'reserve_resource', 'provision']].groupby('id')
    houses.replace({np.nan: None})
    result = []
    for house_id in gr.groups:
        tmp = houses[['address', 'center', 'district', 'municipality', 'block']].loc[house_id]
        address, center, district, municipality, block = tmp.iloc[0] if isinstance(tmp, pd.DataFrame) else tmp
        service_types = [{'service_type': service_type, 'reserve_resources': reserve, 'provision': provision} \
                for _, (service_type, reserve, provision) in gr.get_group(house_id).iterrows()]
        result.append({
            'id': house_id,
            'address': address,
            'center': center,
            'district': district,
            'municipality': municipality,
            'block': int(block) if block == block else None,
            'service_types': service_types
        })
    return make_response(jsonify({
        '_links': {
            'self': {'href': request.path},
            'services': {'href': '/api/provision_v3/house_services/{house_id}/{?service_type}', 'templated': True},
            'house_info': {'href': '/api/provision_v3/house/{house_id}/{?service_type}', 'templated': True}
        },
        '_embedded': {
            'parameters': {
                'service_type': service_type,
                'location': location
            },
            'houses': result
        }
    }))

@app.route('/api/provision_v3/house/<int:house_id>', methods=['GET'])
@app.route('/api/provision_v3/house/<int:house_id>/', methods=['GET'])
def provision_v3_house(house_id: int) -> Response:
    house_info: Dict[str, Any] = dict()
    with properties.houses_conn.cursor() as cur:
        cur.execute('SELECT h.address, ST_AsGeoJSON(h.center), d.full_name AS district, m.full_name AS municipality,'
                ' h.block_id, s.name as service_type, p.reserve_resource, p.provision FROM provision.houses p JOIN houses h ON p.house_id = h.id'
                ' JOIN districts d ON h.district_id = d.id JOIN municipalities m ON h.municipality_id = m.id'
                ' JOIN service_types s ON p.service_type_id = s.id' + 
                ' WHERE h.residential_object_id = %s' + 
                (' AND s.name = %s' if 'service_type' in request.args and not request.args['service_type'].isnumeric() else \
                        ' AND s.id = %s' if 'service_type' in request.args else ''),
                (house_id, request.args['service_type']) if 'service_type' in request.args else (house_id,))
        house_service_types = pd.DataFrame(cur.fetchall(),
                columns=('address', 'center', 'district', 'municipality', 'block', 'service_type', 'reserve_resource', 'provision'))
        if house_service_types.shape[0] == 0:
            house_info['address'] = 'Not found'
        else:
            tmp = house_service_types[['address', 'center', 'district', 'municipality', 'block']].iloc[0]
            house_info['address'], house_info['center'], house_info['district'], house_info['municipality'], house_info['block'] = \
                    tmp.iloc[0] if isinstance(tmp, pd.DataFrame) else tmp
            house_info['center'] = json.loads(house_info['center'])
            house_info['block'] = int(house_info['block']) if house_info['block'] == house_info['block'] else None
            house_info['services'] = [{'service_type': service_type, 'reserve_resources': reserve, 'provision': provision} \
                    for _, (service_type, reserve, provision) in house_service_types[['service_type', 'reserve_resource', 'provision']].iterrows()]
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'house': house_info,
            'parameters': {
                'house_id': house_id,
                'service_type': request.args.get('service_type')
            }
        }
    }), 404 if house_info['address'] == 'Not found' else 200)

@app.route('/api/provision_v3/house_services/<int:house_id>', methods=['GET'])
@app.route('/api/provision_v3/house_services/<int:house_id>/', methods=['GET'])
def house_services(house_id: int) -> Response:
    service_type = request.args.get('service_type')
    if service_type and service_type.isnumeric():
        service_type = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in list(infrastructure['service_type_id']) else None
    with properties.houses_conn.cursor() as cur:
        if 'service_type' in request.args:
            cur.execute('SELECT a.func_id, a.service_name, ST_AsGeoJSON(a.center), hs.load FROM provision.houses_services hs JOIN all_services a'
                    ' ON hs.functional_object_id = a.func_id WHERE hs.house_id = %s'
                    ' AND a.service_type = %s', (house_id, service_type))
            services = [{'id': func_id, 'name': name, 'center': json.loads(center), 'load': load} for func_id, name, center, load in cur.fetchall()]
        else:
            cur.execute('SELECT a.func_id, a.service_name, ST_AsGeoJSON(a.center), a.service_type, hs.load FROM provision.houses_services hs JOIN all_services a'
                    ' ON hs.functional_object_id = a.func_id WHERE hs.house_id = %s',
                    (house_id,))
            services = [{'id': func_id, 'name': name, 'center': json.loads(center), 'service_type': service_type, 'load': load} for
                    func_id, name, center, service_type, load in cur.fetchall()]
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'house_id': house_id,
                'service_type': service_type
            },
            'services': services
        }
    }))

@app.route('/api/provision_v3/ready', methods=['GET'])
@app.route('/api/provision_v3/ready/', methods=['GET'])
def provision_v3_ready() -> Response:
    with properties.houses_conn.cursor() as cur:
        cur.execute('SELECT c.service_type, c.count, n.normative, n.max_load, n.radius_meters, '
                '   n.public_transport_time, n.service_evaluation, n.house_evaluation'
                ' FROM (SELECT s.service_type, count(*) FROM all_services s'
                '       JOIN provision.services v ON s.func_id = v.functional_object_id GROUP BY s.service_type) as c'
                '   JOIN service_types st ON c.service_type = st.name'
                '   LEFT JOIN provision.normatives n ON n.service_type_id = st.id')
        df = pd.DataFrame(cur.fetchall(), columns=('service_type', 'count', 'normative', 'max_load', 'radius_meters',
                'public_transport_time', 'service_evaluation', 'house_evaluation'))
        if not 'include_evaluation_scale' in request.args:
            df = df.drop(['service_evaluation', 'house_evaluation'], axis=True)
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'ready': list(df.replace({np.nan: None}).transpose().to_dict().values())
            }
        }))

@app.route('/api/provision_v3/not_ready', methods=['GET'])
@app.route('/api/provision_v3/not_ready/', methods=['GET'])
def provision_v3_not_ready() -> Response:
    with properties.houses_conn.cursor() as cur:
        cur.execute('SELECT st.name as service_type, s.count AS unevaluated, c.count AS total'
                ' FROM (SELECT service_type_id, count(*)'
                '   FROM functional_objects where id not in'
                '       (SELECT functional_object_id from provision.services) GROUP BY service_type_id ORDER BY 1) AS s'
                ' JOIN service_types st ON s.service_type_id = st.id'
                ' JOIN (SELECT service_type_id, count(*) FROM functional_objects'
                ' GROUP BY service_type_id) AS c ON c.service_type_id = st.id'
                ' ORDER BY 1')
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'ready': [{'service_type': service_type, 'unevaluated': unevaluated, 'total_count': total} for service_type, unevaluated, total in cur.fetchall()]
            }
        }))

@app.route('/api/provision_v3/prosperity', methods=['GET'])
@app.route('/api/provision_v3/prosperity/', methods=['GET'])
def provision_v3_prosperity() -> Response:
    error: Optional[str] = None
    if not ('social_group' in request.args and 'service_type' in request.args and 'location' in request.args):
        error = "Request must include all of the ('social_group', 'service_type', 'location') arguments"
    
    parameters: Dict[str, Optional[str]] = {
        'social_group': request.args.get('social_group'),
        'service_type': request.args.get('service_type'),
        'location': request.args.get('location')
    }
    social_group = request.args.get('social_group')
    if social_group and social_group.isnumeric():
        parameters['social_group'] = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    service_type = request.args.get('service_type')
    if service_type and service_type.isnumeric():
        parameters['service_type'] = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in infrastructure['service_type_id'] else 'None'
    del social_group, service_type

    provision: Optional[float] = None
    if parameters['location'] and parameters['service_type']:
        res = pd.Series(dtype=object)
        if parameters['location'] in city_hierarchy['district_full_name'].unique() or parameters['location'] in city_hierarchy['district_short_name'].unique():
            districts = city_hierarchy[['district_full_name', 'district_short_name']]
            res = provision_districts[(provision_districts['district'] == districts[(districts['district_short_name'] == parameters['location']) | \
                    (districts['district_full_name'] == parameters['location'])]['district_short_name'].iloc[0]) &
                    (provision_districts['service_type'] == parameters['service_type'])]
        elif parameters['location'] in city_hierarchy['municipality_full_name'].unique() or parameters['location'] in city_hierarchy['municipality_short_name']:
            municipalities = city_hierarchy[['municipality_full_name', 'municipality_short_name']]
            res = provision_municipalities[(provision_municipalities['municipality'] == municipalities[(municipalities['municipality_short_name'] == parameters['location']) | \
                    (municipalities['municipality_full_name'] == parameters['location'])]['municipality_short_name'].iloc[0]) &
                    (provision_municipalities['service_type'] == parameters['service_type'])]
        else:
            error = '; '.join((*filter(lambda x: x is not None, (error,)), # type: ignore
                        'cannot determine type of location. Should be one of the districts or municipalities'))
        if res.shape[0] != 0:
            provision = round(res['provision'].mean(), 2)
        else:
            error = '; '.join((*filter(lambda x: x is not None, (error,)), # type: ignore
                    'significane is not found for a given combinaton of social_group and service_type'))

    significance: Optional[float] = None
    if parameters['social_group'] and parameters['service_type']:
        n = needs['significance'][(needs['social_group'] == parameters['social_group']) & (needs['service_type'] == parameters['service_type'])]

        if n.shape[0] == 0:
            error = '; '.join((*filter(lambda x: x is not None, (error,)), # type: ignore
                    'significane is not found for a given combinaton of social_group and service_type'))
        else:
            significance = n.iloc[0]


    if error is not None:
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'prosperity': {
                    'provision': provision,
                    'significance': significance,
                    'prosperity': None
                },
                'error': error,
                'parameters': parameters
            }
        }), 400)

    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'prosperity': {
                'provision': provision,
                'significance': significance,
                'prosperity': round(10 + significance * (provision - 10), 2) # type: ignore
            },
            'parameters': parameters
        }
    }))

@app.route('/api/provision_v3/prosperity/<location_type>', methods=['GET'])
@app.route('/api/provision_v3/prosperity/<location_type>/', methods=['GET'])
def provision_v3_prosperity_multiple(location_type: str) -> Response:
    if location_type not in ('districts', 'municipalities', 'blocks'):
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'error': f"location_type must be 'blocks', 'districts' or 'municipalities', but '{location_type}' is given"
            }
        }), 400)
    social_group: str = request.args.get('social_group', 'all')
    if social_group.isnumeric():
        social_group = listings.social_groups[listings.social_groups['id'] == int(social_group)]['name'].iloc[0] \
                if int(social_group) in listings.social_groups['id'] else 'None'
    service_type: Optional[str] = request.args.get('service_type')
    if service_type and service_type.isnumeric():
        service_type = infrastructure[infrastructure['service_type_id'] == int(service_type)]['service_type'].iloc[0] \
                if int(service_type) in list(infrastructure['service_type_id']) else 'None'
    city_function: Optional[str] = request.args.get('city_function')
    if city_function and city_function.isnumeric():
        city_function = infrastructure[infrastructure['function'] == int(city_function)]['function'].iloc[0] \
                if int(city_function) in list(infrastructure['function'].unique()) else 'None'
    infra: Optional[str] = request.args.get('infrastructure')
    if infra and infra.isnumeric():
        infra = infrastructure[infrastructure['infrastructure'] == int(infra)]['infrastructure'].iloc[0] \
                if int(infra) in list(infrastructure['infrastructure'].unique()) else 'None'
    if 'provision_only' in request.args and request.args['provision_only'] not in ('0', 'false', '-', 'no'):
        provision_only = True
    else:
        provision_only = False

    aggregation_type = 'service_type'
    aggregation_value = service_type
    if service_type is None and city_function is None and infra is None:
        service_type = 'all'
        aggregation_value = 'all'
    elif service_type: 
        city_function = infra = None
    elif city_function:
        infra = None
        aggregation_type = 'function'
        aggregation_value = city_function
    else:
        aggregation_type = 'infrastructure'
        aggregation_value = infra

    district: Optional[str] = request.args.get('district', 'all')
    if district:
        if district.isnumeric():
            district = city_hierarchy[city_hierarchy['district_id'] == int(district)]['district_full_name'].iloc[0] \
                    if int(district) in city_hierarchy['district_id'] else 'None'
        elif district in city_hierarchy['district_short_name'].unique():
            district = city_hierarchy[city_hierarchy['district_short_name'] == district]['district_full_name'].iloc[0]
    municipality: Optional[str] = request.args.get('municipality', 'all')
    if municipality:
        if municipality.isnumeric():
            municipality = city_hierarchy[city_hierarchy['municipality_id'] == int(municipality)]['municipality_full_name'].iloc[0] \
                    if int(municipality) in city_hierarchy['municipality_id'] else 'None'
        elif municipality in city_hierarchy['municipality_short_name'].unique():
            municipality = city_hierarchy[city_hierarchy['municipality_short_name'] == municipality]['municipality_full_name'].iloc[0]
    block: Optional[Union[str, int]] = request.args.get('block', 'all')
    if block and block != 'all':
        if block.isnumeric(): # type: ignore
            block = int(block)
        elif block != 'mean':
            block = 'all'

    location_type_single = 'district' if location_type == 'districts' else 'municipality' if location_type == 'municipalities' else 'block'

    if location_type == 'districts':
        res = provision_districts
        if municipality == 'mean':
            municipality = None
        if block == 'mean':
            block = None
    elif location_type == 'municipalities':
        res = provision_municipalities
        if district == 'all' or district == 'mean':
            district = None
        if block == 'mean':
            block = None
    else:
        res = provision_blocks
        if district == 'all' or district == 'mean':
            district = None
        if municipality == 'all' or municipality == 'mean':
            municipality = None

    if location_type == 'blocks':
        if block and block not in ('all', 'mean'):
            res = res[res['block'] == block]
        elif municipality and municipality not in ('all', 'mean'):
            res = res[res['block'].isin(blocks[blocks['municipality'] == municipality].index)]
        elif district and district not in ('all', 'mean'):
            res = res[res['block'].isin(blocks[blocks['district'] == district].index)]
    elif location_type == 'municipalities':
        if block and block not in ('all', 'mean'):
            res = res[res['municipality'] == blocks[blocks['municipality'].loc[block]]]
        elif municipality and municipality not in ('all', 'mean'):
            res = res[res['municipality'] == municipality]
        elif district and district not in ('all', 'mean'):
            res = res[res['municipality'].isin(city_hierarchy[city_hierarchy['district_full_name'] == district]['municipality_full_name'])]
    else:
        if block and block not in ('all', 'mean'):
            res = res[res['district'] == blocks[blocks['district'].loc[block]]]
        elif municipality and municipality not in ('all', 'mean'):
            res = res[res['district'] == city_hierarchy[city_hierarchy['municipality_full_name'] == municipality]['district_full_name'].iloc[0]]
        elif district and district not in ('all', 'mean'):
            res = res[res['district'] == district]


    if not provision_only:
        n: pd.DataFrame = needs[['service_type', 'social_group', 'significance']].merge(
                infrastructure[['service_type', 'function', 'infrastructure']], how='inner', on='service_type')[[aggregation_type, 'social_group', 'significance']]
        n = n.groupby([aggregation_type, 'social_group']).mean().reset_index()
        if social_group not in ('all', 'mean'):
            n = n[n['social_group'] == social_group][[aggregation_type, 'significance']]

        res = res.merge(infrastructure[['service_type', 'function', 'infrastructure']], how='inner', on='service_type') \
                [[location_type_single, aggregation_type, 'houses_count', 'services_count', 'services_load_mean', 'services_load_sum',
                        'houses_reserve_mean', 'houses_reserve_sum', 'services_reserve_mean', 'services_reserve_sum', 'houses_provision', 'services_evaluation']]
        res = res.merge(n, how='inner', on=aggregation_type)
    else:
        res = res.merge(infrastructure[['service_type', 'function', 'infrastructure']], how='inner', on='service_type') \
                [[location_type_single, aggregation_type, 'houses_count', 'services_count', 'services_load_mean', 'services_load_sum',
                        'houses_reserve_mean', 'houses_reserve_sum', 'services_reserve_mean', 'services_reserve_sum', 'houses_provision', 'services_evaluation']]

    if aggregation_value not in ('all', 'mean'):
        res = res[res[aggregation_type] == aggregation_value]

    aggregation_labels: List[str] = []
    if aggregation_value == 'mean':
        aggregation_labels.append(aggregation_type)
    if social_group == 'mean' and not provision_only:
        aggregation_labels.append('social_group')
    if district == 'mean' or municipality == 'mean' or block == 'mean':
        aggregation_labels.append(location_type_single)

    if res[[location_type_single, aggregation_type]].drop_duplicates().shape[0] != res.shape[0]:
        for column in ('services_load_mean', 'services_reserve_mean', 'services_evaluation'):
            res[column] *= res['services_count']
        for column in ('houses_reserve_mean', 'houses_provision'):
            res[column] *= res['houses_count']
        if not provision_only:
            res['significance'] *= res['services_count']
            gr = res.groupby([location_type_single, aggregation_type, 'social_group'])
        else:
            gr = res.groupby([location_type_single, aggregation_type])
        res = gr.sum()[['houses_count', 'services_count', 'services_load_mean', 'services_load_sum', 'services_reserve_mean', 'services_reserve_sum',
                'houses_reserve_mean', 'houses_reserve_sum', 'houses_provision', 'services_evaluation']]
        if not provision_only:
            res = res.join(gr.sum()['significance'])
            res['significance'] /= res['services_count'].replace({0: 1})
        res = res.reset_index()
        for column in ('services_load_mean', 'services_reserve_mean', 'services_evaluation'):
            res[column] /= res['services_count'].replace({0 : 1})
        for column in ('houses_reserve_mean', 'houses_provision'):
            res[column] /= res['houses_count'].replace({0 : 1})

    if len(aggregation_labels) != 0:
        res = res.drop(aggregation_labels, axis=True)
        aggr = set(res.columns) - {'houses_provision', 'services_evaluation', 'significance', 'houses_count', 'services_count', 'services_load_mean', 'services_load_sum',
                'houses_reserve_mean', 'houses_reserve_sum', 'services_reserve_mean', 'services_reserve_sum'} - set(aggregation_labels)
        for column in ('services_load_mean', 'services_reserve_mean', 'services_evaluation'):
            res[column] *= res['services_count']
        for column in ('houses_reserve_mean', 'houses_provision'):
            res[column] *= res['houses_count'].replace({0 : 1})
        if not provision_only:
            res['significance'] *= res['services_count']
        if len(aggr) == 0:
            res = pd.DataFrame(res.sum()).transpose()
        else:
            res = res.groupby(list(aggr)).sum().reset_index()
        for column in ('services_load_mean', 'services_reserve_mean', 'services_evaluation'):
            res[column] /= res['services_count']
        for column in ('houses_reserve_mean', 'houses_provision'):
            res[column] /= res['houses_count'].replace({0 : 1})
        if not provision_only:
            res['significance'] /= res['services_count'].replace({0: 1})

    if not provision_only:
        res['prosperity'] = (10 + res['significance'] * (res['houses_provision'] - 10)).apply(lambda x: round(x, 2))
        res['significance'] = res['significance'].apply(lambda x: round(x, 2))
    for column in ('services_load_mean', 'houses_reserve_mean', 'houses_reserve_sum', 'services_reserve_mean', 'services_reserve_sum',
            'houses_provision', 'services_evaluation'):
        res[column] = res[column].apply(lambda x: round(x, 2))
    
    parameters: Dict[str, Any] = {
        'aggregation_type': aggregation_type,
        'aggregation_value': aggregation_value,
    }
    if municipality or location_type == 'municipalities':
        parameters['municipality'] = municipality
    if district or location_type == 'districts':
        parameters['district'] = district
    if block or location_type == 'blocks':
        parameters['block'] = block
    if not provision_only:
        parameters['social_group'] = social_group

    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'prosperity': list(res.transpose().replace({np.nan: None}).to_dict().values()),
            'parameters': parameters
        }
    }))

@app.errorhandler(404)
def not_found(_):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.errorhandler(Exception)
def any_error(error: Exception):
    properties.houses_conn.rollback()
    properties.provision_conn.rollback()
    print(f'{request.path}?{"&".join(map(lambda x: f"{x[0]}={x[1]}", request.args.items()))}')
    traceback.print_exc()
    return make_response(jsonify({
        'error': str(error),
        'error_type': str(type(error)),
        'path': request.path,
        'params': '&'.join(map(lambda x: f'{x[0]}={x[1]}', request.args.items())),
        'trace': list(itertools.chain.from_iterable(map(lambda x: x.split('\n'), traceback.format_tb(error.__traceback__))))
    }), 500)


if __name__ == '__main__':

    # Default properties settings

    properties = Properties(
            'localhost', 5432, 'provision', 'postgres', 'postgres', 
            'localhost', 5432, 'citydb', 'postgres', 'postgres',
            8080, 'http://10.32.1.61:8080/api.v2/isochrones'
    )
    aggregate_target = '-'

    # Environment variables

    if 'PROVISION_API_PORT' in os.environ:
        properties.api_port = int(os.environ['PROVISION_API_PORT'])
    if 'PROVISION_DB_ADDR' in os.environ:
        properties.provision_db_addr = os.environ['PROVISION_DB_ADDR']
    if 'PROVISION_DB_NAME' in os.environ:
        properties.provision_db_name = os.environ['PROVISION_DB_NAME']
    if 'PROVISION_DB_PORT' in os.environ:
        properties.provision_db_port = int(os.environ['PROVISION_DB_PORT'])
    if 'PROVISION_DB_USER' in os.environ:
        properties.provision_db_user = os.environ['PROVISION_DB_USER']
    if 'PROVISION_DB_PASS' in os.environ:
        properties.provision_db_pass = os.environ['PROVISION_DB_PASS']
    if 'HOUSES_DB_ADDR' in os.environ:
        properties.houses_db_addr = os.environ['HOUSES_DB_ADDR']
    if 'HOUSES_DB_NAME' in os.environ:
        properties.houses_db_name = os.environ['HOUSES_DB_NAME']
    if 'HOUSES_DB_PORT' in os.environ:
        properties.houses_db_port = int(os.environ['HOUSES_DB_PORT'])
    if 'HOUSES_DB_USER' in os.environ:
        properties.houses_db_user = os.environ['HOUSES_DB_USER']
    if 'HOUSES_DB_PASS' in os.environ:
        properties.houses_db_pass = os.environ['HOUSES_DB_PASS']
    if 'PROVISION_AGGREGATE' in os.environ:
        aggregate_target = os.environ['PROVISION_AGGREGATE']
    if 'TRANSPORT_MODEL_ADDR' in os.environ:
        properties.transport_model_api_endpoint = os.environ['TRANSPORT_MODEL_ADDR']

    # CLI Arguments

    parser = argparse.ArgumentParser(
        description='Starts up the provision API server')
    parser.add_argument('-pH', '--provision_db_addr', action='store', dest='provision_db_addr',
                        help=f'postgres host address [default: {properties.provision_db_addr}]', type=str)
    parser.add_argument('-pP', '--provision_db_port', action='store', dest='provision_db_port',
                        help=f'postgres port number [default: {properties.provision_db_port}]', type=int)
    parser.add_argument('-pd', '--provision_db_name', action='store', dest='provision_db_name',
                        help=f'postgres database name [default: {properties.provision_db_name}]', type=str)
    parser.add_argument('-pU', '--provision_db_user', action='store', dest='provision_db_user',
                        help=f'postgres user name [default: {properties.provision_db_user}]', type=str)
    parser.add_argument('-pW', '--provision_db_pass', action='store', dest='provision_db_pass',
                        help=f'database user password [default: {properties.provision_db_pass}]', type=str)
    parser.add_argument('-hH', '--houses_db_addr', action='store', dest='houses_db_addr',
                        help=f'postgres host address [default: {properties.houses_db_addr}]', type=str)
    parser.add_argument('-hP', '--houses_db_port', action='store', dest='houses_db_port',
                        help=f'postgres port number [default: {properties.houses_db_port}]', type=int)
    parser.add_argument('-hd', '--houses_db_name', action='store', dest='houses_db_name',
                        help=f'postgres database name [default: {properties.houses_db_name}]', type=str)
    parser.add_argument('-hU', '--houses_db_user', action='store', dest='houses_db_user',
                        help=f'postgres user name [default: {properties.houses_db_user}]', type=str)
    parser.add_argument('-hW', '--houses_db_pass', action='store', dest='houses_db_pass',
                        help=f'database user password [default: {properties.houses_db_pass}]', type=str)
    parser.add_argument('-p', '--port', action='store', dest='api_port',
                        help=f'postgres port number [default: {properties.api_port}]', type=int)
    parser.add_argument('-t', '--aggregate_target', action='store', dest='aggregate_target',
                        help=f'aggregate municipality, district with municipalities or everything [default: {aggregate_target}', type=str)
    parser.add_argument('-T', '--transport_model_api', action='store', dest='transport_model_api_endpoint',
                        help=f'url of transport model api [default: {properties.transport_model_api_endpoint}]', type=str)
    parser.add_argument('-D', '--debug', action='store_true', dest='debug', help=f'debug trigger')
    args = parser.parse_args()

    if args.provision_db_addr is not None:
        properties.provision_db_addr = args.provision_db_addr
    if args.provision_db_port is not None:
        properties.provision_db_port = args.provision_db_port
    if args.provision_db_name is not None:
        properties.provision_db_name = args.provision_db_name
    if args.provision_db_user is not None:
        properties.provision_db_user = args.provision_db_user
    if args.provision_db_pass is not None:
        properties.provision_db_pass = args.provision_db_pass
    if args.houses_db_addr is not None:
        properties.houses_db_addr = args.houses_db_addr
    if args.houses_db_port is not None:
        properties.houses_db_port = args.houses_db_port
    if args.houses_db_name is not None:
        properties.houses_db_name = args.houses_db_name
    if args.houses_db_user is not None:
        properties.houses_db_user = args.houses_db_user
    if args.houses_db_pass is not None:
        properties.houses_db_pass = args.houses_db_pass
    if args.api_port is not None:
        properties.api_port = args.api_port
    if args.aggregate_target:
        aggregate_target = args.aggregate_target
    
    print('Getting global data')

    update_global_data()

    avaliability = Avaliability()

    if aggregate_target == '-':
        print('Skipping aggregation')
    else:
        print(f'Starting aggregation in 2 seconds: aggregating {aggregate_target}')
        time.sleep(2)
        update_aggregation(aggregate_target, True)

    print(f'Starting application on 0.0.0.0:{properties.api_port} with houses DB ({properties.houses_db_user}@{properties.houses_db_addr}:{properties.houses_db_port}/{properties.houses_db_name}) and'
        f' provision DB ({properties.provision_db_user}@{properties.provision_db_addr}:{properties.provision_db_port}/{properties.provision_db_name}).')
    print(f'Transport model API endpoint is {properties.transport_model_api_endpoint}')

    if args.debug:
        app.run(host='0.0.0.0', port=properties.api_port, debug=args.debug)
    else:
        import gevent.pywsgi

        app_server = gevent.pywsgi.WSGIServer(('0.0.0.0', properties.api_port), app)
        try:
            app_server.serve_forever()
        except KeyboardInterrupt:
            print('Finishing')
            app_server.stop()
