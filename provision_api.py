from multiprocessing import Pipe
from multiprocessing.connection import Connection
import traceback
from flask import Flask, jsonify, make_response, request, Response
from flask_compress import Compress
import psycopg2
import pandas as pd, numpy as np
import argparse
import json
import requests
import time
import sys, os, threading, multiprocessing
from typing import Any, Tuple, List, Dict, Optional, Union

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
    def provision_conn_string(self) -> str:
        return f'host={self.provision_db_addr} port={self.provision_db_port} dbname={self.provision_db_name}' \
                f' user={self.provision_db_user} password={self.provision_db_pass}'
    def houses_conn_string(self) -> str:
        return f'host={self.houses_db_addr} port={self.houses_db_port} dbname={self.houses_db_name}' \
                f' user={self.houses_db_user} password={self.houses_db_pass}'

class Avaliability:
    def __init__(self):
        self.conn = psycopg2.connect(properties.provision_conn_string())

    def get_walking(self, lat: float, lan: float, t: int, pipe: Optional[Connection] = None) -> str:
        if t == 0:
            res = json.dumps({
                    'type': 'Polygon',
                    'coordinates': []
            })
            if pipe is not None:
                pipe.send(res)
            return res
        cur = self.conn.cursor()
        cur.execute(f'select ST_AsGeoJSON(geometry) from walking where latitude = {lat} and longitude = {lan} and time = {t}')
        res = cur.fetchall()
        if len(res) != 0:
            if pipe is not None:
                pipe.send(res[0])
            return res[0][0]
        try:
            print(f'downloading walking for {lat}, {lan}, {t}')
            ans = json.dumps(
                requests.get(f'https://galton.urbica.co/api/foot/?lng={lat}&lat={lan}&radius=5&cellSize=0.1&intervals={t}', timeout=15).json()['features'][0]['geometry']
            )
        except Exception:
            ans = json.dumps({'type': 'Polygon', 'coordinates': []})
        if round(lat, 3) == lat and round(lan, 3) == lan:
            cur.execute(f"insert into walking (latitude, longitude, time, geometry) values ({lat}, {lan}, {t}, ST_SetSRID(ST_GeomFromGeoJSON('{ans}'::text), 4326))")
            self.conn.commit()
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
        cur = self.conn.cursor()
        cur.execute(f'select ST_AsGeoJSON(geometry) from transport where latitude = {lat} and longitude = {lan} and time = {t}')
        res = cur.fetchall()
        if len(res) != 0:
            if pipe is not None:
                pipe.send(res[0][0])
            return res[0][0]
        if t >= 60:
            cur.execute(f'select ST_AsGeoJSON(geometry) from transport where time = {t} limit 1')
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

        if round(lat, 3) == lat and round(lan, 3) == lan:
            cur.execute('INSERT INTO transport (latitude, longitude, time, geometry) VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))', (lat, lan, t, ans))
            self.conn.commit()
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
        cur = self.conn.cursor()
        cur.execute(f'select ST_AsGeoJSON(geometry) from car where latitude = {lat} and longitude = {lan} and time = {t}')
        res = cur.fetchall()
        if len(res) != 0:
            if pipe is not None:
                pipe.send(res[0][0])
            return res[0][0]
        if t >= 60:
            cur.execute(f'select ST_AsGeoJSON(geometry) from car where time = {t} limit 1')
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

        if round(lat, 3) == lat and round(lan, 3) == lan:
            cur.execute('INSERT INTO car (latitude, longitude, time, geometry) VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))', (lat, lan, t, ans))
            self.conn.commit()
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

database_needs_sql = '''select gr.name as social_group_name, sit.name as living_situation, fun.name as city_function, 
        walking, public_transport, personal_transport, intensity from needs need
	inner join living_situations sit on need.living_situation_id = sit.id
	inner join social_groups gr on need.social_group_id = gr.id
	inner join city_functions fun on need.city_function_id = fun.id'''

database_significance_sql = '''select gr.name as social_group_name, fun.name as city_function_name, significance from values val
	inner join social_groups gr on val.social_group_id = gr.id
	inner join city_functions fun on val.city_function_id = fun.id'''

social_groups: List[str]
living_situations: List[str]
municipalities: List[str]
districts: List[str]
needs_table: Dict[Tuple[str, str, str], Tuple[int, int, int, int]]
values_table: Dict[Tuple[str, str], float]
all_houses: pd.DataFrame

function_service: Dict[str, Tuple[Optional[Tuple[str, str]], ...]] = {
    'Жилье': (('houses', 'жилой дом'),),
    'Мусор': tuple(),
    'Перемещение по городу': tuple(),
    'Образование': (('kindergartens', 'детский сад'), ('schools', 'школа'), ('colleges', 'колледж'), ('universities', 'университет')),
    'Здравоохранение': (('clinics', 'клиника'), ('hospitals', 'больница'), ('pharmacies', 'аптека'), ('woman_doctors', 'женская консультация'),
        ('health_centers', 'медицинский центр'), ('maternity_hospitals', 'роддом'), ('dentists', 'стоматология')),
    'Религия': (('cemeteries', 'кладбище'), ('churches', 'церковь')),
    'Социальное обслуживание': tuple(),
    'Личный транспорт': (('gas_stations', 'автозаправка'), ('charging_stations', 'электрозаправка')),
    'Уход за собой': tuple(),
    'Продовольствие': (('markets', 'рынок'), ('supermarkets', 'супермаркет'), ('hypermarkets', 'гипермаркет'),
        ('conveniences', 'магазин у дома'), ('department_stores', 'универсам')),
    'Финансы': tuple(),
    'Хозяйственные товары': tuple(),
    'Ремонт': tuple(),
    'Товары': tuple(),
    'Питомцы': (('veterinaries', 'ветеринарный магазин'),),
    'Третье место': (('playgrounds', 'игровая площадка'), ('art_spaces', 'креативное пространство')),
    'Культура': (('zoos', 'зоопарк'), ('theaters', 'театр'), ('museums', 'музей'), ('libraries', 'библиотека')),
    'Спорт': (('swimming_pools', 'бассейн'), ('sports_sections', 'спортивная секция')),
    'Развлечения': (('cinemas', 'кинотеатр'),),
    'Питание': (('bars', 'бар'), ('bakeries', 'булочная'), ('cafes', 'кафе'), ('restaurants', 'ресторан'), ('fastfood', 'фастфуд')),
    'Товары для туристов': tuple(),
    'Жилье для приезжих': tuple(),
    'Достопримечательности': tuple(),
    'Точки притяжения': tuple(),
    'Офис': tuple(),
    'Промышленность': tuple(),
    'Специализированные учреждения': tuple()
}

city_functions: List[str] = list(map(lambda x: x[0], filter(lambda x: len(x[1]) != 0, function_service.items())))

def compute_atomic_provision(conn_houses: psycopg2.extensions.connection, conn_provision: psycopg2.extensions.connection,
        soc_group: str, situation: str, function: str, coords: Tuple[float, float], use_database: Optional[bool] = False,
        **kwargs) -> Dict[str, Any]:
    cur_houses: psycopg2.excensions.cursor = conn_houses.cursor()
    cur_provision: psycopg2.excensions.cursor = conn_provision.cursor()

    if (soc_group, situation, function) not in needs_table:
        print(f'No data found for needs (soc_group = {soc_group}, situation = {situation}, function = {function}')
        raise Exception(f'No data found for needs (soc_group = {soc_group}, situation = {situation}, function = {function}')
    if (soc_group, function) not in values_table:
        print(f'No data found for values (soc_group = {soc_group}, function = {function})')
        raise Exception(f'No data found for values (soc_group = {soc_group}, function = {function})')

    walking_time_cost, transport_time_cost, personal_transport_time_cost, intensity = needs_table[(soc_group, situation, function)]
    significance = values_table[(soc_group, function)]

    if 'walking_time_cost' in kwargs:
        walking_time_cost = int(kwargs['walking_time_cost'])
    if 'transport_time_cost' in kwargs:
        transport_time_cost = int(kwargs['transport_time_cost'])
    if 'personal_transport_time_cost' in kwargs:
        personal_transport_time_cost = int(kwargs['personal_transport_time_cost'])
    walking_availability = float(kwargs.get('walking_availability', 1))
    public_transport_availability_multiplier = float(kwargs.get('public_transport_availability_multiplier', 1))
    personal_transport_availability_multiplier = float(kwargs.get('personal_transport_availability_multiplier', 0))
    max_target_s = float(kwargs.get('max_target_s', 30.0))
    target_s_divider = float(kwargs.get('target_s_divider', 6))
    coeff_multiplier = float(kwargs.get('coeff_multiplier', 5))

    if walking_time_cost == 0 and transport_time_cost == 0:
        return {
            'walking_geometry': json.dumps({'type': 'Polygon', 'coordinates': []}),
            'transport_geometry': json.dumps({'type': 'Polygon', 'coordinates': []}),
            'car_geometry': json.dumps({'type': 'Polygon', 'coordinates': []}),
            'services': dict(),
            'provision_result': 0.0,
            'parameters': {
                'walking_time_cost': walking_time_cost,
                'transport_time_cost': transport_time_cost,
                'personal_transport_time_cost': personal_transport_time_cost,
                'intensity': intensity,
                'significance': significance
            }
        }

    # walking_geometry, transport_geometry, car_geometry = avaliability.ensure_ready(*coords, walking_time_cost, transport_time_cost, personal_transport_time_cost)

    # Walking

    walking_geometry = avaliability.get_walking(*coords, walking_time_cost)

    cur_houses.execute('\nUNION\n'.join(
        map(lambda function_and_name: f"SELECT id, address, name, ST_AsGeoJSON(ST_Centroid(geometry)), capacity as power, '{function_and_name[1]}' as service_type FROM {function_and_name[0]} WHERE ST_WITHIN(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))", # type: ignore
            function_service[function])), [walking_geometry] * len(function_service[function]))
    walking_data: List[List[Tuple[int, str, str, int, str]]] = cur_houses.fetchall()
    walking_ids = set(map(lambda id_and_others: id_and_others[0], walking_data))

    df_target_servs = pd.DataFrame(walking_data, columns=('service_id', 'address', 'service_name', 'point', 'power', 'service_type'))
    df_target_servs['point'] = pd.Series(
        map(lambda geojson: (round(float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), 4), round(float(geojson[geojson.rfind(',') + 1:-2]), 4)),
            df_target_servs['point'])
    )
    df_target_servs = df_target_servs.join(pd.Series(['walking'] * df_target_servs.shape[0], name='availability_type', dtype=str))

    # public transport

    transport_geometry = avaliability.get_transport(*coords, transport_time_cost)
    
    cur_houses.execute('\nUNION\n'.join(
        map(lambda function_and_name: f"SELECT id, address, name, ST_AsGeoJSON(ST_Centroid(geometry)), capacity as power, '{function_and_name[1]}' as service_type FROM {function_and_name[0]} WHERE ST_WITHIN(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))", # type: ignore
            function_service[function])), [transport_geometry] * len(function_service[function]))
    transport_data: List[List[Tuple[int, str, str, int, str]]] = list(filter(lambda id_and_others: id_and_others[0] not in walking_ids, cur_houses.fetchall()))
    transport_ids = set(map(lambda id_and_others: id_and_others[0], walking_data))
    
    transport_servs = pd.DataFrame(transport_data, columns=('service_id', 'address', 'service_name', 'point', 'power', 'service_type'))
    transport_servs['point'] = pd.Series(
        map(lambda geojson: (round(float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), 4), round(float(geojson[geojson.rfind(',') + 1:-2]), 4)),
            transport_servs['point'])
    )
    transport_servs = transport_servs.join(pd.Series(['public_transport'] * transport_servs.shape[0], name='availability_type', dtype=str))

    df_target_servs = df_target_servs.append(transport_servs, ignore_index=True)
    del transport_servs

    # perosonal_transport (car)

    car_geometry = avaliability.get_car(*coords, personal_transport_time_cost)
    
    cur_houses.execute('\nUNION\n'.join(
        map(lambda function_and_name: f"SELECT id, address, name, ST_AsGeoJSON(ST_Centroid(geometry)), capacity as power, '{function_and_name[1]}' as service_type FROM {function_and_name[0]} WHERE ST_WITHIN(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))", # type: ignore
            function_service[function])), [car_geometry] * len(function_service[function]))
    car_data: List[List[Tuple[int, str, str, int, str]]] = list(filter(lambda id_and_others: id_and_others[0] not in walking_ids and id_and_others[0] not in transport_ids, cur_houses.fetchall()))

    car_servs = pd.DataFrame(car_data, columns=('service_id', 'address', 'service_name', 'point', 'power', 'service_type'))
    car_servs['point'] = pd.Series(
        map(lambda geojson: (round(float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), 4), round(float(geojson[geojson.rfind(',') + 1:-2]), 4)),
            car_servs['point'])
    )
    car_servs = car_servs.join(pd.Series(['personal_transport'] * car_servs.shape[0], name='availability_type', dtype=str))
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

        # Найти I
        target_I = intensity

        # Найти значимость V
        target_V = significance

        # Рассчитать доступность D услуг
        df_target_servs['availability'] = np.where(df_target_servs['availability_type'] == 'walking', walking_availability,
                np.where(df_target_servs['availability_type'] == 'public_transport',
                        round(1 / target_I * public_transport_availability_multiplier, 2), round(1 / target_I * personal_transport_availability_multiplier)))

        # Вычислить мощность S предложения по целевому типу услуги для целевой группы
        target_S = (df_target_servs['power'] * df_target_servs['availability']).sum()

        # Если рассчитанная мощность S > max_target_s, то S принимается равной max_target_s
        if target_S > max_target_s:
            target_S = max_target_s

        # Вычислить значение обеспеченности О
        if target_V == 0.5:
            target_O = target_S / target_s_divider
        else:
            coeff = abs(target_V - 0.5) * coeff_multiplier
            if target_V > 0.5:
                target_O = ((target_S / target_s_divider) ** (coeff + 1)) / (5 ** coeff)
            else:
                target_O = 5 - ((5 - target_S / target_s_divider) ** (coeff + 1)) / (5 ** coeff)

    target_O = round(target_O, 2)

    if use_database:
        cur_provision.execute('SELECT id from atomic WHERE latitude = %s AND longitude = %s AND walking = %s AND transport = %s AND intensity = %s AND significance = %s',
                (*coords, walking_time_cost, transport_time_cost, intensity, significance))
        id = cur_provision.fetchall()
        if len(id) == 0:
            cur_provision.execute('INSERT INTO atomic (latitude, longitude, walking, transport, intensity, significance, provision_value) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                    (*coords, walking_time_cost, transport_time_cost, intensity, significance, target_O))
            conn_provision.commit()

    return {
        'walking_geometry': json.loads(walking_geometry),
        'transport_geometry': json.loads(transport_geometry) if transport_geometry is not None else None,
        'car_geometry': json.loads(car_geometry) if car_geometry is not None else None,
        'services': 
            dict([(service_type, list(df_target_servs[df_target_servs['service_type'] == service_type].dropna().drop(columns=['service_type']).transpose().to_dict().values())) for service_type in df_target_servs['service_type'].unique()])
        ,
        'provision_result': target_O,
        'parameters': {
            'walking_time_cost': walking_time_cost,
            'transport_time_cost': transport_time_cost,
            'personal_transport_time_cost': personal_transport_time_cost,
            'intensity': intensity,
            'significance': significance
        }
    }

def compute_atomic_provision_light(conn_houses: psycopg2.extensions.connection, conn_provision: psycopg2.extensions.connection,
        soc_group: str, situation: str, function: str, coords: Tuple[float, float]) -> Tuple[float, float, float]:
    cur_provision: psycopg2.excensions.cursor = conn_provision.cursor()
    if (soc_group, situation, function) not in needs_table:
        print(f'No data found for needs (soc_group = {soc_group}, situation = {situation}, function = {function}')
        raise Exception(f'No data found for needs (soc_group = {soc_group}, situation = {situation}, function = {function}')
    if (soc_group, function) not in values_table:
        print(f'No data found for values (soc_group = {soc_group}, function = {function})')
        raise Exception(f'No data found for values (soc_group = {soc_group}, function = {function})')

    walking_time_cost, transport_time_cost, _, intensity = needs_table[(soc_group, situation, function)]
    significance = values_table[(soc_group, function)]

    if walking_time_cost == 0 and transport_time_cost == 0:
        return 0, intensity, significance

    cur_provision.execute('SELECT provision_value from atomic WHERE latitude = %s AND longitude = %s AND walking = %s AND transport = %s AND intensity = %s AND significance = %s',
            (*coords, walking_time_cost, transport_time_cost, intensity, significance))
    provision_value = cur_provision.fetchall()
    if len(provision_value) != 0:
        return provision_value[0][0], intensity, significance

    return compute_atomic_provision(conn_houses, conn_provision, soc_group, situation, function, coords, use_database=True)['provision_result'], intensity, significance
    

def get_aggregation(conn_provision: psycopg2.extensions.connection, conn_houses: psycopg2.extensions.connection, where: Union[str, Tuple[float, float]],
        where_type: str, soc_group: Optional[str], situation: Optional[str], function: Optional[str], update: bool = False) -> Dict[str, Union[float, str]]:
    cur_houses: psycopg2.extensions.cursor = conn_houses.cursor()
    cur_provision: psycopg2.extensions.cursor = conn_provision.cursor()

    found_id: Optional[int] = None
    where_column = 'district' if where_type == 'districts' else 'municipality'

    if where_type in ('districts', 'municipalities'):
        cur_provision.execute(f'SELECT id, avg_intensity, avg_significance, avg_provision, time_done FROM aggregation_{where_column}'
                ' WHERE social_group_id = (SELECT id from social_groups where name = %s)'
                ' AND living_situation_id = (SELECT id from living_situations where name = %s)'
                ' AND city_function_id = (SELECT id from city_functions where name = %s)'
                f' AND {where_column}_id = (SELECT id from {where_type} where full_name = %s)',
                (soc_group, situation, function, where))
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
        del cur_data
    elif where_type == 'house':
        cur_provision.execute('SELECT id, avg_intensity, avg_significance, avg_provision, time_done FROM aggregation_house'
                ' WHERE social_group_id = (SELECT id from social_groups where name = %s)'
                ' AND living_situation_id = (SELECT id from living_situations where name = %s)'
                ' AND city_function_id = (SELECT id from city_functions where name = %s)'
                ' AND latitude = %s AND longitude = %s',
                (soc_group, situation, function, *where))
    elif where_type == 'total':
        raise Exception('This method is not available for now')
        
    if soc_group is None:
        soc_groups = social_groups
    else:
        soc_groups = [soc_group]
    
    if situation is None:
        situations = living_situations
    else:
        situations = [situation]

    if function is None:
        functions = city_functions
    else:
        functions = [function]

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
        for soc_group in soc_groups:
            cnt_functions = 0
            provision_function = 0.0
            intensity_function = 0.0
            significance_function = 0.0
            for function in functions:
                if values_table.get((soc_group, function), 0.0) == 0 or (len(soc_groups) != 1 and len(functions) != 1 and values_table.get((soc_group, function), 0.0) <= 0.5):
                    continue
                cnt_atomic = 0
                provision_atomic = 0.0
                intensity_atomic = 0.0
                significance_atomic = 0.0
                for situation in situations:
                    walking_time_cost, transport_time_cost, _, _ = needs_table.get((soc_group, situation, function), (0, 0, 0, 0))
                    if walking_time_cost == 0 and transport_time_cost == 0:
                        continue
                    try:
                        prov = compute_atomic_provision_light(conn_houses, conn_provision, soc_group, situation, function, house)
                        provision_atomic += prov[0]
                        intensity_atomic += prov[1]
                        significance_atomic += prov[2]
                        cnt_atomic += 1
                    except Exception as ex:
                        print(f'Exception occured: {ex}')
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
                groups_provision[soc_group] = (provision_function / cnt_functions, intensity_function / cnt_functions, intensity_function / cnt_functions)
                cnt_groups += 1
        cur_houses.execute('SELECT sum(ss.number) FROM social_structure ss'
                ' INNER JOIN social_groups sg on ss.social_group_id = sg.id'
                ' WHERE house_id in (SELECT id from houses WHERE ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float = %s'
                ' AND ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float = %s)', (house[0], house[1]))
        res = cur_houses.fetchall()
        if len(soc_groups) != 1:
            if len(res) != 0 and res[0][0] is not None:
                cnt_functions = res[0][0]
                provision_group = 0
                intensity_group = 0
                significance_group = 0
                for soc_group in groups_provision.keys():
                    cur_houses.execute('SELECT ss.number FROM social_structure ss'
                            ' INNER JOIN social_groups sg on ss.social_group_id = sg.id'
                            ' WHERE house_id in (SELECT id FROM houses WHERE ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float = %s'
                            ' AND ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float = %s) AND sg.name = %s union select 0', (house[0], house[1], soc_group))
                    number = cur_houses.fetchall()[0][0]
                    provision_group += groups_provision[soc_group][0] * number / cnt_functions
                    intensity_group += groups_provision[soc_group][1] * number / cnt_functions
                    significance_group += groups_provision[soc_group][2] * number / cnt_functions
            else:
                provision_group = sum(map(lambda x: x[0], groups_provision.values())) / cnt_groups
                intensity_group = sum(map(lambda x: x[1], groups_provision.values())) / cnt_groups
                significance_group = sum(map(lambda x: x[2], groups_provision.values())) / cnt_groups
        elif cnt_groups != 0:
            provision_group /= cnt_groups
            intensity_group /= cnt_groups
            significance_group /= cnt_groups
        provision_houses += provision_group
        intensity_houses += intensity_group
        significance_houses += intensity_group
        cnt_houses += 1
                
    if cnt_houses != 0:
        provision_houses /= cnt_houses
        intensity_houses /= cnt_houses
        significance_houses /= cnt_houses
    done_time: Any = time.localtime()
    done_time = f'{done_time.tm_year}-{done_time.tm_mon}-{done_time.tm_mday} {done_time.tm_hour}:{done_time.tm_min}:{done_time.tm_sec}'


    if where_type in ('districts', 'municipalities'):
        if found_id is None:
            cur_provision.execute(f'INSERT INTO aggregation_{where_column} (social_group_id, living_situation_id, city_function_id, {where_column}_id, avg_intensity, avg_significance, avg_provision, time_done)'
                    ' VALUES ((SELECT id from social_groups where name = %s), (SELECT id from living_situations where name = %s), (SELECT id from city_functions where name = %s),'
                    f' (SELECT id from {where_type} where full_name = %s), %s, %s, %s, %s)',
                    (soc_groups[0] if len(soc_groups) == 1 else None, situations[0] if len(situations) == 1 else None, functions[0] if len(functions) == 1 else None,
                            where, intensity_houses, significance_houses, provision_houses, done_time))
        else:
            cur_provision.provision.execute(f'UPDATE aggregation_{where_column} SET avg_intensity = %s, avg_significance = %s, avg_provision = %s, time_done = %s WHERE id = %s',
                    (intensity_houses, significance_houses, provision_houses, done_time, found_id))
    else:
        if found_id is None:
            cur_provision.execute('INSERT INTO aggregation_house (social_group_id, living_situation_id, city_function_id, latitude, longitude, avg_intensity, avg_significance, avg_provision, time_done)'
                    'VALUES ((SELECT id from social_groups where name = %s), (SELECT id from living_situations where name = %s), (SELECT id from city_functions where name = %s),'
                    ' %s, %s, %s, %s, %s, %s)',
                    (soc_group, situation, function, *where, intensity_houses, significance_houses, provision_houses, done_time))
        else:
            cur_provision.execute('UPDATE aggregation_house SET intensity = %s, avg_significance = %s, avg_provision = %s, time_done = %s WHERE id = %s', 
                    (intensity_houses, significance_houses, provision_houses, done_time, found_id))
    conn_provision.commit()
    return {
        'provision': provision_houses,
        'intensity': intensity_houses,
        'significance': significance_houses,
        'time_done': done_time
    }

def aggregate_district(conn_provision: psycopg2.extensions.connection, conn_houses: psycopg2.extensions.connection,
        district: str, soc_group: str, situation: str, function: str):
    print(f'Aggregating soc_group({soc_group}) + situation({situation}) + function({function}) + district({district}): ', end='') # type: ignore
    sys.stdout.flush()
    start = time.time()
    res = get_aggregation(conn_provision, conn_houses, district, 'districts', soc_group, situation, function, False)
    print(f'finished in {time.time() - start:6.2f} seconds (total_value = {res["provision"]:.2f})')

def aggregate_municipality(conn_provision: psycopg2.extensions.connection, conn_houses: psycopg2.extensions.connection,
        municipality: str, soc_group: str, situation: str, function: str):
    print(f'Aggregating soc_group({soc_group}) + situation({situation}) + function({function}) + municipality({municipality}): ', end='')
    sys.stdout.flush()
    start = time.time()
    res = get_aggregation(conn_provision, conn_houses, municipality, 'municipalities', soc_group, situation, function, False)
    print(f'finished in {time.time() - start:6.2f} seconds (total_value = {res["provision"]:.2f})')

def update_all_aggregations() -> None:
    full_start = time.time()
    with psycopg2.connect(properties.provision_conn_string()) as conn_provision, \
            psycopg2.connect(properties.houses_conn_string()) as conn_houses:
        try:
            for soc_group in social_groups + [None]: # type: ignore
                for function in city_functions + [None]: # type: ignore
                    intensity = values_table.get((soc_group, function), 0.0)
                    if intensity == 0 and soc_group is not None and function is not None:
                        continue
                    for situation in living_situations + [None]: # type: ignore
                        try:
                            walking_time_cost, transport_time_cost, _, _ = needs_table.get((soc_group, situation, function), (0, 0, 0, 0))
                            if walking_time_cost == 0 and transport_time_cost == 0 and \
                                    soc_group is not None and function is not None and \
                                    intensity is not None and situation is not None:
                                continue
                            for district in districts:
                                aggregate_district(conn_provision, conn_houses, district, soc_group, situation, function)
                            for municipality in municipalities:
                                aggregate_municipality(conn_provision, conn_houses, municipality, soc_group, situation, function)
                        except Exception as ex:
                            traceback.print_exc()
                            print(f'Exception occured! {ex}')
                            conn_provision.rollback()
                            time.sleep(2)
        finally:
            print(f'Finished updating all agregations in {time.time() - full_start:.2f} seconds')

def update_aggregation_district(district: str, including_municipalities: bool = False) -> None:
    full_start = time.time()
    with psycopg2.connect(properties.provision_conn_string()) as conn_provision, \
                psycopg2.connect(properties.houses_conn_string()) as conn_houses:
        try:
            for soc_group in social_groups + [None]: # type: ignore
                for function in city_functions + [None]: # type: ignore
                    intensity = values_table.get((soc_group, function), 0.0)
                    if intensity == 0 and soc_group is not None and function is not None:
                        continue
                    # p = multiprocessing.Pool(os.cpu_count() // 2) # type: ignore
                    for situation in living_situations + [None]: # type: ignore
                        walking_time_cost, transport_time_cost, _, _ = needs_table.get((soc_group, situation, function), (0, 0, 0, 0))
                        if walking_time_cost == 0 and transport_time_cost == 0 and \
                                soc_group is not None and function is not None and \
                                intensity is not None and situation is not None:
                            continue
                        try:
                            aggregate_district(conn_provision, conn_houses, district, soc_group, situation, function)
                        except Exception as ex:
                            traceback.print_exc()
                            print(f'Exception occured! {ex}')
                            time.sleep(2)
                        if including_municipalities:
                            for municipality in all_houses[all_houses['district'] == district]['municipality'].unique():
                                try:
                                    aggregate_municipality(conn_provision, conn_houses, municipality, soc_group, situation, function)
                                except Exception as ex:
                                    traceback.print_exc()
                                    print(f'Exception occured! {ex}')
                                    time.sleep(2)
                    # p.close()
                    # p.join()
        finally:
            print(f'Finished updating all agregations in {time.time() - full_start:.2f} seconds')

def update_global_data() -> None:
    global social_groups
    global living_situations
    global municipalities
    global districts
    global needs_table
    global values_table
    global all_houses
    with psycopg2.connect(properties.houses_conn_string()) as conn:
        cur: psycopg2.extensions.cursor = conn.cursor()

        cur.execute('SELECT name FROM social_groups')
        social_groups = list(map(lambda x: x[0], cur.fetchall()))
        
        cur.execute('SELECT name FROM living_situations')
        living_situations = list(map(lambda x: x[0], cur.fetchall()))

        cur.execute('SELECT full_name FROM municipalities')
        municipalities = list(map(lambda x: x[0], cur.fetchall()))

        cur.execute('SELECT full_name FROM districts')
        districts = list(map(lambda x: x[0], cur.fetchall()))

        needs_table = dict()
        cur.execute(database_needs_sql)
        for soc_group, situation, function, walking, transport, car, intensity in cur.fetchall():
            needs_table[(soc_group, situation, function)] = (walking, transport, car, intensity)

        values_table = dict()
        cur.execute(database_significance_sql)
        for soc_group, city_function, significance in cur.fetchall():
            values_table[(soc_group, city_function)] = significance

        cur.execute('SELECT DISTINCT dist.full_name, muni.full_name, ROUND(ST_X(ST_Centroid(h.geometry))::numeric, 3)::float as latitude, ROUND(ST_Y(ST_Centroid(h.geometry))::numeric, 3)::float as longitude FROM houses h inner join districts dist on dist.id = h.district_id inner join municipalities muni on muni.id = h.municipal_id')
        all_houses = pd.DataFrame(cur.fetchall(), columns=('district', 'municipality', 'latitude', 'longitude'))

compress = Compress()

app = Flask(__name__)
compress.init_app(app)

@app.after_request
def after_request(response) -> Response:
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route('/api/reload_data', methods=['POST'])
def reload_data() -> Response:
    update_global_data()
    return make_response('OK')

# Расчет обеспеченности для атомарной ситуации: обеспеченность одной социальной группы в одной жизненной ситуации
# одной городской функцией, относительно одного жилого дома.

# Для сервисов передаются следующие атрибуты:
# -  идентификатор (service_id)
# -  название сервиса(service_name)
# -  признак принадлежности изохрону пешеходной доступности (walking_dist, boolean)
# -  признак принадлежности изохронам транспортной доступности (transport_dist, boolean)
# -  мощность сервиса (power, со значениями от 1 до 10)

# Сервис возвращает числовую оценку обеспеченности целевой социальной группы в целевой жизненной ситуации сервисами,
# относящимися к целевой городской функции, в целевой точке (доме)
@app.route('/api/provision/atomic', methods=['GET'])
def atomic_provision() -> Response:
    if not ('soc_group' in request.args and 'situation' in request.args and 'function' in request.args and 'point' in request.args):
        return make_response(jsonify({'error': 'Request must include all of the ("soc_group", "situation", "function", "point") arguments'}))
    soc_group: str = request.args['soc_group'] # type: ignore
    situation: str = request.args['situation'] # type: ignore
    function: str = request.args['function'] # type: ignore
    if not (soc_group in social_groups and situation in living_situations and function in city_functions):
        return make_response(jsonify({'error': 'At least one of the ("soc_group", "situation", "function") is not in the list of avaliable'}))
    coords: Tuple[int, int] = tuple(map(float, request.args['point'].split(','))) # type: ignore

    with psycopg2.connect(properties.houses_conn_string()) as conn_houses, psycopg2.connect(properties.provision_conn_string()) as conn_provision:
        try:
            return make_response(jsonify({
                '_links': {'self': {'href': request.path}},
                '_embedded': compute_atomic_provision(conn_houses, conn_provision, coords=coords, **request.args)
            }))
        except Exception as ex:
            traceback.print_exc()
            return make_response(jsonify({'error': str(ex)}))

@app.route('/api/provision/aggregated', methods=['GET'])
def aggregated_provision() -> Response:
    soc_group: Optional[str] = request.args.get('soc_group', None, type=str)
    situation: Optional[str] = request.args.get('situation', None, type=str)
    function: Optional[str] = request.args.get('function', None, type=str)
    district: Optional[str] = request.args.get('region', None, type=str)
    municipality: Optional[str] = request.args.get('municipality', None, type=str)
    house: Optional[Tuple[float, float]]
    if 'house' in request.args:
        house = tuple(map(float, request.args.get('house', type=str).split(','))) # type: ignore
    else:
        house = None
    if not ((soc_group is None or soc_group in social_groups)
            and (situation is None or situation in living_situations)
            and (function is None or function in city_functions)):
        return make_response(jsonify({'error': "At least one of the ('soc_group', 'situation', 'function') is not in the list of avaliable"}))
    
    where: Union[str, Tuple[float, float]]
    where_type: str
    if district is None and municipality is None and house is None:
        where = 'total'
        where_type = 'total'
    elif house is not None:
        where = house
        where_type = 'house'
    elif district is not None:
        where = district
        where_type = 'districts'
    else:
        where = municipality # type: ignore
        where_type = 'municipalities'

    with psycopg2.connect(properties.provision_conn_string()) as conn_provision, \
            psycopg2.connect(properties.houses_conn_string()) as conn_houses:
        try:
            return make_response(jsonify({
                '_links': {'self': {'href': request.path}},
                '_embedded': {
                    'params': {
                        'soc_group': soc_group,
                        'situation': situation,
                        'function': function,
                        'region': district
                    },
                    'result': get_aggregation(conn_provision, conn_houses, where, where_type, soc_group, situation, function)
                }
            }))
        except Exception as ex:
            traceback.print_exc()
            return make_response(jsonify({'error': str(ex)}))

@app.route('/api/provision/ready/houses', methods=['GET'])
def ready_houses() -> Response:
    soc_group: Optional[str] = request.args.get('soc_group', None, type=str)
    situation: Optional[str] = request.args.get('situation', None, type=str)
    function: Optional[str] = request.args.get('function', None, type=str)
    house: Tuple[Optional[float], Optional[float]]
    if 'house' in request.args:
        house = tuple(map(float, request.args['house'].split(','))) # type: ignore
    else:
        house = (None, None)
    with psycopg2.connect(properties.provision_conn_string()) as conn:
        cur = conn.cursor()
        cur_str = 'SELECT soc.name, liv.name, fun.name, a.latitude, a.longitude, avg_provision' \
            ' FROM aggregation_house a JOIN living_situations liv ON liv.id = a.living_situation_id' \
            ' JOIN social_groups soc ON soc.id = a.social_group_id' \
            ' JOIN city_functions fun ON fun.id = a.city_function_id'
        wheres = []
        for column_name, value in (('soc.name', soc_group), ('liv.name', situation), ('fun.name', function), ('latitude', house[0]), ('longitude', house[1])):
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
                'soc_group': soc_group,
                'situation': situation,
                'function': function,
                'house': f'{house[0]},{house[1]}' if house[0] is not None else None
            },
            'result': list((row[1].to_dict() for row in ans.iterrows()))
        }
    }))

@app.route('/api/provision/ready/regions', methods=['GET'])
def ready_districts() -> Response:
    soc_group: Optional[str] = request.args.get('soc_group', None, type=str)
    situation: Optional[str] = request.args.get('situation', None, type=str)
    function: Optional[str] = request.args.get('function', None, type=str)
    district: Optional[str] = request.args.get('region', None, type=str)
    with psycopg2.connect(properties.provision_conn_string()) as conn:
        cur = conn.cursor()
        cur_str = 'SELECT soc.name, liv.name, fun.name, dist.full_name, avg_provision' \
            ' FROM aggregation_district a JOIN living_situations liv ON liv.id = a.living_situation_id' \
            ' JOIN social_groups soc ON soc.id = a.social_group_id' \
            ' JOIN city_functions fun ON fun.id = a.city_function_id' \
            ' JOIN districts dist ON dist.id = a.district_id'
        wheres = []
        for column_name, value in (('soc.name', soc_group), ('liv.name', situation), ('fun.name', function), ('dist.full_name', district)):
            if value is not None:
                wheres.append(f"{column_name} = '{value}'")
        if len(wheres) != 0:
            cur_str += ' WHERE ' + ' AND '.join(wheres)
        cur.execute(cur_str)
        ans = pd.DataFrame(cur.fetchall(), columns=('social_group', 'living_situation', 'city_function', 'region', 'provision'))
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'params': {
                'soc_group': soc_group,
                'situation': situation,
                'function': function,
                'region': district
            },
            'result': list((row[1].to_dict() for row in ans.iterrows()))
        }
    }))

@app.route('/api/provision/ready/municipalities', methods=['GET'])
def ready_municipalities() -> Response:
    soc_group: Optional[str] = request.args.get('soc_group', None, type=str)
    situation: Optional[str] = request.args.get('situation', None, type=str)
    function: Optional[str] = request.args.get('function', None, type=str)
    municipality: Optional[str] = request.args.get('municipality', None, type=str)
    with psycopg2.connect(properties.provision_conn_string()) as conn:
        cur = conn.cursor()
        cur_str = 'SELECT soc.name, liv.name, fun.name, muni.full_name, avg_provision' \
            ' FROM aggregation_municipality a JOIN living_situations liv ON liv.id = a.living_situation_id' \
            ' JOIN social_groups soc ON soc.id = a.social_group_id' \
            ' JOIN city_functions fun ON fun.id = a.city_function_id' \
            ' JOIN municipalities muni ON muni.id = a.municipality_id'
        wheres = []
        for column_name, value in (('soc.name', soc_group), ('liv.name', situation), ('fun.name', function), ('muni.full_name', municipality)):
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
                'soc_group': soc_group,
                'situation': situation,
                'function': function,
                'municipality': municipality
            },
            'result': list((row[1].to_dict() for row in ans.iterrows()))
        }
    }))
    
@app.route('/api/houses')
def houses_in_square() -> Response:
    if 'firstPoint' not in request.args or 'secondPoint' not in request.args:
        return make_response(jsonify({'error': "'firstPoint' and 'secondPoint' must be provided as query parameters"}))
    point_1: Tuple[int, int] = tuple(map(float, request.args['firstPoint'].split(','))) # type: ignore
    point_2: Tuple[int, int] = tuple(map(float, request.args['secondPoint'].split(','))) # type: ignore
    with psycopg2.connect(properties.houses_conn_string()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float, ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float FROM houses"
                " WHERE ST_WITHIN(geometry, ST_POLYGON(text('LINESTRING({lat1} {lan1}, {lat1} {lan2}, {lat2} {lan2}, {lat2} {lan1}, {lat1} {lan1})'), 4326))".format(
            lat1=point_1[0], lan1 = point_1[1], lat2 = point_2[0], lan2 = point_2[1]
        ))
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'houses': list(cur.fetchall())
            }
        }
        ))


@app.route('/api/list/social_groups', methods=['GET'])
def list_social_groups() -> Response:
    if 'function' in request.args:
        with psycopg2.connect(properties.houses_conn_string()) as conn:
            cur = conn.cursor()
            cur.execute('SELECT f.name, s.name, significance FROM values v'
                ' JOIN social_groups s ON v.social_group_id = s.id'
                ' JOIN city_functions f ON v.city_function_id = f.id'
                ' WHERE s.name = %s AND significance > 0',
                (request.args['function'],))
            res = list(map(lambda y: y[1], filter(lambda x: len(function_service[x[0]]) != 0, cur.fetchall())))
    else:
        res = city_functions
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'social_groups': res
        }
    }))

@app.route('/api/list/city_functions', methods=['GET'])
def list_city_functions() -> Response:
    if 'soc_group' in request.args:
        with psycopg2.connect(properties.houses_conn_string()) as conn:
            cur = conn.cursor()
            cur.execute('SELECT f.name, significance FROM values v'
                ' JOIN social_groups s ON v.social_group_id = s.id'
                ' JOIN city_functions f ON v.city_function_id = f.id'
                ' WHERE s.name = %s AND significance > 0',
                (request.args['soc_group'],))
            res = list(filter(lambda x: len(function_service[x]) != 0, map(lambda y: y[0], cur.fetchall())))
    else:
        res = city_functions
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'city_functions': res
        }
    }))

@app.route('/api/list/living_situations', methods=['GET'])
def list_living_situations() -> Response:
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'living_situations': living_situations
        }
    }))

@app.route('/api/list/regions', methods=['GET'])
def list_districts() -> Response:
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'regions': districts
        }
    }))

@app.route('/api/list/municipalities', methods=['GET'])
def list_municipalities() -> Response:
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'municipalities': municipalities
        }
    }))

@app.route('/api', methods=['GET'])
def api_help() -> Response:
    return make_response(jsonify({
        'version': '2020-10-20',
        '_links': {
            'self': {
                'href': request.path
            },
            'atomic_provision': {
                'href': '/api/provision/atomic{?soc_group,situation,function,point}',
                'templated': True
            },
            'aggregated-provision': {
                'href': '/api/provision/aggregated{?soc_group,situation,function,region,municipality,house}',
                'templated': True
            },
            'get-houses': {
                'href': '/api/houses{?firstPoint,secondPoint}',
                'templated': True
            },
            'list-social_groups': {
                'href': '/api/list/social_groups'
            },
            'list-living_situations': {
                'href': '/api/list/living_situations'
            },
            'list-city_functions': {
                'href': '/api/list/city_functions{?soc_group}',
                'templated': True
            },
            'list-regions': {
                'href': '/api/list/regions'
            },
            'list-municipalities': {
                'href': '/api/list/municipalities'
            },
            'ready_aggregations_houses': {
                'href': '/api/provision/ready/houses{?soc_group,situation,function,house}',
                'templated': True
            },
            'ready_aggregations_regions': {
                'href': '/api/provision/ready/regions{?soc_group,situation,function,region}',
                'templated': True
            },
            'ready_aggregations_municipalities': {
                'href': '/api/provision/ready/municipalities{?soc_group,situation,function,municipality}',
                'templated': True
            }
        }
    }))

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':

    # Default properties settings

    properties = Properties(
            'localhost', 5432, 'provision', 'postgres', 'postgres', 
            'localhost', 5432, 'citydb', 'postgres', 'postgres',
            8080, 'http://10.32.1.61:8080/api.v2/isochrones'
    )
    skip_aggregation = False

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
    if 'PROVISION_SKIP_AGGREGATION' in os.environ:
        skip_aggregation = True
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
    parser.add_argument('-hp', '--port', action='store', dest='api_port',
                        help=f'postgres port number [default: {properties.api_port}]', type=int)
    parser.add_argument('-S', '--skip_aggregation', action='store_true', dest='skip_aggregation',
                        help=f'skip the process of calculation of aggregations')
    parser.add_argument('-T', '--transport_model_api', action='store', dest='transport_model_api_endpoint',
                        help=f'url of transport model api [default: {properties.transport_model_api_endpoint}]', type=str)
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
    if args.skip_aggregation:
        skip_aggregation = True
    
    update_global_data()

    avaliability = Avaliability()

    if skip_aggregation:
        print('Skipping aggregation')
    else:
        print('Starting aggregation')
        time.sleep(2)
        update_aggregation_district('Василеостровский район', True)

    print(f'Starting application on 0.0.0.0:{properties.api_port} with houses DB ({properties.houses_db_user}@{properties.houses_db_addr}:{properties.houses_db_port}/{properties.houses_db_name}) and'
        f' provision DB ({properties.provision_db_user}@{properties.provision_db_addr}:{properties.provision_db_port}/{properties.provision_db_name}).')
    print(f'Transport model API endpoint is {properties.transport_model_api_endpoint}')

    app.run(host='0.0.0.0', port=properties.api_port)
