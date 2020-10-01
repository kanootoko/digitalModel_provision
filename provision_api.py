from collect_geometry import get_walking
from flask import Flask, jsonify, make_response, request, Response
from flask_compress import Compress
import psycopg2
import pandas as pd, numpy as np
import argparse
import json
import requests
import time
import threading
from typing import Any, Tuple, List, Dict, Optional, Union
from os import environ

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
        print(properties.provision_conn_string())
        self.conn = psycopg2.connect(properties.provision_conn_string())

    def get_walking(self, lat: float, lan: float, t: int) -> str:
        if t == 0:
            return json.dumps({
                    'type': 'Polygon',
                    'coordinates': []
            })
        cur = self.conn.cursor()
        cur.execute(f'select ST_AsGeoJSON(geometry) from walking where latitude = {lat} and longitude = {lan} and time = {t}')
        res = cur.fetchall()
        if len(res) != 0:
            return res[0][0]
        try:
            print(f'downloading walking for {lat}, {lan}, {t}')
            res = json.dumps(
                requests.get(f'https://galton.urbica.co/api/foot/?lng={lat}&lat={lan}&radius=5&cellSize=0.1&intervals={t}', timeout=15).json()['features'][0]['geometry']
            )
        except Exception:
            return json.dumps({'type': 'Polygon', 'coordinates': []})
        cur.execute(f"insert into walking (latitude, longitude, time, geometry) values ({lat}, {lan}, {t}, '{res}')")
        return res


    def get_transport(self, lat: float, lan: float, t: int) -> str:
        if t == 0:
            return json.dumps({
                'type': 'Polygon',
                'coordinates': []
            })
        cur = self.conn.cursor()
        cur.execute(f'select ST_AsGeoJSON(geometry) from transport where latitude = {lat} and longitude = {lan} and time = {t}')
        res = cur.fetchall()
        if len(res) != 0:
            return res[0][0]
        if t >= 60:
            cur.execute(f'select ST_AsGeoJSON(geometry) from transport where time = {t} limit 1')
            res = cur.fetchall()
            if len(res) != 0:
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
        except Exception:
            return json.dumps({'type': 'Polygon', 'coordinates': []})
        if len(data['features']) == 0:
            ans = json.dumps({'type': 'Polygon', 'coordinates': []})
            cur.execute(f"INSERT INTO transport (latitude, longitude, time, geometry) VALUES ({lat}, {lan}, {t}, ST_SetSRID(ST_GeomFromGeoJSON('{ans}'::text), 4326))")
            return ans
        cur.execute('INSERT INTO transport (latitude, longitude, time, geometry) VALUES (%s, %s, %s, (SELECT ST_UNION(ARRAY[' + \
                ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')', data['features'])) + '])))', (lat, lan, t))
        return res
    
    def ensure_ready(self, lat: float, lan: float, time_walking: int, time_transport: int) -> None:
        walking = threading.Thread(target=lambda: self.get_walking(lat, lan, time_walking))
        transport = threading.Thread(target=lambda: self.get_transport(lat, lan, time_transport))

        walking.start()
        transport.start()

        walking.join()
        transport.join()
    
properties: Properties
avaliability: Avaliability

database_needs_sql = '''select gr.name as social_group_name, gr.code as social_group_code, sit.name as living_situation, fun.name as city_function, 
        walking, public_transport, personal_transport, intensity from needs need
	inner join living_situations sit on need.living_situation_id = sit.id
	inner join social_groups gr on need.social_group_id = gr.id
	inner join city_functions fun on need.city_function_id = fun.id
	where gr.name = %s and sit.name = %s and fun.name = %s'''

database_significance_sql = '''select significance from values val
	inner join social_groups gr on val.social_group_id = gr.id
	inner join city_functions fun on val.city_function_id = fun.id
	where gr.name = %s and fun.name = %s'''

social_groups: List[str]
# city_functions: List[str]
living_situations: List[str]
municipalities: List[str]
regions: List[str]

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
    'Культура': (('zoos', 'зоопарк'), ('theatres', 'театр'), ('museums', 'музей'), ('libraries', 'библиотека')),
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

def compute_atomic_provision(conn: psycopg2.extensions.connection, soc_group: str, situation: str, function: str,
        coords: Tuple[float, float]) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(database_needs_sql, (soc_group, situation, function))
        _, _, _, _, walking_time_cost, transport_time_cost, personal_transport_cost, intensity = cur.fetchall()[0]

        cur.execute(database_significance_sql, (soc_group, function))
        significance = cur.fetchall()[0][0]

        if walking_time_cost == 0 and transport_time_cost == 0:
            return {
                'walking_geometry': None,
                'transport_geometry': None,
                'services': list(),
                'provision_result': 0.0,
                'parameters': {
                    'walking_time_cost': walking_time_cost,
                    'transport_time_cost': transport_time_cost,
                    'personal_transport_time_cost': personal_transport_cost,
                    'intensity': intensity,
                    'significance': significance
                }
            }

        avaliability.ensure_ready(*coords, walking_time_cost, transport_time_cost)

        # Walking

        walking_geometry = avaliability.get_walking(*coords, walking_time_cost)

        try:
            cur.execute('\nUNION\n'.join(
                map(lambda function_and_name: f"SELECT id, name, ST_AsGeoJSON(ST_Centroid(geometry)), capacity as power, '{function_and_name[1]}' as service_type FROM {function_and_name[0]} WHERE ST_WITHIN(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))", # type: ignore
                    function_service[function])), [walking_geometry] * len(function_service[function]))
        except Exception as ex:
            if str(ex) == "can't execute an empty query":
                return {'error': 'For now there is no data avaliable for this service'}
            raise
        walking_ids_names: List[List[Tuple[int, str]]] = cur.fetchall()
        walking_ids = set(map(lambda id_name: id_name[0], walking_ids_names))

        # # Сохранить сервисы в датафрейм

        df_target_servs = pd.DataFrame(walking_ids_names, columns=('service_id', 'service_name', 'point', 'power', 'service_type'))
        df_target_servs['point'] = pd.Series(
            map(lambda geojson: (round(float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), 4), round(float(geojson[geojson.rfind(',') + 1:-2]), 4)),
                df_target_servs['point'])
        )
        df_target_servs = df_target_servs.join(pd.Series([1] * df_target_servs.shape[0], name='walking_dist'))
        df_target_servs = df_target_servs.join(pd.Series([0] * df_target_servs.shape[0], name='transport_dist'))
        # df_target_servs = df_target_servs.join(pd.Series([5] * df_target_servs.shape[0], name='power'))

        # Transport

        transport_geometry = avaliability.get_transport(*coords, transport_time_cost)
        
        cur.execute('\nUNION\n'.join(
            map(lambda function_and_name: f"SELECT id, name, ST_AsGeoJSON(ST_Centroid(geometry)), capacity as power, '{function_and_name[1]}' as service_type FROM {function_and_name[0]} WHERE ST_WITHIN(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))", # type: ignore
                function_service[function])), [transport_geometry] * len(function_service[function]))
        transport_ids_names = list(filter(lambda id_name: id_name[0] not in walking_ids, cur.fetchall()))
        
        transport_servs = pd.DataFrame(transport_ids_names, columns=('service_id', 'service_name', 'point', 'power', 'service_type'))
        transport_servs['point'] = pd.Series(
            map(lambda geojson: (round(float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), 4), round(float(geojson[geojson.rfind(',') + 1:-2]), 4)),
                transport_servs['point'])
        )
        transport_servs = transport_servs.join(pd.Series([0] * transport_servs.shape[0], name='walking_dist'))
        transport_servs = transport_servs.join(pd.Series([1] * transport_servs.shape[0], name='transport_dist'))
        # transport_servs = transport_servs.join(pd.Series([5] * transport_servs.shape[0], name='power'))

        df_target_servs = df_target_servs.append(transport_servs, ignore_index=True)
        del transport_servs

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
        df_target_servs['availability'] = np.where(df_target_servs['walking_dist'], 1, 1 / target_I)

        # Вычислить мощность S предложения по целевому типу услуги для целевой группы
        target_S = (df_target_servs['power'] * df_target_servs['availability']).sum()

        # Если рассчитанная мощность S > 30, то S принимается равной 30
        if target_S > 30:
            target_S = 30.0

        # Вычислить значение обеспеченности О
        if target_V == 0.5:
            target_O = target_S / 6
        elif target_V > 0.5:
            # Вычислить вспомогательный параметр a
            a = (target_V - 0.5) * 5
            target_O = ((target_S / 6) ** (a + 1)) / (5 ** a)
        else:
            # Вычислить вспомогательный параметр b
            b = (0.5 - target_V) * 5
            target_O = 5 - ((5 - target_S / 6) ** (b + 1)) / 5 ** b

    target_O = round(target_O, 2)
    print(f'Обеспеченность социальной группы ({soc_group}) функцией ({function}) в ситуации ({situation}) в точке {coords}: {target_O}')

    return {
        'walking_geometry': json.loads(walking_geometry),
        'transport_geometry': json.loads(transport_geometry) if transport_geometry is not None else None,
        'services': 
            dict([(service_type, list(df_target_servs[df_target_servs['service_type'] == service_type].dropna().drop(columns=['service_type', 'walking_dist']).transpose().to_dict().values())) for service_type in df_target_servs['service_type'].unique()])
        ,
        'provision_result': target_O,
        'parameters': {
            'walking_time_cost': walking_time_cost,
            'transport_time_cost': transport_time_cost,
            'personal_transport_time_cost': personal_transport_cost,
            'intensity': intensity,
            'significance': significance
        }
    }

def get_aggregation(conn_provision: psycopg2.extensions.connection, conn_houses: psycopg2.extensions.connection, where: Union[List[str], Tuple[float, float]], where_type: str, soc_groups: Union[str, List[str]],
        situations: Union[str, List[str]], functions: Union[str, List[str]], update: bool = False) -> dict:
    with conn_provision.cursor() as cur_provision, conn_houses.cursor() as cur_houses:
        if isinstance(soc_groups, str):
            soc_groups = [soc_groups]
        if isinstance(situations, str):
            situations = [situations]
        if isinstance(functions, str):
            functions = [functions]
        if isinstance(where, str):
            where = [where]

        found_id: Optional[int] = None

        soc_groups = sorted(soc_groups)
        situations = sorted(situations)
        functions = sorted(functions)
        if isinstance(where, list):
            where = sorted(where)

        cur_provision.execute('SELECT id, avg_intensity, avg_significance, total_value, constituents, time_done from provision_aggregation')
        for id, intensity, significance, provision, consituents, done in cur_provision.fetchall():
            if (sorted(consituents['districts' if where_type == 'regions' else 'municipalities']) == where or where_type == 'house' and consituents['house'] == where) \
                    and sorted(consituents['soc_groups']) == soc_groups and \
                    sorted(consituents['situations']) == situations and sorted(consituents['functions']) == functions:
                if not update:
                    return {
                        'provision': provision,
                        'intensity': intensity,
                        'significance': significance,
                        'time_done': done
                    }
                else:
                    found_id = id
                    break
        houses: List[Tuple[float, float]]
        if where_type != 'house':
            where_str = '(' + ', '.join(map(lambda x: f"'{x}'", where)) + ')'
            cur_houses.execute(f'SELECT DISTINCT ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float, ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float FROM houses WHERE'
                    f' {"district_id" if where_type == "regions" else "municipal_id"} in (SELECT id FROM {"districts" if where_type == "regions" else "municipalities"} WHERE full_name in {where_str})')
            houses = list(map(lambda x: (x[0], x[1]), cur_houses.fetchall()))
        else:
            houses = [houses] # type: ignore

        # FIXME there needs to be code optimization

        cnt_main = 0
        provision_main = 0.0
        intensity_main = 0.0
        significance_main = 0.0

        for house in houses:
            cnt = 0
            provision = 0.0
            intensity = 0.0
            significance = 0.0
            if len(soc_groups) == 1:
                if len(functions) == 1:
                    for situation in situations:
                        prov = compute_atomic_provision(conn_houses, soc_groups[0], situation, functions[0], house)
                        provision += prov['provision_result']
                        intensity += prov['parameters']['intensity'] # type: ignore
                        significance += prov['parameters']['significance'] # type: ignore
                        cnt += 1
                else:
                    for function in functions:
                        cur_houses.execute(database_significance_sql, (soc_groups[0], function))
                        if cur_houses.fetchall()[0][0] <= 0.5:
                            continue
                        cnt_1 = 0
                        provision_1 = 0.0
                        intensity_1 = 0.0
                        significance_1 = 0.0
                        for situation in situations:
                            prov = compute_atomic_provision(conn_houses, soc_groups[0], situation, function, house)
                            provision_1 += prov['provision_result']
                            intensity_1 += prov['parameters']['intensity'] # type: ignore
                            significance_1 += prov['parameters']['significance'] # type: ignore
                            cnt_1 += 1
                        if cnt_1 != 0:
                            provision += provision_1 / cnt_1
                            intensity += intensity_1 / cnt_1
                            significance += significance_1 / cnt_1
                            cnt += 1
            else:
                if len(functions) == 1:
                    groups_provision = dict()
                    for soc_group in soc_groups:
                        cnt_1 = 0
                        provision_1 = 0.0
                        intensity_1 = 0.0
                        significance_1 = 0.0
                        for situation in situations:
                            prov = compute_atomic_provision(conn_houses, soc_group, situation, functions[0], house)
                            provision_1 += prov['provision_result']
                            intensity_1 += prov['parameters']['intensity'] # type: ignore
                            significance_1 += prov['parameters']['significance'] # type: ignore
                            cnt_1 += 1
                        groups_provision[soc_group] = (provision_1 / cnt_1, intensity_1 / cnt_1, significance_1 / cnt_1)
                    cur_houses.execute('select sum(ss.number) from social_structure ss'
                                    ' inner join social_groups sg on ss.social_group_id = sg.id'
                                    ' where house_id in (select id from houses where ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float = %s'
                                    ' and ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float = %s)', (house[1], house[1]))
                    res = cur_houses.fetchll()[0]
                    if len(res) != 0:
                        cnt_1 = res[0]
                        for soc_group in soc_groups:
                            cur_houses.execute('select sum(ss.number) from social_structure ss'
                                    ' inner join social_groups sg on ss.social_group_id = sg.id'
                                    ' where house_id in (select id from houses where ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float = %s'
                                    ' and ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float = %s) where sg.name = %s union select 0', (house[1], house[1], soc_group))
                            number = cur_houses.fetchall()[0][0]
                            provision += groups_provision[soc_group][0] * number / cnt_1
                            intensity += groups_provision[soc_group][1] * number / cnt_1
                            significance += groups_provision[soc_group][2] * number / cnt_1
                        cnt = 1
                    else:
                        cnt = len(soc_groups)
                        provision = sum(map(lambda x: x[0], groups_provision.values()))
                        intensity = sum(map(lambda x: x[1], groups_provision.values()))
                        significance = sum(map(lambda x: x[2], groups_provision.values()))
                else:
                    groups_provision = dict()
                    for soc_group in soc_groups:
                        cnt_1 = 0
                        provision_1 = 0.0
                        intensity_1 = 0.0
                        significance_1 = 0.0
                        for function in functions:
                            cur_houses.execute(database_significance_sql, (soc_groups[0], function))
                            if cur_houses.fetchall()[0][0] <= 0.5:
                                continue
                            cnt_2 = 0
                            provision_2 = 0.0
                            intensity_2 = 0.0
                            significance_2 = 0.0
                            for situation in situations:
                                prov = compute_atomic_provision(conn_houses, soc_groups[0], situation, function, house)
                                provision_2 += prov['provision_result']
                                intensity_2 += prov['parameters']['intensity'] # type: ignore
                                significance_2 += prov['parameters']['significance'] # type: ignore
                                cnt_2 += 1
                            if cnt_2 != 0:
                                provision += provision_2 / cnt_2
                                intensity += intensity_2 / cnt_2
                                significance += intensity_2 / cnt_2
                                cnt_1 += 1
                        if cnt_1 != 0:
                            provision += provision_1 / cnt_1
                            intensity += intensity_1 / cnt_1
                            significance += intensity_1 / cnt_1
                        groups_provision[soc_group] = (provision_1 / cnt_1, intensity_1 / cnt_1, significance_1 / cnt_1)
                    cur_houses.execute('select sum(ss.number) from social_structure ss'
                                    ' inner join social_groups sg on ss.social_group_id = sg.id'
                                    ' where house_id in (select id from houses where ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float = %s'
                                    ' and ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float = %s)', (house[1], house[1]))
                    res = cur_houses.fetchall()[0]
                    if len(res) != 0:
                        cnt_1 = res[0]
                        for soc_group in soc_groups:
                            cur_houses.execute('select sum(ss.number) from social_structure ss'
                                    ' inner join social_groups sg on ss.social_group_id = sg.id'
                                    ' where house_id in (select id from houses where ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float = %s'
                                    ' and ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float = %s) where sg.name = %s union select 0', (house[1], house[1], soc_group))
                            number = cur.fetchall()[0][0]
                            provision += groups_provision[soc_group][0] * number / cnt_1
                            intensity += groups_provision[soc_group][1] * number / cnt_1
                            significance += groups_provision[soc_group][2] * number / cnt_1
                        cnt = 1
                    else:
                        cnt = len(soc_groups)
                        provision = sum(map(lambda x: x[0], groups_provision.values()))
                        intensity = sum(map(lambda x: x[1], groups_provision.values()))
                        significance = sum(map(lambda x: x[2], groups_provision.values()))
            if cnt != 0:
                provision_main += provision / cnt
                intensity_main += intensity / cnt
                significance_main += intensity / cnt
                cnt_main += 1
                    
        if cnt_main != 0:
            provision_main /= cnt_main
            intensity_main /= cnt_main
            significance_main /= cnt_main
        done_time: Any = time.localtime()
        done_time = f'{done_time.tm_year}-{done_time.tm_mon}-{done_time.tm_mday} {done_time.tm_hour}:{done_time.tm_min}:{done_time.tm_sec}'

        if found_id is None:
            cur_provision.execute('INSERT INTO provision_aggregation (avg_intensity, avg_significance, total_value, constituents, time_done) VALUES (%s, %s, %s, %s, %s)',
                    (intensity_main, significance_main, provision_main, json.dumps({
                            'soc_groups': soc_groups,
                            'functions': functions,
                            'situations': situations,
                            'districts': where if where_type == 'regions' else [],
                            'municipalities': where if where_type == 'municipalities' else [],
                            'house': where if where_type == 'house' else []
                    }), done_time))
        else:
            cur_provision.execute('UPDATE provision_aggregation SET avg_intensity = %s, avg_significance = %s, total_value = %s, time_done = %s WHERE id = %s', 
                    (intensity_main, significance_main, provision_main, done_time, found_id))
        conn_provision.commit()
        return {
            'provision': provision_main,
            'intensity': intensity_main,
            'significance': significance_main,
            'time_done': done_time
        }

def update_all_aggregations():
    import sys
    full_start = time.time()
    try:
        with psycopg2.connect(properties.provision_conn_string()) as conn_provision, \
                psycopg2.connect(properties.houses_conn_string()) as conn_houses:
            for soc_group in social_groups + [social_groups]:
                for situation in living_situations + [living_situations]:
                    for function in city_functions + [city_functions]:
                        for region in regions + [regions]:
                            print(f'Aggregating soc_group({soc_group}) + situation({situation}) + function({function}) + region({region}): ', end='')
                            sys.stdout.flush()
                            start = time.time()
                            res = get_aggregation(conn_provision, conn_houses, region, 'regions', soc_group, situation, function, False)
                            print(f'finished in {time.time() - start:6.2f} seconds (total_value = {res["provision"]})')
                        for municipality in municipalities + [municipalities]:
                            print(f'Aggregating soc_group({soc_group}) + situation({situation}) + function({function}) + municipality({municipality}): ', end='')
                            sys.stdout.flush()
                            start = time.time()
                            res = get_aggregation(conn_provision, conn_houses, municipality, 'municipalities', soc_group, situation, function, False)
                            print(f'finished in {time.time() - start:6.2f} seconds (total_value = {res["provision"]})')
    finally:
        print(f'Finished updating all agregations in {time.time() - full_start:.2} seconds')

compress = Compress()

app = Flask(__name__)
compress.init_app(app)

@app.after_request
def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

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
    soc_group: str = request.args.get('soc_group') # type: ignore
    situation: str = request.args.get('situation') # type: ignore
    function: str = request.args.get('function') # type: ignore
    if not (soc_group in social_groups and situation in living_situations and function in city_functions):
        return make_response(jsonify({'error': 'At least one of the ("soc_group", "situation", "function") is not in the list of avaliable'}))
    coords: Tuple[int, int] = tuple(map(float, request.args.get('point').split(','))) # type: ignore

    with psycopg2.connect(properties.houses_conn_string()) as conn:
        try:
            return make_response(jsonify({'_embedded': compute_atomic_provision(conn, soc_group, situation, function, coords)}))
        except Exception as ex:
            return make_response(jsonify({'error': str(ex)}))

@app.route('/api/provision/aggregated', methods=['GET'])
def aggregated_provision() -> Response:
    if not ('soc_group' in request.args and 'situation' in request.args and 'function' in request.args and
            ('point' in request.args or 'municipality' in request.args or 'region' in request.args)):
        return make_response(jsonify({'error': 'Request must include all of the ("soc_group", "situation", "function", ("point" | "region" | "municipality")) arguments'}))
    soc_group: str = request.args.get('soc_group', '', type=str)
    situation: str = request.args.get('situation', '', type=str)
    function: str = request.args.get('function', '', type=str)
    region: str = ''
    municipality: str = ''
    if not ((soc_group == '' or soc_group in social_groups) and (situation == '' or situation in living_situations) and (function == '' or function in city_functions)):
        return make_response(jsonify({'error': 'At least one of the ("soc_group", "situation", "function") is not in the list of avaliable'}))
    
    if soc_group != '':
        soc_groups = [soc_group]
    else:
        soc_groups = social_groups
    
    if situation != '':
        situations = [situation]
    else:
        situations = living_situations
    
    if function != '':
        functions = [function]
    else:
        functions = city_functions

    if region == '' and municipality == '':
        where = regions
        where_type = 'regions'
    elif region == '':
        where = [municipality]
        where_type = 'municipalities'
    else:
        where = [region]
        where_type = 'regions'
    with psycopg2.connect(properties.provision_conn_string()) as conn_provision, \
            psycopg2.connect(properties.houses_conn_string()) as conn_houses:
        try:
            return make_response(jsonify({
                '_embedded': get_aggregation(conn_provision, conn_houses, where, where_type, soc_groups, situations, functions)
            }))
        except Exception as ex:
            return make_response(jsonify({'error': str(ex)}))
    
@app.route('/api/houses')
def houses_in_square() -> Response:
    if 'firstPoint' not in request.args or 'secondPoint' not in request.args:
        return make_response(jsonify({'error': '"firstPoint" and "secondPoint" must be provided as query parameters'}))
    point_1: Tuple[int, int] = tuple(map(float, request.args.get('firstPoint').split(','))) # type: ignore
    point_2: Tuple[int, int] = tuple(map(float, request.args.get('secondPoint').split(','))) # type: ignore
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
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'social_groups': social_groups
        }
    }))

@app.route('/api/list/city_functions', methods=['GET'])
def list_city_functions() -> Response:
    if 'soc_group' in request.args:
        with psycopg2.connect(properties.houses_conn_string()) as conn:
            cur = conn.cursor()
            cur.execute('SELECT f.name, significance FROM values v'
                ' inner join social_groups s on v.social_group_id = s.id'
                ' inner join city_functions f on v.city_function_id = f.id'
                ' where s.name = %s and significance > 0',
                (request.args.get('soc_group'),))
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
def list_regions() -> Response:
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'regions': regions
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
        'version': '2020-10-01',
        '_links': {
            'self': {
                'href': request.path
            },
            'atomic_provision': {
                'href': '/api/provision/atomic{?soc_group,situation,function,point}',
                'templated': True
            },
            'aggregated-provision': {
                'href': '/api/provision/aggregated{?soc_group,situation,function,region,municipality,'

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

    if 'PROVISION_API_PORT' in environ:
        properties.api_port = int(environ['PROVISION_API_PORT'])
    if 'PROVISION_DB_ADDR' in environ:
        properties.provision_db_addr = environ['PROVISION_DB_ADDR']
    if 'PROVISION_DB_NAME' in environ:
        properties.provision_db_name = environ['PROVISION_DB_NAME']
    if 'PROVISION_DB_PORT' in environ:
        properties.provision_db_port = int(environ['PROVISION_DB_PORT'])
    if 'PROVISION_DB_USER' in environ:
        properties.provision_db_user = environ['PROVISION_DB_USER']
    if 'PROVISION_DB_PASS' in environ:
        properties.provision_db_pass = environ['PROVISION_DB_PASS']
    if 'HOUSES_DB_ADDR' in environ:
        properties.houses_db_addr = environ['HOUSES_DB_ADDR']
    if 'HOUSES_DB_NAME' in environ:
        properties.houses_db_name = environ['HOUSES_DB_NAME']
    if 'HOUSES_DB_PORT' in environ:
        properties.houses_db_port = int(environ['HOUSES_DB_PORT'])
    if 'HOUSES_DB_USER' in environ:
        properties.houses_db_user = environ['HOUSES_DB_USER']
    if 'HOUSES_DB_PASS' in environ:
        properties.houses_db_pass = environ['HOUSES_DB_PASS']
    if 'PROVISION_SKIP_AGGREGATION' in environ:
        skip_aggregation = True
    if 'TRANSPORT_MODEL_ADDR' in environ:
        properties.transport_model_api_endpoint = environ['TRANSPORT_MODEL_ADDR']

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
    
    with psycopg2.connect(properties.houses_conn_string()) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT name FROM social_groups')
            social_groups = list(map(lambda x: x[0], cur.fetchall()))
            
            # cur.execute('SELECT name FROM city_functions')
            # city_functions = list(map(lambda x: x[0], cur.fetchall()))

            cur.execute('SELECT name FROM living_situations')
            living_situations = list(map(lambda x: x[0], cur.fetchall()))

            cur.execute('SELECT full_name FROM municipalities')
            municipalities = list(map(lambda x: x[0], cur.fetchall()))

            cur.execute('SELECT full_name FROM districts')
            regions = list(map(lambda x: x[0], cur.fetchall()))

    avaliability = Avaliability()

    if not skip_aggregation:
        update_all_aggregations()

    app.run(host='0.0.0.0', port=properties.api_port)
