from flask import Flask, jsonify, make_response, request, Response
from flask_compress import Compress
import psycopg2
import pandas as pd
import numpy as np
import argparse
import json
import requests
import pickle
import time
import sys
from typing import Any, Tuple, List, Dict, Optional, Union
from os import environ

class Properties:
    def __init__(self, db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, api_port: int, transport_model_api_endpoint: str):
        self.db_addr = db_addr
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.api_port = api_port
        self.transport_model_api_endpoint = transport_model_api_endpoint

class Avaliability:
    # def __init__(self, filename: Optional[str] = None):
        # d: Dict[Tuple[float, float, int, str], dict]
        # d = dict()
        # try:
        #     if filename is not None:
        #         with open(filename, 'rb') as f:
        #             d = pickle.load(f)
        # except Exception:
        #     pass
        # self.data = d

    # @classmethod
    # def from_json(self, filename: str):
    #     d: Dict[Tuple[float, float, int, str], dict]
    #     with open(filename, 'r') as f:
    #         d = json.load(f)
    #     self.data = d

    # def ensure_ready(self, lat: float, lan: float, time_walking: int, time_transport: int) -> Tuple[dict, dict]:
    #     if (lat, lan, time_walking, 'walk') not in self.data:
    #         if time_walking == 0:
    #             self.data[(lat, lan, time_walking, 'walk')] = {
    #                 'source': [
    #                     lan,
    #                     lat
    #                 ],
    #                 'cost': 0.0,
    #                 'day_time': 46800,
    #                 'mode_type': 'pt_cost',
    #                 'attr_objects': None,
    #                 'isochrones': []
    #             }
    #         else:
    #             # print('requesting')
    #             self.data[(lat, lan, time_walking, 'walk')] = requests.get(f'https://galton.urbica.co/api/foot/?lng={lat}&lat={lan}&radius=5&cellSize=0.1&intervals={time_walking}').json()
    #     if (lat, lan, time_transport, 'tran') not in self.data:
    #         if time_transport == 0:
    #             self.data[(lat, lan, time_transport, 'tran')] = {
    #                 'type': 'FeatureCollection',
    #                 'features': []
    #             }
    #         else:
    #             # print('requesting')
    #             self.data[(lat, lan, time_transport, 'tran')] = requests.post(f'{properties.transport_model_api_endpoint}', timeout = 5, json=
    #                 {
    #                     'source': [lan, lat],
    #                     'cost': time_transport * 60,
    #                     'day_time': 46800,
    #                     'mode_type': 'pt_cost'
    #                 }
    #             ).json()
    #     if (len(self.data) % 100 == 0):
    #         self.dump('avaliability_geometry.pickle')
    #         self.dump_json('avaliability_geometry.json')
    #     return (self.data[(lat, lan, time_walking, 'walk')], self.data[(lat, lan, time_transport, 'tran')])

    def get_walking(self, lat: float, lan: float, time: int):
        # if (lat, lan, time, 'walk') not in self.data:
        if time == 0:
                # self.data[(lat, lan, time, 'walk')] = {
            return {
                'source': [
                    lan,
                    lat
                ],
                'cost': 0.0,
                'day_time': 46800,
                'mode_type': 'pt_cost',
                'attr_objects': None,
                'isochrones': []
            }
        else:
            #     # print('requesting')
            # self.data[(lat, lan, time, 'walk')] = 
            return requests.get(f'https://galton.urbica.co/api/foot/?lng={lat}&lat={lan}&radius=5&cellSize=0.1&intervals={time}').json()
        # return self.data[(lat, lan, time, 'walk')]

    def get_transport(self, lat: float, lan: float, time: int) -> dict:
        # if (lat, lan, time, 'tran') not in self.data:
        if time == 0:
            # self.data[(lat, lan, time, 'tran')] = {
            return {
                'type': 'FeatureCollection',
                'features': []
            }
        else:
                # print('requesting')
                # self.data[(lat, lan, time, 'tran')] = 
            return requests.post(f'{properties.transport_model_api_endpoint}', timeout=30, json=
                {
                    'source': [lan, lat],
                    'cost': time * 60,
                    'day_time': 46800,
                    'mode_type': 'pt_cost'
                }
            ).json()
        # return self.data[(lat, lan, time, 'tran')]
    
    # def dump(self, filename: str):
    #     try:
    #         with open(filename, 'wb') as f:
    #             pickle.dump(self.data, f)
    #     except KeyboardInterrupt:
    #         print('Oh, interruption caught')
    #         with open(filename, 'wb') as f:
    #             pickle.dump(self.data, f)
    #         print('raising again')
    #         raise

    # def dump_json(self, filename: str):
    #     with open(filename, 'w') as f:
    #         json.dump(list(filter(lambda x: x[0][2] != 0, self.data.items())), f)

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
city_functions: List[str]
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
    'Религия': (('cementeries', 'кладбище'), ('churches', 'церковь')),
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

def compute_atomic_provision(conn: psycopg2.extensions.connection, soc_group: str, situation: str, function: str,
        coords: Tuple[float, float]) -> Dict[str, Any]:
    try:
        cur: psycopg2.extensions.cursor = conn.cursor()

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

        # avaliability.ensure_ready(*coords, walking_time_cost, transport_time_cost)

        # Walking

        walking_json = avaliability.get_walking(*coords, walking_time_cost)
        walking_geometry = walking_json['features'][0]['geometry']

        try:
            cur.execute('\nUNION\n'.join(
                map(lambda function_and_name: f"SELECT id, name, ST_AsGeoJSON(ST_Centroid(geometry)), capacity as power, '{function_and_name[1]}' as service_type FROM {function_and_name[0]} WHERE ST_INTERSECTS(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))", # type: ignore
                    function_service[function])), [json.dumps(walking_geometry)] * len(function_service[function]))
        except Exception as ex:
            if str(ex) == "can't execute an empty query":
                return {'error': 'For now there is no data avaliable for this service'}
            raise
        walking_ids_names: List[List[Tuple[int, str]]] = cur.fetchall()
        walking_ids = set(map(lambda id_name: id_name[0], walking_ids_names))

        # # Сохранить сервисы в датафрейм

        df_target_servs = pd.DataFrame(walking_ids_names, columns=('service_id', 'service_name', 'point', 'power', 'service_type'))
        df_target_servs['point'] = pd.Series(
            map(lambda geojson: (float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), float(geojson[geojson.rfind(',') + 1:-2])),
                df_target_servs['point'])
        )
        df_target_servs = df_target_servs.join(pd.Series([1] * df_target_servs.shape[0], name='walking_dist'))
        df_target_servs = df_target_servs.join(pd.Series([0] * df_target_servs.shape[0], name='transport_dist'))
        # df_target_servs = df_target_servs.join(pd.Series([5] * df_target_servs.shape[0], name='power'))

        # Transport

        transport_json = avaliability.get_transport(*coords, transport_time_cost)
        
        transport_geometry_sql = 'ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_SetSRID(ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\'), 4326)', transport_json['features'])) + '])'

        cur.execute('\nUNION\n'.join(
            map(lambda function_and_name: f"SELECT id, name, ST_AsGeoJSON(ST_Centroid(geometry)), capacity as power, '{function_and_name[1]}' as service_type FROM {function_and_name[0]} WHERE ST_INTERSECTS(geometry, (SELECT {transport_geometry_sql}))", # type: ignore
                function_service[function])), [json.dumps(walking_geometry)] * len(function_service[function]))
        transport_ids_names = list(filter(lambda id_name: id_name[0] not in walking_ids, cur.fetchall()))
        cur.execute(f'SELECT ST_AsGeoJSON({transport_geometry_sql})')
        transport_geometry = cur.fetchall()[0][0]

        transport_servs = pd.DataFrame(transport_ids_names, columns=('service_id', 'service_name', 'point', 'power', 'service_type'))
        transport_servs['point'] = pd.Series(
            map(lambda geojson: (round(float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), 2), round(float(geojson[geojson.rfind(',') + 1:-2]), 2)),
                transport_servs['point'])
        )
        transport_servs = transport_servs.join(pd.Series([0] * transport_servs.shape[0], name='walking_dist'))
        transport_servs = transport_servs.join(pd.Series([1] * transport_servs.shape[0], name='transport_dist'))
        # transport_servs = transport_servs.join(pd.Series([5] * transport_servs.shape[0], name='power'))

        df_target_servs = df_target_servs.append(transport_servs, ignore_index=True)
        transport_servs = None
    except Exception as ex:
        print(ex)
        return {'error': str(ex)}
    finally:
        cur.close() # type: ignore

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
        'walking_geometry': walking_geometry,
        'transport_geometry': json.loads(transport_geometry),
        'services': list(df_target_servs.transpose().to_dict().values()),
        'provision_result': target_O,
        'parameters': {
            'walking_time_cost': walking_time_cost,
            'transport_time_cost': transport_time_cost,
            'personal_transport_time_cost': personal_transport_cost,
            'intensity': intensity,
            'significance': significance
        }
    }

def get_aggregation(conn: psycopg2.extensions.connection, where: Union[List[str]], where_type: str, soc_groups: Union[str, List[str]], situations: Union[List[str]], functions: Union[List[str]], update: bool = False) -> dict:
    cur: psycopg2.extensions.cursor = conn.cursor()

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
    where = sorted(where)

    cur.execute('SELECT id, avg_intensity, avg_significance, total_value, constituents, time_done from provision_aggregation')
    for id, intensity, significance, provision, consituents, done in cur.fetchall():
        # consituents = json.loads(consituents)
        # print(id, intensity, significance, provision, consituents, done)
        if sorted(consituents['districts' if where_type == 'regions' else 'municipalities']) == where and sorted(consituents['soc_groups']) == soc_groups and \
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
    where_str = '(' + ', '.join(map(lambda x: f"'{x}'", where)) + ')'
    cur.execute(f'SELECT ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float, ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float FROM houses WHERE {"district_id" if where_type == "regions" else "municipality_id"} in (SELECT id FROM {"districts" if where_type == "regions" else "municipalities"} WHERE full_name in {where_str}))')
    houses: List[Tuple[float, float]] = list(map(lambda x: (x[0], x[1]), cur.fetchall()))

    cnt = 0
    provision = 0.0
    intensity = 0.0
    significance = 0.0
    for soc_group in soc_groups:
        for situation in situations:
            for function in functions:
                for house in houses:
                    prov = compute_atomic_provision(conn, soc_group, situation, function, house)
                    if 'error' in prov:
                        continue
                    provision += prov['provision_result']
                    intensity += prov['parameters']['intensity'] # type: ignore
                    significance += prov['parameters']['significance'] # type: ignore
                    cnt += 1
    if cnt != 0:
        provision /= cnt
        intensity /= cnt
        significance /= cnt
    done_time: Any = time.localtime()
    done_time = f'{done_time.tm_year}-{done_time.tm_mon}-{done_time.tm_mday} {done_time.tm_hour}:{done_time.tm_min}:{done_time.tm_sec}'

    if found_id is None:
        cur.execute('INSERT INTO provision_aggregation (avg_intensity, avg_significance, total_value, constituents, time_done) VALUES (%s, %s, %s, %s, %s)',
                (intensity, significance, provision, json.dumps({
                        'soc_groups': soc_groups,
                        'functions': functions,
                        'situations': situations,
                        'districts': where if where_type == 'regions' else [],
                        'municipalities': where if where_type == 'municipalities' else []
                }), done_time))
    else:
        cur.execute('UPDATE provision_aggregation SET avg_intensity = %s, avg_significance = %s, total_value = %s, time_done = %s WHERE id = %s', 
                (intensity, significance, provision, done_time, found_id))
    return {
        'provision': provision,
        'intensity': intensity,
        'significance': significance,
        'time_done': done_time
    }

# def update_all_aggregations():
#     full_start = time.time()
#     try:
#         conn: psycopg2.extensions.connection = psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
#             f' user={properties.db_user} password={properties.db_pass}')
#         for soc_group in social_groups + [social_groups]:
#             for situation in living_situations + [living_situations]:
#                 for function in city_functions + [city_functions]:
#                     for region in regions + [regions]:
#                         print(f'Aggregating soc_group({soc_group}) + situation({situation}) + function({function}) + region({region}): ', end='')
#                         sys.stdout.flush()
#                         start = time.time()
#                         res = get_aggregation(conn, region, 'regions', soc_group, situation, function, True)
#                         print(f'finished in {time.time() - start:6.2f} seconds (total_value = {res["provision"]})')
#                     for municipality in municipalities + [municipalities]:
#                         print(f'Aggregating soc_group({soc_group}) + situation({situation}) + function({function}) + municipality({municipality}): ', end='')
#                         sys.stdout.flush()
#                         start = time.time()
#                         res = get_aggregation(conn, municipality, 'municipalities', soc_group, situation, function, True)
#                         print(f'finished in {time.time() - start:6.2f} seconds (total_value = {res["provision"]})')
#     finally:
#         print(f'Finished updating all agregations in {time.time() - full_start:.2} seconds')
#         # avaliability.dump('avaliability_geometry_final.pickle')
#         # avaliability.dump_json('avaliability_geometry_final.json')

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

    # Вернуть значение обеспеченности
    try:
        conn: psycopg2.extensions.conneection = psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
            f' user={properties.db_user} password={properties.db_pass}')
        return make_response(jsonify({'_embedded': compute_atomic_provision(conn, soc_group, situation, function, coords)}))
    except Exception as ex:
        return make_response(jsonify({'error': str(ex)}))

# @app.route('/api/provision/aggregated', methods=['GET'])
def aggregated_provision() -> Response:
    if not ('soc_group' in request.args and 'situation' in request.args and 'function' in request.args and 'point' in request.args):
        return make_response(jsonify({'error': 'Request must include all of the ("soc_group", "situation", "function", "point") arguments'}))
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
    try:
        conn: psycopg2.extensions.cursor = psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
            f' user={properties.db_user} password={properties.db_pass}')
        return make_response(jsonify({
            '_embedded': get_aggregation(conn, where, where_type, soc_groups, situations, functions)
        }))
    except Exception as ex:
        return make_response(jsonify({'error': str(ex)}))
    

@app.route('/api/list/social_groups', methods=['GET'])
def list_social_groups() -> Response:
    return make_response(jsonify({
        '_embedded': {
            'social_groups': social_groups
        }
    }))

@app.route('/api/list/city_functions', methods=['GET'])
def list_city_functions() -> Response:
    return make_response(jsonify({
        '_embedded': {
            'city_functions': city_functions
        }
    }))

@app.route('/api/list/living_situations', methods=['GET'])
def list_living_situations() -> Response:
    return make_response(jsonify({
        '_embedded': {
            'living_situations': living_situations
        }
    }))

@app.route('/api/list/municipalities', methods=['GET'])
def list_municipalities() -> Response:
    return make_response(jsonify({
        '_embedded': {
            'municipalities': municipalities
        }
    }))

@app.route('/api', methods=['GET'])
def api_help() -> Response:
    return make_response(jsonify({
        'version': '2020-09-19',
        '_links': {
            'self': {
                'href': request.path
            },
            'atomic_provision': {
                'href': '/api/provision/atomic{?soc_group,situation,function,point}',
                'templated': True
            },
            'list-social_groups': {
                'href': '/api/list/social_groups'
            },
            'list-living_situations': {
                'href': '/api/list/living_situations'
            },
            'list-city_functions': {
                'href': '/api/list/city_functions'
            }
        }
    }))

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':

    # Default properties settings

    properties = Properties('localhost', 5432, 'citydb', 'postgres', 'postgres', 8080, 'http://10.32.1.61:8080/api.v2/isochrones')

    # Environment variables

    if 'PROVISION_API_PORT' in environ:
        properties.api_port = int(environ['PROVISION_API_PORT'])
    if 'PROVISION_DB_ADDR' in environ:
        properties.db_addr = environ['PROVISION_DB_ADDR']
    if 'PROVISION_DB_NAME' in environ:
        properties.db_name = environ['PROVISION_DB_NAME']
    if 'PROVISION_DB_PORT' in environ:
        properties.db_port = int(environ['PROVISION_DB_PORT'])
    if 'PROVISION_DB_USER' in environ:
        properties.db_user = environ['PROVISION_DB_USER']
    if 'PROVISION_DB_PASS' in environ:
        properties.db_pass = environ['PROVISION_DB_PASS']
    if 'TRANSPORT_MODEL_ADDR' in environ:
        properties.transport_model_api_endpoint = environ['TRANSPORT_MODEL_ADDR']

    # CLI Arguments

    parser = argparse.ArgumentParser(
        description='Starts up the provision API server')
    parser.add_argument('-H', '--db_addr', action='store', dest='db_addr',
                        help=f'postgres host address [default: {properties.db_addr}]', type=str)
    parser.add_argument('-P', '--db_port', action='store', dest='db_port',
                        help=f'postgres port number [default: {properties.db_port}]', type=int)
    parser.add_argument('-d', '--db_name', action='store', dest='db_name',
                        help=f'postgres database name [default: {properties.db_name}]', type=str)
    parser.add_argument('-U', '--db_user', action='store', dest='db_user',
                        help=f'postgres user name [default: {properties.db_user}]', type=str)
    parser.add_argument('-W', '--db_pass', action='store', dest='db_pass',
                        help=f'database user password [default: {properties.db_pass}]', type=str)
    parser.add_argument('-p', '--port', action='store', dest='api_port',
                        help=f'postgres port number [default: {properties.api_port}]', type=int)
    # parser.add_argument('-S', '--skip_aggregation', action='store_true', dest='skip_aggregation',
    #                     help=f'skip the process of calculation of aggregations')
    parser.add_argument('-T', '--transport_model_api', action='store', dest='transport_model_api_endpoint',
                        help=f'url of transport model api [default: {properties.transport_model_api_endpoint}]', type=str)
    args = parser.parse_args()

    if args.db_addr is not None:
        properties.db_addr = args.db_addr
    if args.db_port is not None:
        properties.db_port = args.db_port
    if args.db_name is not None:
        properties.db_name = args.db_name
    if args.db_user is not None:
        properties.db_user = args.db_user
    if args.db_pass is not None:
        properties.db_pass = args.db_pass
    if args.api_port is not None:
        properties.api_port = args.api_port
    
    try:
        conn: psycopg2.extensions.connection = psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
            f' user={properties.db_user} password={properties.db_pass}')
        cur: psycopg2.extensions.cursor = conn.cursor()

        cur.execute('SELECT name FROM social_groups')
        social_groups = list(map(lambda x: x[0], cur.fetchall()))
        
        cur.execute('SELECT name FROM city_functions')
        city_functions = list(map(lambda x: x[0], cur.fetchall()))

        cur.execute('SELECT name FROM living_situations')
        living_situations = list(map(lambda x: x[0], cur.fetchall()))

        cur.execute('SELECT full_name FROM municipalities')
        municipalities = list(map(lambda x: x[0], cur.fetchall()))

        cur.execute('SELECT full_name FROM districts')
        regions = list(map(lambda x: x[0], cur.fetchall()))

    finally:
        cur.close() # type: ignore
        conn.close() # type: ignore

    avaliability = Avaliability() #'availability_geometry.pickle')

    # if not args.skip_aggregation:
    #     update_all_aggregations()

    app.run(host='0.0.0.0', port=properties.api_port)
