from flask import Flask, jsonify, make_response, request, Response
from flask_compress import Compress
import psycopg2
import pandas as pd
import numpy as np
import argparse
import json
import requests
from typing import Tuple, NamedTuple, List, Dict, Optional

class Properties(NamedTuple):
    db_name: str
    db_addr: str
    db_user: str
    db_pass: str
    db_port: int
    api_port: int
    transport_model_api: str

properties: Properties

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

    try:
        conn: psycopg2.extensions.connection = psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
            f' user={properties.db_user} password={properties.db_pass}')
        cursor: psycopg2.extensions.cursor = conn.cursor()

        cursor.execute(database_needs_sql, (soc_group, situation, function))
        _, _, _, _, walking_time_cost, transport_time_cost, personal_transport_cost, intensity = cursor.fetchall()[0]

        cursor.execute(database_significance_sql, (soc_group, function))
        significance = cursor.fetchall()[0][0]

        # Walking

        walking_json = requests.get(f'https://galton.urbica.co/api/foot/?lng={coords[0]}&lat={coords[1]}&radius=5&cellSize=0.1&intervals={walking_time_cost}').json()
        walking_geometry = walking_json['features'][0]['geometry']

        try:
            cursor.execute('\nUNION\n'.join(
                map(lambda function_and_name: f"SELECT id, name, ST_AsGeoJSON(ST_Centroid(geometry)), '{function_and_name[1]}' as service_name FROM {function_and_name[0]} WHERE ST_INTERSECTS(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))", # type: ignore
                    function_service[function])), [json.dumps(walking_geometry)] * len(function_service[function]))
        except Exception as e:
            if str(e) == "can't execute an empty query":
                return make_response(jsonify({'error': 'For now there is no data avaliable for this service'}))
            raise   
        walking_ids_names: List[List[Tuple[int, str]]] = cursor.fetchall()
        walking_ids = set(map(lambda id_name: id_name[0], walking_ids_names))

        # # Сохранить сервисы в датафрейм

        df_target_servs = pd.DataFrame(walking_ids_names, columns=('service_id', 'service_name', 'point', 'service_name'))
        df_target_servs['point'] = pd.Series(
            map(lambda geojson: (float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), float(geojson[geojson.rfind(',') + 1:-2])),
                df_target_servs['point'])
        )
        df_target_servs = df_target_servs.join(pd.Series([1] * df_target_servs.shape[0], name='walking_dist'))
        df_target_servs = df_target_servs.join(pd.Series([0] * df_target_servs.shape[0], name='transport_dist'))
        df_target_servs = df_target_servs.join(pd.Series([5] * df_target_servs.shape[0], name='power'))

        # Transport

        transport_json = requests.post(f'{properties.transport_model_api}/matsim-routing/api/isochrones', json=
            {
                'source': [coords[1], coords[0]],
                'cost': transport_time_cost * 60,
                'day_time': 46800,
                'mode_type': 'pt_cost'
            }
        ).json()
        
        transport_geometry_sql = 'ST_UNION(ARRAY[' + ',\n'.join(map(lambda iso: f"ST_GeomFromGeoJSON('{json.dumps(iso['isochrone'])}')", transport_json['isochrones'])) + '\n])'

        cursor.execute('\nUNION\n'.join(
            map(lambda function_and_name: f"SELECT id, name, ST_AsGeoJSON(ST_Centroid(geometry)), '{function_and_name[1]}' as service_name FROM {function_and_name[0]} WHERE ST_INTERSECTS(geometry, (SELECT {transport_geometry_sql}))", # type: ignore
                function_service[function])), [json.dumps(walking_geometry)] * len(function_service[function]))
        transport_ids_names = list(filter(lambda id_name: id_name[0] not in walking_ids, cursor.fetchall()))
        cursor.execute(f'SELECT ST_AsGeoJSON({transport_geometry_sql})')
        transport_geometry = cursor.fetchall()[0][0]

        transport_servs = pd.DataFrame(transport_ids_names, columns=('service_id', 'service_name', 'point', 'service_name'))
        transport_servs['point'] = pd.Series(
            map(lambda geojson: (round(float(geojson[geojson.find('[') + 1:geojson.rfind(',')]), 2), round(float(geojson[geojson.rfind(',') + 1:-2]), 2)),
                transport_servs['point'])
        )
        transport_servs = transport_servs.join(pd.Series([0] * transport_servs.shape[0], name='walking_dist'))
        transport_servs = transport_servs.join(pd.Series([1] * transport_servs.shape[0], name='transport_dist'))
        transport_servs = transport_servs.join(pd.Series([5] * transport_servs.shape[0], name='power'))

        df_target_servs = df_target_servs.append(transport_servs, ignore_index=True)
        transport_servs = None
    except Exception as e:
        return make_response(jsonify({'error': str(e)}))
    finally:
        cursor.close() # type: ignore
        conn.close() # type: ignore

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

    # Вернуть значение обеспеченности
    return make_response(jsonify({
        '_embedded': {
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
    }))

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

@app.route('/api', methods=['GET'])
def api_help() -> Response:
    return make_response(jsonify({
        'version': '2020-09-14',
        '_links': {
            'self': {
                'href': request.path
            },
            'atomic_provision': {
                'href': '/api/provision/atomic'
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
    parser = argparse.ArgumentParser(
        description='Starts up the provision API server')
    parser.add_argument('-H', '--db_addr', action='store', dest='db_addr',
                        help='postgres host address [default: localhost]', type=str, default='localhost')
    parser.add_argument('-d', '--db_name', action='store', dest='db_name',
                        help='postgres database name [default: citydb]', type=str, default='citydb')
    parser.add_argument('-U', '--db_user', action='store', dest='db_user',
                        help='postgres user name [default: postgres]', type=str, default='postgres')
    parser.add_argument('-W', '--db_pass', action='store', dest='db_pass',
                        help='database user password [default: postgres]', type=str, default='postgres')
    parser.add_argument('-P', '--db_port', action='store', dest='db_port',
                        help='postgres port number [default: 5432]', type=int, default=5432)
    parser.add_argument('-p', '--port', action='store', dest='api_port',
                        help='postgres port number [default: 8080]', type=int, default=8080)
    parser.add_argument('-T', '--transport_model_api', action='store', dest='transport_model_api',
                        help='url of transport model api [default: http://10.32.1.61:8080]', type=str, default='http://10.32.1.61:8080')
    args = parser.parse_args()

    properties = Properties(args.db_name, args.db_addr, args.db_user, args.db_pass, args.db_port, args.api_port, args.transport_model_api)

    try:
        conn: psycopg2.extensions.connection = psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
            f' user={properties.db_user} password={properties.db_pass}')
        cursor: psycopg2.extensions.cursor = conn.cursor()

        cursor.execute('SELECT name FROM social_groups')
        social_groups = list(map(lambda x: x[0], cursor.fetchall()))
        
        cursor.execute('SELECT name FROM city_functions')
        city_functions = list(map(lambda x: x[0], cursor.fetchall()))

        cursor.execute('SELECT name FROM living_situations')
        living_situations = list(map(lambda x: x[0], cursor.fetchall()))

    finally:
        cursor.close() # type: ignore
        conn.close() # type: ignore

    app.run(port=properties.api_port)
