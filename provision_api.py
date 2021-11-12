import traceback
from flask import Flask, jsonify, make_response, request, Response
from flask_compress import Compress
import psycopg2
import pandas as pd, numpy as np
import argparse
import simplejson as json
import itertools
import os
from typing import Any, Literal, Tuple, List, Dict, Optional, Union, NamedTuple

import logging

log = logging.getLogger(__name__)

class NonASCIIJSONEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        kwargs['ensure_ascii'] = False
        super().__init__(**kwargs)

class Properties:
    def __init__(self, db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, api_port: int):
        self.db_addr = db_addr
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.api_port = api_port
        self._conn: Optional[psycopg2.extensions.connection] = None

    @property
    def conn_string(self) -> str:
        return f'host={self.db_addr} port={self.db_port} dbname={self.db_name}' \
                f' user={self.db_user} password={self.db_pass}'

    @property
    def conn(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.conn_string)
        return self._conn
            
    def close(self):
        if self.conn is not None:
            self._conn.close()

properties: Properties

needs: pd.DataFrame
infrastructure: pd.DataFrame
blocks: pd.DataFrame
city_hierarchy: pd.DataFrame

Listings = NamedTuple('Listings', [
    ('infrastructures', pd.DataFrame),
    ('city_functions', pd.DataFrame),
    ('service_types', pd.DataFrame),
    ('living_situations', pd.DataFrame),
    ('social_groups', pd.DataFrame)
])
listings: Listings

provision_administrative_units: pd.DataFrame
provision_municipalities: pd.DataFrame
provision_blocks: pd.DataFrame

city_name = 'Санкт-Петербург'

def update_global_data() -> None:
    global needs
    global infrastructure
    global listings
    global blocks
    global city_hierarchy
    global provision_administrative_units
    global provision_municipalities
    global provision_blocks
    with properties.conn.cursor() as cur:
        cur.execute('SELECT it.id, it.name, it.code, cf.id, cf.name, cf.code, st.id, st.name, st.code FROM city_functions cf'
                '   JOIN city_infrastructure_types it ON cf.city_infrastructure_type_id = it.id'
                '   JOIN city_service_types st ON st.city_function_id = cf.id'
                ' ORDER BY it.name, cf.name, st.name')
        infrastructure = pd.DataFrame(cur.fetchall(),
                columns=('infrastructure_id', 'infrastructure', 'infrastructure_code', 'city_function_id', 'city_function',
                        'city_function_code', 'service_type_id', 'service_type', 'service_type_code'))

        cur.execute('SELECT s.name, l.name, st.name, n.walking, n.public_transport, n.personal_transport, n.intensity FROM needs n'
                ' JOIN social_groups s ON s.id = n.social_group_id'
                ' JOIN living_situations l ON l.id = n.living_situation_id'
                ' JOIN city_service_types st ON st.id = n.city_service_type_id'
                ' ORDER BY 1, 2, 3')
        needs = pd.DataFrame(cur.fetchall(), columns=('social_group', 'living_situation', 'service_type', 'walking', 'transport', 'car', 'intensity'))
        cur.execute('SELECT s.name, st.name, v.significance FROM values v'
                '   JOIN social_groups s ON s.id = v.social_group_id'
                '   JOIN city_functions f ON f.id = v.city_function_id'
                '   JOIN city_service_types st ON st.city_function_id = f.id')
        tmp = pd.DataFrame(cur.fetchall(), columns=('social_group', 'service_type', 'significance'))
        needs = needs.merge(tmp, on=['social_group', 'service_type'], how='inner')

        cur.execute('SELECT m.id, m.name, m.population, au.id, au.name, au.population FROM municipalities m'
                '   LEFT JOIN administrative_units au on m.admin_unit_parent_id = au.id'
                ' WHERE m.city_id = (SELECT id from cities WHERE name = %s)'
                ' ORDER BY au.name, m.name', (city_name,))
        city_hierarchy = pd.DataFrame(cur.fetchall(), columns=('municipality_id', 'municipality',
                'municipality_population', 'district_id', 'district', 'district_population'))

        cur.execute('SELECT b.id, b.population, m.name as municipality, au.name as district FROM blocks b'
                '   LEFT JOIN municipalities m ON st_within(b.center, m.geometry)'
                '   LEFT JOIN administrative_units au ON au.id = m.admin_unit_parent_id'
                ' WHERE b.city_id = (SELECT id from cities WHERE name = %s)'
                ' ORDER BY 4, 3, 1', (city_name,))
        blocks = pd.DataFrame(cur.fetchall(), columns=('id', 'population', 'municipality', 'district')).set_index('id')
        blocks['population'] = blocks['population'].replace({np.nan: None})

        cur.execute('SELECT loc.name, st.name, houses.count, prov.count, prov.service_load_mean, prov.service_load_sum,'
                '   houses.provision_mean, prov.evaluation_mean, prov.reserve_resources_mean, prov.reserve_resources_sum,'
                '   houses.reserve_resources_mean, houses.reserve_resources_sum'
                ' FROM provision.services_administrative_units prov'
                '   JOIN provision.houses_administrative_units houses ON'
                '       houses.city_service_type_id = prov.city_service_type_id and houses.administrative_unit_id = prov.administrative_unit_id'
                '   JOIN administrative_units loc ON prov.administrative_unit_id = loc.id'
                '   JOIN city_service_types st ON prov.city_service_type_id = st.id'
                ' WHERE loc.city_id = (SELECT id from cities WHERE name = %s)'
                ' ORDER BY 1, 2',
                (city_name,)
        )
        provision_administrative_units = pd.DataFrame(cur.fetchall(), columns=('district', 'service_type', 'houses_count', 'services_count',
                'services_load_mean', 'services_load_sum', 'houses_provision', 'services_evaluation', 'services_reserve_mean', 'services_reserve_sum',
                'houses_reserve_mean', 'houses_reserve_sum'))

        cur.execute('SELECT loc.name, st.name, houses.count, eval.count, eval.service_load_mean, eval.service_load_sum,'
                '   houses.provision_mean, eval.evaluation_mean, eval.reserve_resources_mean, eval.reserve_resources_sum,'
                '   houses.reserve_resources_mean, houses.reserve_resources_sum'
                ' FROM provision.services_municipalities eval'
                '   JOIN provision.houses_municipalities houses ON'
                '       houses.city_service_type_id = eval.city_service_type_id and houses.municipality_id = eval.municipality_id'
                '   JOIN municipalities loc ON eval.municipality_id = loc.id'
                '   JOIN city_service_types st ON eval.city_service_type_id = st.id'
                ' WHERE loc.city_id = (SELECT id from cities WHERE name = %s)'
                ' ORDER BY 1, 2',
                (city_name,)
        )
        provision_municipalities = pd.DataFrame(cur.fetchall(), columns=('municipality', 'service_type', 'houses_count', 'services_count',
                'services_load_mean', 'services_load_sum', 'houses_provision', 'services_evaluation', 'services_reserve_mean',
                'services_reserve_sum', 'houses_reserve_mean', 'houses_reserve_sum'))

        cur.execute('SELECT eval.block_id, s.name, houses.count, eval.count, eval.service_load_mean, eval.service_load_sum,'
                '   houses.provision_mean, eval.evaluation_mean, eval.reserve_resources_mean, eval.reserve_resources_sum,'
                '   houses.reserve_resources_mean, houses.reserve_resources_sum'
                ' FROM provision.services_blocks eval'
                '   JOIN blocks b ON eval.block_id = b.id'
                '   JOIN provision.houses_blocks houses ON houses.city_service_type_id = eval.city_service_type_id and houses.block_id = eval.block_id'
                '   JOIN city_service_types s ON eval.city_service_type_id = s.id'
                ' WHERE b.city_id = (SELECT id from cities WHERE name = %s)'
                ' ORDER BY 1, 2',
                (city_name,)
        )
        provision_blocks = pd.DataFrame(cur.fetchall(), columns=('block', 'service_type', 'houses_count', 'services_count', 'services_load_mean',
                'services_load_sum', 'houses_provision', 'services_evaluation', 'services_reserve_mean', 'services_reserve_sum',
                'houses_reserve_mean', 'houses_reserve_sum'))

        cur.execute('SELECT id, name, code FROM city_functions ORDER BY name')
        city_functions = pd.DataFrame(cur.fetchall(), columns=('id', 'name', 'code'))
        cur.execute('SELECT id, name, code FROM living_situations ORDER BY name')
        living_situations = pd.DataFrame(cur.fetchall(), columns=('id', 'name', 'code'))
        cur.execute('SELECT id, name, code FROM social_groups ORDER BY name')
        social_groups = pd.DataFrame(cur.fetchall(), columns=('id', 'name', 'code'))
        cur.execute('SELECT id, name, code FROM city_infrastructure_types ORDER BY name')
        infrastructures = pd.DataFrame(cur.fetchall(), columns=('id', 'name', 'code'))
        cur.execute('SELECT id, name, code FROM city_service_types ORDER BY name')
        service_types = pd.DataFrame(cur.fetchall(), columns=('id', 'name', 'code'))
        listings = Listings(infrastructures, city_functions, service_types, living_situations, social_groups)
    # blocks['population'] = blocks['population'].fillna(-1).astype(int)


def get_parameter_of_request(
        input_value: Optional[Union[str, int]],
        type_of_input: Literal['service_type', 'city_function', 'infrastructure', 'living_situation', 'social_group'],
        what_to_get: Literal['name', 'code', 'id'] = 'name',
        raise_errors: bool = False) -> Optional[Union[int, str]]:
    if input_value is None:
        return None
    if type_of_input in ('service_type', 'city_function', 'infrastructure', 'social_group', 'living_situation'):
        if what_to_get not in ('name', 'code', 'id'):
            if raise_errors:
                raise ValueError(f'"{what_to_get}" could not be get from {type_of_input}')
            else:
                return None
        source = {'service_type': listings.service_types, 'city_function': listings.city_functions, 'infrastructure': listings.infrastructures,
                'social_group': listings.social_groups, 'living_situation': listings.living_situations}[type_of_input]
        res = None
        if isinstance(input_value, int) or input_value.isnumeric():
            if int(input_value) not in source['id'].unique():
                if raise_errors:
                    raise ValueError(f'id={input_value} is given for {type_of_input}, but it is out of bounds')
                else:
                    return None
            res = source[source['id'] == int(input_value)].iloc[0][what_to_get]
        if input_value in source['name'].unique():
            res = source[source['name'] == input_value].iloc[0][what_to_get]
        if input_value in source['code'].unique():
            res = source[source['code'] == input_value].iloc[0][what_to_get]
        if res is not None:
            if what_to_get == 'id':
                return int(res)
            else:
                return res
        if raise_errors:
            raise ValueError(f'"{input_value}" is not found in ids, names or codes of {type_of_input}s')
        else:
            return None
    else:
        if raise_errors:
            raise ValueError(f'Unsupported type_of_input: {type_of_input}')
        else:
            return None


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
    global city_name
    if 'city' in request.args:
        city_name = request.args['city']
    update_global_data()
    return make_response('OK\n')

def get_social_groups(service_type: Optional[Union[str, int]] = None, living_situation: Optional[Union[str, int]] = None,
        to_list: bool = False) -> Union[List[str], pd.DataFrame]:
    service_type = get_parameter_of_request(service_type, 'service_type', 'name')
    living_situation = get_parameter_of_request(living_situation, 'living_situation', 'name')
    res = needs[(needs['significance'] > 0) & (needs['intensity'] > 0)]
    if living_situation is None:
        res = res.drop(['living_situation', 'intensity', 'walking', 'transport', 'car'], axis=1).drop_duplicates()
    else:
        res = res[res['living_situation'] == living_situation].drop('living_situation', axis=1)
    res = res[res['social_group'].apply(lambda name: name[-1] == ')')] \
        .merge(listings.social_groups, left_on='social_group', right_on='name') \
        .sort_values('id') \
        .drop(['id', 'name', 'code'], axis=1)
    if service_type is None:
        res = res.drop(['service_type', 'significance'], axis=1)
    else:
        res = res[res['service_type'] == service_type].drop('service_type', axis=1)
    if to_list:
        return list(res['social_group'].unique())
    else:
        return res

@app.route('/api/relevance/social_groups', methods=['GET'])
@app.route('/api/relevance/social_groups/', methods=['GET'])
def relevant_social_groups() -> Response:
    res: pd.DataFrame = get_social_groups(request.args.get('service_type'), request.args.get('living_situation'))
    res = res.merge(listings.social_groups.set_index('name'), how='inner', left_on='social_group', right_index=True)
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'service_type': request.args.get('service_type'),
                'living_situation': request.args.get('living_situation')
            },
            'social_groups': list(res.drop_duplicates().replace({np.nan: None}).transpose().to_dict().values()),
        }
    }))

@app.route('/api/list/social_groups', methods=['GET'])
@app.route('/api/list/social_groups/', methods=['GET'])
def list_social_groups() -> Response:
    res: List[str] = get_social_groups(request.args.get('service_type'), request.args.get('living_situation'), to_list=True)
    ids = list(listings.social_groups.set_index('name').loc[list(res)]['id'])
    codes = list(listings.social_groups.set_index('name').loc[list(res)]['code'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'service_type': request.args.get('service_type'),
                'living_situation': request.args.get('living_situation')
            },
            'social_groups': res,
            'social_groups_ids': ids,
            'social_groups_codes': codes
        }
    }))


def get_city_functions(social_group: Optional[Union[str, int]] = None, living_situation: Optional[Union[str, int]] = None,
        to_list: bool = False) -> Union[List[str], pd.DataFrame]:
    social_group = get_parameter_of_request(social_group, 'social_group', 'name')
    living_situation = get_parameter_of_request(living_situation, 'living_situation', 'name')
    res = needs[(needs['significance'] > 0) & (needs['intensity'] > 0) & needs['service_type'].isin(infrastructure['service_type'].dropna().unique())]
    if social_group is None:
        res = res.drop(['social_group', 'significance'], axis=1)
    else:
        res = res[res['social_group'] == social_group].drop('social_group', axis=1)
    if living_situation is None:
        res = res.drop(['living_situation', 'intensity', 'walking', 'transport', 'car'], axis=1).drop_duplicates()
    else:
        res = res[res['living_situation'] == living_situation].drop('living_situation', axis=1)
    if to_list:
        return list(infrastructure[infrastructure['service_type'].isin(res['service_type'].unique())]['city_function'].unique())
    else:
        return res.join(infrastructure[['city_function', 'service_type']].set_index('service_type'), on='service_type', how='inner') \
                .drop('service_type', axis=1).drop_duplicates()

@app.route('/api/relevance/city_functions', methods=['GET'])
@app.route('/api/relevance/city_functions/', methods=['GET'])
def relevant_city_functions() -> Response:
    res: pd.DataFrame = get_city_functions(request.args.get('social_group'), request.args.get('living_situation'))
    res = res.merge(listings.city_functions.set_index('name'), how='inner', left_on='city_function', right_index=True)
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'living_situation': request.args.get('living_situation')
            },
            'city_functions': list(res.replace({np.nan: None}).transpose().to_dict().values()),
        }
    }))

@app.route('/api/list/city_functions', methods=['GET'])
@app.route('/api/list/city_functions/', methods=['GET'])
def list_city_functions() -> Response:
    res: List[str] = sorted(get_city_functions(request.args.get('social_group'), request.args.get('living_situation'), to_list=True))
    ids = list(listings.city_functions.set_index('name').loc[list(res)]['id'])
    codes = list(listings.city_functions.set_index('name').loc[list(res)]['code'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'living_situation': request.args.get('living_situation')
            },
            'city_functions': res,
            'city_functions_ids': ids,
            'city_functions_codes': codes
        }
    }))

def get_service_types(social_group: Optional[Union[str, int]] = None, living_situation: Optional[Union[str, int]] = None,
        to_list: bool = False) -> Union[List[str], pd.DataFrame]:
    social_group = get_parameter_of_request(social_group, 'social_group', 'name')
    living_situation = get_parameter_of_request(living_situation, 'living_situation', 'name')
    res = needs[(needs['significance'] > 0) & (needs['intensity'] > 0) & needs['service_type'].isin(infrastructure['service_type'].dropna().unique())]
    if social_group is None:
        res = res.drop(['social_group', 'significance'], axis=1)
    else:
        res = res[res['social_group'] == social_group].drop('social_group', axis=1)
    if living_situation is None:
        res = res.drop(['living_situation', 'intensity', 'walking', 'transport', 'car'], axis=1)
    else:
        res = res[res['living_situation'] == living_situation].drop('living_situation', axis=1)
    if to_list:
        return list(res['service_type'].unique())
    else:
        return res.drop_duplicates()

@app.route('/api/relevance/service_types', methods=['GET'])
@app.route('/api/relevance/service_types/', methods=['GET'])
def relevant_service_types() -> Response:
    res: pd.DataFrame = get_service_types(request.args.get('social_group'), request.args.get('living_situation'))
    res = res.merge(listings.service_types.set_index('name'), how='inner', left_on='service_type', right_index=True)
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'living_situation': request.args.get('living_situation')
            },
            'service_types': list(res.replace({np.nan: None}).transpose().to_dict().values()),
        }
    }))

@app.route('/api/list/service_types', methods=['GET'])
@app.route('/api/list/service_types/', methods=['GET'])
def list_service_types() -> Response:
    res: List[str] = sorted(get_service_types(request.args.get('social_group'), request.args.get('living_situation'), to_list=True))
    ids = list(listings.service_types.set_index('name').loc[list(res)]['id'])
    codes = list(listings.service_types.set_index('name').loc[list(res)]['code'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'living_situation': request.args.get('living_situation')
            },
            'service_types': res,
            'service_types_ids': ids,
            'service_types_codes': codes
        }
    }))

def get_living_situations(social_group: Optional[Union[str, int]] = None, service_type: Optional[Union[str, int]] = None,
        to_list: bool = False) -> Union[List[str], pd.DataFrame]:
    social_group = get_parameter_of_request(social_group, 'social_group', 'name')
    service_type = get_parameter_of_request(service_type, 'service_type', 'name')
    res = needs[(needs['significance'] > 0) & (needs['intensity'] > 0)]
    if social_group is not None and service_type is not None:
        res = res[(res['social_group'] == social_group) & (res['service_type'] == service_type)].drop(['service_type', 'social_group'], axis=1)
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
    res = res.merge(listings.living_situations.set_index('name'), how='inner', left_on='living_situation', right_index=True)
    significance: Optional[int] = None
    if 'significance' in res.columns:
        if res.shape[0] > 0:
            significance = next(iter(res['significance']))
        res = res.drop('significance', axis=1)
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'social_group': request.args.get('social_group'),
                'service_type': request.args.get('service_type'),
                'significance': significance
            },
            'living_situations': list(res.drop_duplicates().transpose().to_dict().values()),
        }
    }))

@app.route('/api/list/living_situations', methods=['GET'])
@app.route('/api/list/living_situations/', methods=['GET'])
def list_living_situations() -> Response:
    res: List[str] = get_living_situations(request.args.get('social_group'), request.args.get('service_type'), to_list=True)
    ids = list(listings.living_situations.set_index('name').loc[list(res)]['id'])
    codes = list(listings.living_situations.set_index('name').loc[list(res)]['code'])
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'params': {
                'social_group': request.args.get('social_group'),
                'service_type': request.args.get('service_type'),
            },
            'living_situations': res,
            'living_situations_ids': ids,
            'living_situations_codes': codes
        }
    }))

@app.route('/api/list/infrastructures', methods=['GET'])
@app.route('/api/list/infrastructures/', methods=['GET'])
def list_infrastructures() -> Response:
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'infrastructures': [{
                'id': infra_id,
                'name': infra,
                'code': infra_code,
                'functions': [{
                    'id': city_function_id,
                    'name': city_function,
                    'code': city_function_code,
                    'service_types': [{
                        'id': service_type_id,
                        'name': service_type,
                        'code': service_type_code
                    } for _, (service_type_id, service_type, service_type_code) in \
                            infrastructure[infrastructure['city_function_id'] == city_function_id].dropna() \
                                    [['service_type_id', 'service_type', 'service_type_code']].iterrows()]
                } for _, (city_function_id, city_function, city_function_code) in \
                        infrastructure[infrastructure['infrastructure_id'] == infra_id].dropna() \
                                [['city_function_id', 'city_function', 'city_function_code']].drop_duplicates().iterrows()]
            } for _, (infra_id, infra, infra_code) in infrastructure[['infrastructure_id', 'infrastructure', 'infrastructure_code']].drop_duplicates().iterrows()]
        }
    }))

@app.route('/api/list/districts', methods=['GET'])
@app.route('/api/list/districts/', methods=['GET'])
def list_districts() -> Response:
    districts = city_hierarchy[['district_id', 'district']].dropna().drop_duplicates().sort_values('district')
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'districts': list(districts['district']),
            'districts_ids': list(districts['district_id'])
        }
    }))

@app.route('/api/list/municipalities', methods=['GET'])
@app.route('/api/list/municipalities/', methods=['GET'])
def list_municipalities() -> Response:
    municipalities = city_hierarchy[['municipality_id', 'municipality']].dropna().drop_duplicates().sort_values('municipality')
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'municipalities': list(municipalities['municipality']),
            'municipalities_ids': list(municipalities['municipality_id'])
        }
    }))

@app.route('/api/list/city_hierarchy', methods=['GET'])
@app.route('/api/list/city_hierarchy/', methods=['GET'])
def list_city_hierarchy() -> Response:
    local_hierarchy = city_hierarchy
    if 'location' in request.args:
        if request.args['location'] in city_hierarchy['district'].unique():
            local_hierarchy = local_hierarchy[local_hierarchy['district'] == request.args['location']]
        elif request.args['location'] in city_hierarchy['municipality'].unique():
            local_hierarchy = local_hierarchy[local_hierarchy['municipality'] == request.args['location']]
        elif request.args['location'].isnumeric():
            local_hierarchy = local_hierarchy[local_hierarchy['municipality'] == blocks.loc[int(request.args['location'])]['municipality']]
        else:
            return make_response(jsonify({'error': f"location '{request.args['location']}' is not found in any of districts, municipalities or blocks"}), 400)
    
    districts = [{'id': id, 'name': name, 'population': population}
            for _, (id, name, population) in
                    local_hierarchy[['district_id', 'district', 'district_population']].dropna().drop_duplicates().iterrows()]
    for district in districts:
        district['municipalities'] = [{'id': id, 'name': name, 'population': population}
                for _, (id, name, population) in
                        local_hierarchy[local_hierarchy['district_id'] == district['id']][['municipality_id', 'municipality',
                                'municipality_population']].iterrows()]
    municipalities = [{'id': id, 'name': name, 'population': population}
            for _, (id, name, population) in
                    local_hierarchy[local_hierarchy['district_id'].isna()] \
                            [['municipality_id', 'municipality', 'municipality_population']].dropna().drop_duplicates().iterrows()]
    if 'include_blocks' in request.args:
        for district in districts:
            for municipality in district['municipalities']:
                municipality['blocks'] = [{'id': id, 'population': population} for id, population in
                        blocks[blocks['municipality'] == municipality['name']]['population'].items()]
        for municipality in municipalities:
            municipality['blocks'] = [{'id': id, 'population': population} for id, population in
                    blocks[blocks['municipality'] == municipality['name']]['population'].items()]
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'districts': districts,
            'municipalities': municipalities,
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
        'version': '2021-10-27',
        '_links': {
            'self': {
                'href': request.path
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
            'relevant-service_types': {
                'href': '/api/relevance/service_types/{?social_group,living_situation}',
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
            'provision_v3_ready': {
                'href': '/api/provision_v3/ready/{?service_type,include_evaluation_scale}',
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
                'href': '/api/provision_v3/houses/{?service_type,location,everything}',
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
            'provision_v3_service_houses': {
                'href': '/api/provision_v3/service_houses/{service_id}/',
                'templated': True
            },
            'provision_v3_prosperity_districts': {
                'href': '/api/provision_v3/prosperity/districts/'
                        '{?district,municipality,block,service_type,city_function,infrastructure,social_group,provision_only}',
                'templated': True
            },
            'provision_v3_prosperity_municipalities': {
                'href': '/api/provision_v3/prosperity/municipalities/'
                        '{?district,municipality,block,service_type,city_function,infrastructure,social_group,provision_only}',
                'templated': True
            },
            'provision_v3_prosperity_blocks': {
                'href': '/api/provision_v3/prosperity/blocks/'
                        '{?district,municipality,block,service_type,city_function,infrastructure,social_group,provision_only}',
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
    with properties.conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(a.center), a.city_service_type, a.service_name, a.administrative_unit, a.municipality, a.block_id, a.address,'
                '    ps.houses_in_radius, ps.people_in_radius, ps.service_load, ps.needed_capacity, ps.reserve_resource, ps.evaluation,'
                '    ps.service_id'
                ' FROM all_services a JOIN provision.services ps ON a.functional_object_id = ps.service_id' + 
                (' WHERE a.city_service_type = %s' if 'service_type' in request.args else ''), ((service_type,) if 'service_type' in request.args else ()))
        df = pd.DataFrame(cur.fetchall(), columns=('center', 'service_type', 'service_name', 'district', 'municipality', 'block', 'address',
                'houses_in_access', 'people_in_access', 'service_load', 'needed_capacity', 'reserve_resource', 'provision', 'service_id'))
                # TODO: 'provision' -> 'evaluation
    df['center'] = df['center'].apply(json.loads)
    df['block'] = df['block'].replace({np.nan: None})
    df['address'] = df['address'].replace({np.nan: None})
    if location is not None:
        if location in city_hierarchy['district'].unique():
            df = df[df['district'] == location].drop('district', axis=1)
        elif location in city_hierarchy['municipality'].unique():
            df = df[df['municipality'] == location].drop(['district', 'municipality'], axis=1)
        else:
            location = f'Not found ({location})'
            df = df.drop(df.index)
    if 'service_type' in request.args:
        df = df.drop('service_type', axis=1)
    return make_response(jsonify({
        '_links': {
            'self': {'href': request.path},
            'service_info': {'href': '/api/provision_v3/service/{service_id}/', 'templated': True},
            'houses': {'href': '/api/provision_v3/service_houses/{service_id}/', 'templated': True}
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
    with properties.conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(a.center), a.city_service_type, a.service_name, a.administrative_unit, a.municipality, a.block_id, a.address,'
                '    v.houses_in_radius, v.people_in_radius, v.service_load, v.needed_capacity, v.reserve_resource, v.evaluation as provision'
                ' FROM all_services a'
                '   JOIN provision.services v ON a.functional_object_id = %s AND v.service_id = %s', (service_id,) * 2)
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
            service_info['provision'] = res[12] # TODO: 'provision' -> 'evaluation'
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
    service_type = get_parameter_of_request(request.args.get('service_type'), 'service_type', 'id')
    location = request.args.get('location')
    location_tuple: Optional[Tuple[Literal['district', 'municipality'], int]] = None
    if location is not None:
        if location in city_hierarchy['district'].unique():
            location_tuple = 'district', int(city_hierarchy[city_hierarchy['district'] == location]['district_id'].iloc[0])
        elif location in city_hierarchy['municipality'].unique():
            location_tuple = 'municipality', int(city_hierarchy[city_hierarchy['municipality'] == location]['municipality_id'].iloc[0])
        else:
            location = f'{location} (not found)'
    if not location_tuple and not service_type and not 'everything' in request.args:
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'houses': [],
                'parameters': {
                    'location': location,
                    'service_type': None
                },
                'error': "at least one of the 'service_type' and 'location' must be set in request. To avoid this error use ?everything parameter"
            }
        }), 400)
    with properties.conn.cursor() as cur:
        cur.execute('SELECT h.functional_object_id, h.address, ST_AsGeoJSON(h.center), h.resident_number,'
                '   h.administrative_unit, h.municipality, h.block_id FROM houses h' +
                (' WHERE' if location_tuple or service_type else '') +
                (' h.administrative_unit_id = %s ' if location_tuple and location_tuple[0] == 'district' else ' h.municipality_id = %s' \
                        if location_tuple and location_tuple[0] == 'municipality' else '') +
                ' ORDER BY 1',
                (location_tuple[1],) if location_tuple else tuple()
        )
        houses = pd.DataFrame(cur.fetchall(),
                columns=('id', 'address', 'center', 'population', 'district', 'municipality', 'block')).set_index('id') # 'service_type', 'reserve_resource', 'provision'
        houses['center'] = houses['center'].apply(lambda x: json.loads(x))
        houses.replace({np.nan: None})
        result = []
        for house_id, (address, center, population, district, municipality, block),  in houses.iterrows():
            cur.execute('SELECT st.name, reserve_resource, provision FROM provision.houses ph'
                    '   JOIN city_service_types st ON ph.city_service_type_id = st.id'
                    ' WHERE house_id = %s' +
                    ('AND st.id = %s' if service_type else ''),
                    (house_id, service_type,) if service_type else (house_id,)
            )
            service_types = [{'service_type': service_type, 'reserve_resources': reserve, 'provision': provision} \
                    for service_type, reserve, provision in cur.fetchall()]
            result.append({
                'id': house_id,
                'address': address,
                'population': population,
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
    with properties.conn.cursor() as cur:
        cur.execute('SELECT address, ST_AsGeoJSON(center), administrative_unit, municipality, block_id, resident_number FROM houses'
                ' WHERE functional_object_id = %s',
                (house_id,))
        res = cur.fetchone()
        if res is None:
            house_info['address'] = 'Not found'
            house_info['center'] = None
        else:
            house_info['address'], house_info['center'], house_info['district'], house_info['municipality'], house_info['block'], house_info['population'] = res
            house_info['center'] = json.loads(house_info['center'])
            house_info['block'] = int(house_info['block']) if house_info['block'] == house_info['block'] else None
            cur.execute('SELECT st.name as service_type, ph.reserve_resource, ph.provision FROM provision.houses ph'
                    '   JOIN city_service_types st ON ph.city_service_type_id = st.id' + 
                    ' WHERE ph.house_id  = %s' + 
                    (' AND st.name = %s' if 'service_type' in request.args and not request.args['service_type'].isnumeric() else \
                            ' AND st.id = %s' if 'service_type' in request.args else ''),
                    (house_id, request.args['service_type']) if 'service_type' in request.args else (house_id,))
            house_service_types = pd.DataFrame(cur.fetchall(),
                    columns=('service_type', 'reserve_resource', 'provision'))
    if house_info['address'] != 'Not found':
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
    service_type = get_parameter_of_request(request.args.get('service_type'), 'service_type', 'name')
    with properties.conn.cursor() as cur:
        if 'service_type' in request.args:
            cur.execute('SELECT hs.service_id, a.service_name, ST_AsGeoJSON(a.center), hs.load,'
                    '      (SELECT sum(load) FROM provision.houses_services WHERE service_id = hs.service_id) FROM provision.houses_services hs'
                    '   JOIN all_services a ON hs.service_id = a.functional_object_id'
                    ' WHERE hs.house_id = %s AND a.city_service_type = %s', (house_id, service_type))
            services = [{'id': func_id, 'name': name, 'center': json.loads(center), 'load_part': load_part, 'load_service': round(load_service, 2)} for \
                     func_id, name, center, load_part, load_service in cur.fetchall()]
        else:
            cur.execute('SELECT hs.service_id, a.service_name, ST_AsGeoJSON(a.center), a.city_service_type, hs.load,'
                    '      (SELECT sum(load) FROM provision.houses_services WHERE service_id = hs.service_id) FROM provision.houses_services hs'
                    '   JOIN all_services a ON hs.service_id = a.functional_object_id'
                    ' WHERE hs.house_id = %s', (house_id,))
            services = [{'id': func_id, 'name': name, 'center': json.loads(center), 'service_type': service_type, 'load_part': load_part,
                            'load_service': round(load_service, 2)} for func_id, name, center, service_type, load_part, load_service in cur.fetchall()]
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'parameters': {
                'service_type': service_type
            },
            'services': services
        }
    }))

@app.route('/api/provision_v3/service_houses/<int:service_id>', methods=['GET'])
@app.route('/api/provision_v3/service_houses/<int:service_id>/', methods=['GET'])
def service_houses(service_id: int) -> Response:
    with properties.conn.cursor() as cur:
        cur.execute('SELECT normative FROM provision.normatives'
                ' WHERE city_service_type_id = (SELECT city_service_type_id FROM all_services WHERE functional_object_id = %s)', (service_id,))
        res = cur.fetchone()
        if res is None:
            normative = 0
        else:
            normative = res[0]
        cur.execute('SELECT hs.house_id, h.resident_number, ST_AsGeoJSON(h.center), hs.load FROM provision.houses_services hs'
                '   JOIN houses h ON hs.house_id = h.functional_object_id'
                ' WHERE hs.service_id = %s', (service_id,))
        houses = [{'id': func_id, 'population': population, 'center': json.loads(center), 'load_part': load_part,
                        'load_house': round(population * normative / 1000, 2)} for func_id, population, center, load_part in cur.fetchall()]
    return make_response(jsonify({
        '_links': {'self': {'href': request.path}},
        '_embedded': {
            'houses': houses
        }
    }))

@app.route('/api/provision_v3/ready', methods=['GET'])
@app.route('/api/provision_v3/ready/', methods=['GET'])
def provision_v3_ready() -> Response:
    with properties.conn.cursor() as cur:
        cur.execute('SELECT c.city_service_type, c.count, n.normative, n.max_load, n.radius_meters, '
                '   n.public_transport_time, n.service_evaluation, n.house_evaluation'
                ' FROM (SELECT a.city_service_type_id, a.city_service_type, count(*) FROM all_services a'
                '           JOIN provision.services ps ON a.functional_object_id = ps.service_id'
                '       WHERE city = %s'
                '       GROUP BY a.city_service_type_id, a.city_service_type) as c'
                '   LEFT JOIN provision.normatives n ON n.city_service_type_id = c.city_service_type_id',
                (city_name,))
        df = pd.DataFrame(cur.fetchall(), columns=('service_type', 'count', 'normative', 'max_load', 'radius_meters',
                'public_transport_time', 'service_evaluation', 'house_evaluation'))
        if not 'include_evaluation_scale' in request.args:
            df = df.drop(['service_evaluation', 'house_evaluation'], axis=1)
        if 'service_type' in request.args:
            df = df[df['service_type'] == get_parameter_of_request(request.args['service_type'], 'service_type', 'name')]
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'service_types': list(df.replace({np.nan: None}).transpose().to_dict().values())
            }
        }))

@app.route('/api/provision_v3/not_ready', methods=['GET'])
@app.route('/api/provision_v3/not_ready/', methods=['GET'])
def provision_v3_not_ready() -> Response:
    with properties.conn.cursor() as cur:
        cur.execute('SELECT st.name as service_type, s.count AS unevaluated, c.count AS total'
                ' FROM (SELECT city_service_type_id, count(*) FROM all_services WHERE functional_object_id NOT IN'
                '       (SELECT service_id FROM provision.services) AND city = %s'
                '    GROUP BY city_service_type_id ORDER BY 1) AS s'
                ' JOIN city_service_types st ON s.city_service_type_id = st.id'
                ' JOIN (SELECT city_service_type_id, count(*) FROM all_services WHERE city = %s'
                '       GROUP BY city_service_type_id) AS c ON c.city_service_type_id = st.id'
                ' ORDER BY 1',
                (city_name,) * 2)
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'service_types': [{'service_type': service_type, 'unevaluated': unevaluated, 'total_count': total} for \
                        service_type, unevaluated, total in cur.fetchall()]
            }
        }))

@app.route('/api/provision_v3/prosperity/<location_type>', methods=['GET'])
@app.route('/api/provision_v3/prosperity/<location_type>/', methods=['GET'])
def provision_v3_prosperity(location_type: str) -> Response:
    if location_type not in ('districts', 'municipalities', 'blocks'):
        return make_response(jsonify({
            '_links': {'self': {'href': request.path}},
            '_embedded': {
                'error': f"location_type must be 'blocks', 'districts' or 'municipalities', but '{location_type}' is given"
            }
        }), 400)
    social_group: str = request.args.get('social_group', 'all')
    if social_group and social_group not in ('all', 'mean'):
        social_group = get_parameter_of_request(social_group, 'social_group', 'name') # type: ignore
    service_type: Optional[str] = request.args.get('service_type')
    if service_type and service_type not in ('all', 'mean'):
        service_type = get_parameter_of_request(service_type, 'service_type', 'name') # type: ignore
    city_function: Optional[str] = request.args.get('city_function')
    if city_function and city_function not in ('all', 'mean'):
        city_function = get_parameter_of_request(city_function, 'city_function', 'name') # type: ignore
    infra: Optional[str] = request.args.get('infrastructure')
    if infra and infra not in ('all', 'mean'):
        infra = get_parameter_of_request(infra, 'infrastructure', 'name') # type: ignore
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
        aggregation_type = 'city_function'
        aggregation_value = city_function
    else:
        aggregation_type = 'infrastructure'
        aggregation_value = infra

    district: Optional[str] = request.args.get('district', 'all')
    if district:
        if district.isnumeric():
            district = city_hierarchy[city_hierarchy['district_id'] == int(district)]['district'].iloc[0] \
                    if int(district) in city_hierarchy['district_id'] else 'None'
    municipality: Optional[str] = request.args.get('municipality', 'all')
    if municipality:
        if municipality.isnumeric():
            municipality = city_hierarchy[city_hierarchy['municipality_id'] == int(municipality)]['municipality'].iloc[0] \
                    if int(municipality) in city_hierarchy['municipality_id'] else 'None'
    block: Optional[Union[str, int]] = request.args.get('block', 'all')
    if block and block != 'all':
        if block.isnumeric(): # type: ignore
            block = int(block)
        elif block != 'mean':
            block = 'all'

    location_type_single = 'district' if location_type == 'districts' else 'municipality' if location_type == 'municipalities' else 'block'

    if location_type == 'districts':
        res = provision_administrative_units
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
            res = res[res['municipality'].isin(city_hierarchy[city_hierarchy['district'] == district]['municipality'])]
    else:
        if block and block not in ('all', 'mean'):
            res = res[res['district'] == blocks[blocks['district'].loc[block]]]
        elif municipality and municipality not in ('all', 'mean'):
            res = res[res['district'] == city_hierarchy[city_hierarchy['municipality'] == municipality]['district'].iloc[0]]
        elif district and district not in ('all', 'mean'):
            res = res[res['district'] == district]
    
    if not provision_only:
        n: pd.DataFrame = needs[['service_type', 'social_group', 'significance']].merge(
                infrastructure[['service_type', 'city_function', 'infrastructure']], how='inner', on='service_type') \
                        [[aggregation_type, 'social_group', 'significance']]
        n = n.groupby([aggregation_type, 'social_group']).mean().reset_index()
        if social_group not in ('all', 'mean'):
            n = n[n['social_group'] == social_group][[aggregation_type, 'significance']]

        res = res.merge(infrastructure[['service_type', 'city_function', 'infrastructure']], how='inner', on='service_type') \
                [[location_type_single, aggregation_type, 'houses_count', 'services_count', 'services_load_mean', 'services_load_sum',
                        'houses_reserve_mean', 'houses_reserve_sum', 'services_reserve_mean', 'services_reserve_sum', 'houses_provision', 'services_evaluation']]
        res = res.merge(n, how='inner', on=aggregation_type)
    else:
        res = res.merge(infrastructure[['service_type', 'city_function', 'infrastructure']], how='inner', on='service_type') \
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
        res = res.drop(aggregation_labels, axis=1)
        aggr = set(res.columns) - {'houses_provision', 'services_evaluation', 'significance', 'houses_count', 'services_count',
                'services_load_mean', 'services_load_sum', 'houses_reserve_mean', 'houses_reserve_sum', 'services_reserve_mean',
                'services_reserve_sum'} - set(aggregation_labels)
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
    properties.conn.rollback()
    log.error(f'Error at {request.path}?{"&".join(map(lambda x: f"{x[0]}={x[1]}", request.args.items()))} - {repr(error)}')
    log.warning('\n'.join(traceback.format_tb(error.__traceback__)))
    return make_response(jsonify({
        'error': str(error),
        'error_type': str(type(error)),
        'path': request.path,
        'params': '&'.join(map(lambda x: f'{x[0]}={x[1]}', request.args.items())),
        'trace': list(itertools.chain.from_iterable(map(lambda x: x.split('\n'), traceback.format_tb(error.__traceback__))))
    }), 500)


if __name__ == '__main__':

    # Default properties settings

    properties = Properties('localhost', 5432, 'city_db_final', 'postgres', 'postgres', 8080)

    # Environment variables

    if 'PROVISION_API_PORT' in os.environ:
        properties.api_port = int(os.environ['PROVISION_API_PORT'])
    if 'PROVISION_CITY_NAME' in os.environ:
        city_name = os.environ['PROVISION_CITY_NAME']
    if 'HOUSES_DB_ADDR' in os.environ:
        properties.db_addr = os.environ['HOUSES_DB_ADDR']
    if 'HOUSES_DB_NAME' in os.environ:
        properties.db_name = os.environ['HOUSES_DB_NAME']
    if 'HOUSES_DB_PORT' in os.environ:
        properties.db_port = int(os.environ['HOUSES_DB_PORT'])
    if 'HOUSES_DB_USER' in os.environ:
        properties.db_user = os.environ['HOUSES_DB_USER']
    if 'HOUSES_DB_PASS' in os.environ:
        properties.db_pass = os.environ['HOUSES_DB_PASS']

    # CLI Arguments

    parser = argparse.ArgumentParser(description='Starts up the provision API server')
    parser.add_argument('-hH', '--houses_db_addr', action='store', dest='houses_db_addr',
                        help=f'postgres host address [default: {properties.db_addr}]', type=str)
    parser.add_argument('-hP', '--houses_db_port', action='store', dest='houses_db_port',
                        help=f'postgres port number [default: {properties.db_port}]', type=int)
    parser.add_argument('-hd', '--houses_db_name', action='store', dest='houses_db_name',
                        help=f'postgres database name [default: {properties.db_name}]', type=str)
    parser.add_argument('-hU', '--houses_db_user', action='store', dest='houses_db_user',
                        help=f'postgres user name [default: {properties.db_user}]', type=str)
    parser.add_argument('-hW', '--houses_db_pass', action='store', dest='houses_db_pass',
                        help=f'database user password [default: {properties.db_pass}]', type=str)
    parser.add_argument('-c', '--city_name', action='store', dest='city_name',
                        help=f'city name [default: {city_name}]', type=str)
    parser.add_argument('-p', '--port', action='store', dest='api_port',
                        help=f'postgres port number [default: {properties.api_port}]', type=int)
    parser.add_argument('-D', '--debug', action='store_true', dest='debug', help=f'debug trigger')
    args = parser.parse_args()

    if args.houses_db_addr is not None:
        properties.db_addr = args.houses_db_addr
    if args.houses_db_port is not None:
        properties.db_port = args.houses_db_port
    if args.houses_db_name is not None:
        properties.db_name = args.houses_db_name
    if args.houses_db_user is not None:
        properties.db_user = args.houses_db_user
    if args.houses_db_pass is not None:
        properties.db_pass = args.houses_db_pass
    if args.city_name is not None:
        city_name = args.city_name
    if args.api_port is not None:
        properties.api_port = args.api_port

    logging.basicConfig(handlers = (logging.StreamHandler(),), level=0, format='%(message)s')
    log.setLevel(0)
    
    log.info('Getting global data')

    update_global_data()

    log.info(f'Starting application on 0.0.0.0:{properties.api_port} with houses DB as'
            f' ({properties.db_user}@{properties.db_addr}:{properties.db_port}/{properties.db_name}).')

    if args.debug:
        app.run(host='0.0.0.0', port=properties.api_port, debug=args.debug)
    else:
        import gevent.pywsgi

        app_server = gevent.pywsgi.WSGIServer(('0.0.0.0', properties.api_port), app)
        try:
            app_server.serve_forever()
        except KeyboardInterrupt:
            log.info('Finishing')
            app_server.stop()
