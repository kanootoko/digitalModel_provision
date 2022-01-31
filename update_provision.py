import argparse
import itertools
import logging
import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import psycopg2

log = logging.getLogger(__name__)

sys.path.append(os.path.abspath('../libs'))

# from ..libs import collect_geometry # type: ignore
import collect_geometry  # type: ignore

capacity_people = {
    0: 0,
    1: 50,
    2: 150,
    3: 300,
    4: 600,
    5: 1000,
    6: 3000,
    7: 6000,
    8: 10000,
    9: 15000,
    10: 20000
}

class Properties:
    def __init__(self, db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str):
        self.db_addr = db_addr
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self._conn = None

    @property
    def conn_string(self) -> str:
        return f'host={self.db_addr} port={self.db_port} dbname={self.db_name}' \
                f' user={self.db_user} password={self.db_pass}'
    @property
    def conn(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.conn_string)
        return self._conn

properties: Properties
properties_geometry: Properties

# generate

def generate_table_2(conn: psycopg2.extensions.connection, geometry_conn: psycopg2.extensions.connection, service_type: str,
        normative: Dict[str, Any], log_n: int = 50, wait_for_transport_service: bool = True,
        public_transport_service_endpoint: str = 'http://10.32.1.61:8080/api.v2/isochrones') -> pd.DataFrame:
    services: gpd.GeoDataFrame = gpd.GeoDataFrame.from_postgis('SELECT functional_object_id as func_id,'
            ' center, administrative_unit_id as district, municipality_id as municipality, block_id as block, city_service_type, capacity FROM all_services'
            f" WHERE city_service_type = '{service_type}'"
            ' ORDER BY city_service_type, district, municipality, block_id, address', conn, 'center')
    frame: List[Tuple[int, str, str, int, str, int, Optional[int], Optional[int], int, int]] = []
    with conn, conn.cursor() as cur:
        start = time.time()
        last_time = start
        i = 1
        for _, service in services.iterrows():
            if i % log_n == 0:
                t = time.localtime()
                delta = max(int(time.time() - start), 1)
                delta_1 = max(int(time.time() - last_time), 1)
                todo = int((services.shape[0] - i) // max((i / delta), 1))
                todo_1 = int((services.shape[0] - i) // (log_n / delta_1))
                log.debug(f'{t.tm_hour:02}:{t.tm_min:02}:{t.tm_sec:02} - service {i:05} of {services.shape[0]:05}.'
                        f' {delta // 60:02}m:{delta % 60:02}s / {delta_1 // 60:02}m:{delta_1 % 60:02}s passed,'
                        f' {todo // 60:02}m:{todo % 60:02}s / {todo_1 // 60:02}m:{todo_1 % 60:02}s to finish')
                last_time = time.time()
            i += 1
            if normative['public_transport_time']:
                while True:
                    try:
                        transport_polygon = json.dumps(collect_geometry.get_public_transport(service['center'].x, service['center'].y,
                                normative['public_transport_time'], geometry_conn, public_transport_service_endpoint, 300,
                                wait_for_transport_service))
                        break
                    except TimeoutError:
                        log.error(f'Timed out while trying to fetch transport for ({service["center"].x}, {service["center"].y})'
                                f' and time={normative["public_transport_time"]}, trying again in 20s')
                        time.sleep(20)
            center_radius = (service['center'].x, service['center'].y, normative['radius_meters'])
            cur.execute('SELECT functional_object_id, administrative_unit_id as district, municipality_id, block_id, resident_number'
                    ' FROM houses WHERE ' +
                    ('ST_Within(center, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))' if normative['public_transport_time'] else \
                        'ST_Within(center, ST_Buffer(ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)::geometry)') +
                    ' ORDER BY district, municipality, block_id',
                    ((transport_polygon,) if normative['public_transport_time'] else center_radius))
            houses = pd.DataFrame(cur.fetchall(), columns=('func_id', 'district', 'municipality', 'block', 'population'))
            frame.append((service['func_id'], service['district'], service['municipality'], service['block'],
                    service['city_service_type'], service['capacity'], normative['radius_meters'],
                    normative['public_transport_time'], houses.shape[0], houses['population'].sum()))
    return pd.DataFrame(frame,
            columns=('func_id', 'district', 'municipality', 'block', 'service_type', 'capacity', 'radius',
                    'transport', 'houses_available', 'population_available')).set_index('func_id')

def generate_table_1_3(conn: psycopg2.extensions.connection, geometry_conn: psycopg2.extensions.connection,
        service_type: str, normative: Dict[str, Any], log_n: int = 50, wait_for_transport_service: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    services: gpd.GeoDataFrame = gpd.GeoDataFrame.from_postgis('SELECT functional_object_id as func_id,'
            ' center, administrative_unit_id as district, municipality_id, block_id as block, city_service_type, capacity FROM all_services'
            f" WHERE city_service_type = '{service_type}'"
            ' ORDER BY city_service_type, district, municipality, block_id, address', conn, 'center')
    services = services.reindex()
    frame: List[Tuple[int, str, str, int, int, int, Optional[int], Optional[int]]] = []
    with conn, conn.cursor() as cur:
        cur.execute('SELECT functional_object_id, administrative_unit_id, municipality_id, block_id, resident_number'
                ' FROM houses ORDER BY 1')
        table_1 = pd.DataFrame(cur.fetchall(), columns=('house_id', 'district', 'municipality', 'block', 'population')).set_index('house_id')

        provision_base = dict((house_id, 0.0) for house_id in table_1.index)
        provision = provision_base.copy()
        provision_capcacity = provision_base
        start = time.time()
        last_time = start
        i = 0
        for _, service in services.iterrows():
            if i % log_n == 0:
                t = time.localtime()
                delta = max(int(time.time() - start), 1)
                delta_1 = max(int(time.time() - last_time), 1)
                todo = int((services.shape[0] - i) // max((i / delta), 1))
                todo_1 = int((services.shape[0] - i) // (log_n / delta_1))
                log.debug(f'{t.tm_hour:02}:{t.tm_min:02}:{t.tm_sec:02} - service {i:05} of {services.shape[0]:05}.'
                        f' {delta // 60:02}m:{delta % 60:02}s / {delta_1 // 60:02}m:{delta_1 % 60:02}s passed,'
                        f' {todo // 60:02}m:{todo % 60:02}s / {todo_1 // 60:02}m:{todo_1 % 60:02}s to finish')
                last_time = time.time()
            i += 1
            center_radius = (service['center'].x, service['center'].y, normative['radius_meters'])
            if normative['public_transport_time']:
                while True:
                    try:
                        transport_polygon = json.dumps(collect_geometry.get_public_transport(service['center'].x, service['center'].y,
                                normative['public_transport_time'], geometry_conn, 'http://10.32.1.61:8080/api.v2/isochrones', 300,
                                wait_for_transport_service))
                        break
                    except TimeoutError:
                        print(f'Timed out while trying to fetch transport for ({service["center"].x}, {service["center"].y})'
                                f' and time={normative["public_transport_time"]}, trying again')
            cur.execute('SELECT functional_object_id, administrative_unit_id as district,'
                    '   municipality_id as municipality_name, block_id, resident_number FROM houses'
                    ' WHERE ' +
                    ('ST_Within(center, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))' if normative['public_transport_time'] else
                        'ST_Within(center, ST_Buffer(ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)::geometry)'),
                    ((transport_polygon,) if normative['public_transport_time'] else center_radius))
            houses = cur.fetchall()
            for house_id, district, municipality, block, population in houses:
                provision[house_id] += 1 / len(houses)
                provision_capcacity[house_id] += (capacity_people.get(service['capacity']) or 0) / len(houses)
                frame.append((house_id, district, municipality, block, population, service['func_id'],
                        normative['radius_meters'], normative['public_transport_time']))
            if len(frame) == 0:
                frame.append((-1, '---------------', '---------------', -1, 0, service['func_id'],
                        normative['radius_meters'], normative['public_transport_time']))
    table_3 = pd.DataFrame(frame, columns=('house_id', 'district', 'municipality', 'block', 'population',
            'func_id', 'radius', 'transport')).set_index('house_id')
    table_1 = table_1.join(pd.Series(provision.values(), index=provision.keys(), name=service_type +
            (f' ({normative["public_transport_time"]} минут)' if normative["public_transport_time"] else f' ({normative["radius_meters"]} метров)')))
    table_1 = table_1.join(pd.Series(provision_capcacity.values(), index=provision_capcacity.keys(), name=f'{service_type}_capacity' +
            (f' ({normative["public_transport_time"]} минут)' if normative["public_transport_time"] else f' ({normative["radius_meters"]} метров)')))
    return table_1, table_3

# process

def process_tables(table1: pd.DataFrame, table2: pd.DataFrame, table3: pd.DataFrame, normative: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    houses = table1[['district', 'municipality', 'block', 'population']].copy()

    services = table2
    houses_services = table3

    house_counts = houses_services.index.value_counts().to_dict()

    houses_services['Нагрузка'] = houses_services['population'] / pd.Series([house_counts[house_id] for house_id in houses_services.index], index=houses_services.index)
    services = services.join(pd.Series([houses_services[houses_services['func_id'] == func_id]['Нагрузка'].sum() \
                for func_id in sorted(houses_services['func_id'].unique())],
            index=sorted(houses_services['func_id'].unique()), name='Суммарная нагрузка'), on='func_id')
    services['Суммарная нагрузка'] = services['Суммарная нагрузка'].fillna(0)
    services['Нормативная емкость'] = (services['Суммарная нагрузка'] * normative['normative'] / 1000).apply(lambda x: round(x, 2))
    services['Запас по количеству'] = (normative['max_load'] - services['Нормативная емкость'])

    def get_coeff_service(x):
        i = 0
        while i < len(normative['service_evaluation']) and x > normative['service_evaluation'][i]:
            i += 1
        return i

    services['Коэффициент'] = services[f'Запас по количеству'].apply(get_coeff_service)

    houses_services_limited = houses_services[houses_services.index != -1]

    houses['reserve_resource'] = -houses['population'] * normative['normative'] / 1000
    houses[service_type] = 0.0

    i = 1
    for house_id in houses_services_limited.index.unique():
        if i % 1000 == 0:
            log.debug(f'processing {i} of {houses_services_limited.index.nunique()}')
        i += 1
        servs_house = houses_services_limited.loc[house_id]
        if isinstance(servs_house, pd.Series):
            servs_house = pd.DataFrame(servs_house).transpose()
        servs_both = servs_house.merge(services.loc[servs_house['func_id']], left_on='func_id', right_index=True, suffixes = (None, '__right'))
        servs_both = servs_both.drop(list(filter(lambda x: x.endswith('__right'), servs_both.columns)), axis=True)
        servs_both['ratio'] = servs_both['Нагрузка'] / servs_both['Суммарная нагрузка']
        houses.loc[int(house_id), 'reserve_resource'] = (servs_both['Запас по количеству'] * servs_both['ratio']).sum()
    houses[service_type] = 1.0 - houses['reserve_resource'].apply(lambda x: max(-x, 0.0)) * 1000 / houses['population'] / normative['normative']
    houses[service_type] = houses[service_type].apply(lambda x: max(x, 0.0))
    houses['reserve_resource'] = houses['reserve_resource'].apply(lambda x: round(x, 2))

    def get_coeff_house(x):
        i = 0
        while i < len(normative['house_evaluation']) and x > normative['house_evaluation'][i]:
            i += 1
        return i

    houses['coefficient'] = houses['reserve_resource'].apply(get_coeff_house)

    return houses, services, houses_services

# insert results

def ensure_tables(conn: psycopg2.extensions.connection):
    with conn, conn.cursor() as cur:
        cur.execute('CREATE SCHEMA IF NOT EXISTS provision')

        cur.execute('CREATE TABlE IF NOT EXISTS provision.normatives ('
                'city_service_type_id integer PRIMARY KEY REFERENCES city_service_types(id) NOT NULL,'
                'normative float NOT NULL,'
                'max_load integer NOT NULL,'
                'radius_meters integer,'
                'public_transport_time integer,'
                'service_evaluation jsonb,'
                'house_evaluation jsonb,'
                'last_calculations TIMESTAMPTZ'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.services ('
                '  service_id int REFERENCES functional_objects(id) PRIMARY KEY NOT NULL,'
                '  houses_in_radius int NOT NULL,'
                '  people_in_radius int NOT NULL,'
                '  service_load int NOT NULL,'
                '  needed_capacity int NOT NULL,'
                '  reserve_resource int NOT NULL,'
                '  evaluation int NOT NULL'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.services_administrative_units ('
                '  administrative_unit_id int REFERENCES administrative_units(id) NOT NULL,'
                '  city_service_type_id int REFERENCES city_service_types(id) NOT NULL,'
                '  count int NOT NULL,'
                '  service_load_min int NOT NULL,'
                '  service_load_mean float NOT NULL,'
                '  service_load_max int NOT NULL,'
                '  service_load_sum int NOT NULL,'
                '  reserve_resources_min int NOT NULL,'
                '  reserve_resources_mean float NOT NULL,'
                '  reserve_resources_max int NOT NULL,'
                '  reserve_resources_sum int NOT NULL,'
                '  evaluation_min int NOT NULL,'
                '  evaluation_mean float NOT NULL,'
                '  evaluation_max int NOT NULL,'
                '  PRIMARY KEY(administrative_unit_id, city_service_type_id)'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.services_municipalities ('
                '  municipality_id int REFERENCES municipalities(id) NOT NULL,'
                '  city_service_type_id int REFERENCES city_service_types(id) NOT NULL,'
                '  count int NOT NULL,'
                '  service_load_min int NOT NULL,'
                '  service_load_mean float NOT NULL,'
                '  service_load_max int NOT NULL,'
                '  service_load_sum int NOT NULL,'
                '  reserve_resources_min int NOT NULL,'
                '  reserve_resources_mean float NOT NULL,'
                '  reserve_resources_max int NOT NULL,'
                '  reserve_resources_sum int NOT NULL,'
                '  evaluation_min int NOT NULL,'
                '  evaluation_mean float NOT NULL,'
                '  evaluation_max int NOT NULL,'
                '  PRIMARY KEY(municipality_id, city_service_type_id)'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.services_blocks ('
                '  block_id int REFERENCES blocks(id) NOT NULL,'
                '  city_service_type_id int REFERENCES city_service_types(id) NOT NULL,'
                '  count int NOT NULL,'
                '  service_load_min int NOT NULL,'
                '  service_load_mean float NOT NULL,'
                '  service_load_max int NOT NULL,'
                '  service_load_sum int NOT NULL,'
                '  reserve_resources_min int NOT NULL,'
                '  reserve_resources_mean float NOT NULL,'
                '  reserve_resources_max int NOT NULL,'
                '  reserve_resources_sum int NOT NULL,'
                '  evaluation_min int NOT NULL,'
                '  evaluation_mean float NOT NULL,'
                '  evaluation_max int NOT NULL,'
                '  PRIMARY KEY(block_id, city_service_type_id)'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.houses ('
                '  house_id int REFERENCES functional_objects(id) NOT NULL,'
                '  city_service_type_id int REFERENCES city_service_types(id) NOT NULL,'
                '  reserve_resource int NOT NULL,'
                '  provision int NOT NULL,'
                '  PRIMARY KEY(house_id, city_service_type_id)'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.houses_administrative_units ('
                '  administrative_unit_id int REFERENCES administrative_units(id) NOT NULL,'
                '  city_service_type_id int REFERENCES city_service_types(id) NOT NULL,'
                '  count int NOT NULL,'
                '  reserve_resources_min int NOT NULL,'
                '  reserve_resources_mean float NOT NULL,'
                '  reserve_resources_max int NOT NULL,'
                '  reserve_resources_sum int NOT NULL,'
                '  provision_min int NOT NULL,'
                '  provision_mean float NOT NULL,'
                '  provision_max int NOT NULL,'
                '  PRIMARY KEY(administrative_unit_id, city_service_type_id)'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.houses_municipalities ('
                '  municipality_id int REFERENCES municipalities(id) NOT NULL,'
                '  city_service_type_id int REFERENCES city_service_types(id) NOT NULL,'
                '  count int NOT NULL,'
                '  reserve_resources_min int NOT NULL,'
                '  reserve_resources_mean float NOT NULL,'
                '  reserve_resources_max int NOT NULL,'
                '  reserve_resources_sum int NOT NULL,'
                '  provision_min int NOT NULL,'
                '  provision_mean float NOT NULL,'
                '  provision_max int NOT NULL,'
                '  PRIMARY KEY(municipality_id, city_service_type_id)'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.houses_blocks ('
                '  block_id int REFERENCES blocks(id) NOT NULL,'
                '  city_service_type_id int REFERENCES city_service_types(id) NOT NULL,'
                '  count int NOT NULL,'
                '  reserve_resources_min int NOT NULL,'
                '  reserve_resources_mean float NOT NULL,'
                '  reserve_resources_max int NOT NULL,'
                '  reserve_resources_sum int NOT NULL,'
                '  provision_min int NOT NULL,'
                '  provision_mean float NOT NULL,'
                '  provision_max int NOT NULL,'
                '  PRIMARY KEY(block_id, city_service_type_id)'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS provision.houses_services ('
                '  house_id int REFERENCES functional_objects(id) NOT NULL,'
                '  service_id int REFERENCES functional_objects(id) NOT NULL,'
                '  load float NOT NULL,'
                '  PRIMARY KEY(house_id, service_id)'
                ')'
        )

        cur.execute('CREATE INDEX IF NOT EXISTS houses_services_houses_index ON provision.houses_services(house_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS houses_services_services_index ON provision.houses_services(service_id)')

def insert_results(conn: psycopg2.extensions.connection, table_1: pd.DataFrame, table_2: pd.DataFrame, table_3: pd.DataFrame,
        service_type: str, normative: Dict[str, Any]) -> None:
    with conn, conn.cursor() as cur:
        # houses provision
        cur.execute('SELECT id FROM city_service_types WHERE name = %s', (service_type,))
        res = cur.fetchone()
        service_type_id = res[0]
        log.debug(f'houses_provision: Inserting data for service_type "{service_type}" - {table_1.shape[0]} houses')
        for house_id, (reserve_resource, provision) in table_1[['reserve_resource', 'coefficient']].iterrows():
            try:
                cur.execute('INSERT INTO provision.houses (house_id, city_service_type_id, reserve_resource, provision) VALUES (%s, %s, %s, %s)'
                        ' ON CONFLICT (house_id, city_service_type_id) DO UPDATE SET reserve_resource = excluded.reserve_resource, provision = excluded.provision',
                        (house_id, service_type_id, reserve_resource, provision))
            except Exception:
                log.error(f'Error on insertion of house provision: {house_id}, {reserve_resource}, {provision}')
                raise

        if table_1['district'].nunique() > 0:
            resource_describe = table_1.groupby('district')['reserve_resource'].describe()
            provision_describe = table_1.groupby('district')['coefficient'].describe()

            for district in resource_describe.index:
                if district is None:
                    continue
                resource = resource_describe.loc[district]
                provision = provision_describe.loc[district]
                cur.execute('INSERT INTO provision.houses_administrative_units (administrative_unit_id, city_service_type_id, count, reserve_resources_min, reserve_resources_mean,'
                        ' reserve_resources_max, reserve_resources_sum, provision_min, provision_mean, provision_max)'
                        ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        ' ON CONFLICT (administrative_unit_id, city_service_type_id) DO UPDATE SET reserve_resources_min=excluded.reserve_resources_min,'
                        '    reserve_resources_mean=excluded.reserve_resources_mean, reserve_resources_max=excluded.reserve_resources_max,'
                        '    reserve_resources_sum=excluded.reserve_resources_sum, provision_mean=excluded.provision_mean,'
                        '    provision_min=excluded.provision_min, provision_max=excluded.provision_max',
                        (district, service_type_id, resource['count'], resource['min'], round(resource['mean'], 2), resource['max'],
                            resource['count'] * resource['mean'], provision['min'], round(provision['mean'], 2), provision['max']))


        if table_1['municipality'].nunique() > 0:
            resource_describe = table_1.groupby('municipality')['reserve_resource'].describe()
            provision_describe = table_1.groupby('municipality')['coefficient'].describe()

            for municipality in resource_describe.index:
                resource = resource_describe.loc[municipality]
                provision = provision_describe.loc[municipality]
                cur.execute('INSERT INTO provision.houses_municipalities (municipality_id, city_service_type_id, count, reserve_resources_min, reserve_resources_mean,'
                        ' reserve_resources_max, reserve_resources_sum, provision_min, provision_mean, provision_max)'
                        ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        ' ON CONFLICT (municipality_id, city_service_type_id) DO UPDATE SET reserve_resources_min = excluded.reserve_resources_min,'
                        '    reserve_resources_mean = excluded.reserve_resources_mean, reserve_resources_max = excluded.reserve_resources_max,'
                        '    reserve_resources_sum = excluded.reserve_resources_sum, provision_mean = excluded.provision_mean,'
                        '    provision_min = excluded.provision_min, provision_max = excluded.provision_max',
                        (municipality, service_type_id, resource['count'], resource['min'], round(resource['mean'], 2), resource['max'],
                            resource['count'] * resource['mean'], provision['min'], round(provision['mean'], 2), provision['max']))

        if table_1['block'].nunique() > 0:
            resource_describe = table_1.groupby('block')['reserve_resource'].describe()
            provision_describe = table_1.groupby('block')['coefficient'].describe()

            for block in resource_describe.index:
                resource = resource_describe.loc[block]
                provision = provision_describe.loc[block]
                cur.execute('INSERT INTO provision.houses_blocks (block_id, city_service_type_id, count, reserve_resources_min, reserve_resources_mean,'
                        ' reserve_resources_max, reserve_resources_sum, provision_min, provision_mean, provision_max)'
                        ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        ' ON CONFLICT (block_id, city_service_type_id) DO UPDATE SET reserve_resources_min = excluded.reserve_resources_min,'
                        '    reserve_resources_mean = excluded.reserve_resources_mean, reserve_resources_max = excluded.reserve_resources_max,'
                        '    reserve_resources_sum = excluded.reserve_resources_sum, provision_mean = excluded.provision_mean,'
                        '    provision_min = excluded.provision_min, provision_max = excluded.provision_max',
                        (block, service_type_id, resource['count'], resource['min'], round(resource['mean'], 2), resource['max'],
                            resource['count'] * resource['mean'], provision['min'], round(provision['mean'], 2), provision['max']))

        # services evaluation
        services = table_2
        log.debug(f'services evaluation: inserting "{service_type}" - {services.shape[0]} items')
        services['diff'] = services['Запас по количеству']
        services = services.loc[services.index.notna()]
        coeff_name = next(filter(lambda name: name.lower().startswith('коэфф'), services.columns))

        for func_id, service in services.iterrows():
            try:
                cur.execute('INSERT INTO provision.services (service_id, houses_in_radius, people_in_radius, service_load, needed_capacity,'
                        ' reserve_resource, evaluation) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (service_id) DO UPDATE SET'
                        '    houses_in_radius = excluded.houses_in_radius, people_in_radius = excluded.people_in_radius,'
                        '    service_load = excluded.service_load, needed_capacity = excluded.needed_capacity,'
                        '    reserve_resource = excluded.reserve_resource, evaluation = excluded.evaluation',
                        (func_id, service['houses_available'], service['population_available'],
                        int(service['Суммарная нагрузка']), int(service['Нормативная емкость']), service['diff'], service[coeff_name])
                )
            except Exception:
                log.error(f'Failed at service evaluation insertion: {service}')
                raise
        
        if services['district'].nunique() > 0:
            load_describe = services.groupby('district')['Нормативная емкость'].describe()
            diff_describe = services.groupby('district')['diff'].describe()
            evaluation_describe = services.groupby('district')[coeff_name].describe()

            for district in load_describe.index:
                load = load_describe.loc[district]
                diff = diff_describe.loc[district]
                evaluation = evaluation_describe.loc[district]
                cur.execute('INSERT INTO provision.services_administrative_units (administrative_unit_id, city_service_type_id, count,'
                        '    service_load_min, service_load_mean, service_load_max, service_load_sum, reserve_resources_min,'
                        '    reserve_resources_mean, reserve_resources_max, reserve_resources_sum, evaluation_min, evaluation_mean,'
                        '    evaluation_max)'
                        '  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        '  ON CONFLICT (administrative_unit_id, city_service_type_id) DO UPDATE SET count = excluded.count,'
                        '    service_load_min = excluded.service_load_min, service_load_mean = excluded.service_load_mean,'
                        '    service_load_max = excluded.service_load_max, service_load_sum = excluded.service_load_sum,'
                        '    reserve_resources_min = excluded.reserve_resources_min, reserve_resources_mean = excluded.reserve_resources_mean,'
                        '    reserve_resources_max = excluded.reserve_resources_max, reserve_resources_sum = excluded.reserve_resources_sum,'
                        '    evaluation_min = excluded.evaluation_min, evaluation_mean = excluded.evaluation_mean,'
                        '    evaluation_max = excluded.evaluation_max',
                        (district, service_type_id, load['count'], load['min'], round(load['mean'], 2), load['max'], load['count'] * load['mean'],
                            diff['min'], round(diff['mean']), diff['max'], diff['count'] * diff['mean'], evaluation['min'], round(evaluation['mean'], 2), evaluation['max']))
        
        if services['municipality'].nunique() > 0:
            load_describe = services.groupby('municipality')['Нормативная емкость'].describe()
            diff_describe = services.groupby('municipality')['diff'].describe()
            evaluation_describe = services.groupby('municipality')[coeff_name].describe()
        
            for municipality in load_describe.index:
                load = load_describe.loc[municipality]
                diff = diff_describe.loc[municipality]
                evaluation = evaluation_describe.loc[municipality]
                try:
                    cur.execute('INSERT INTO provision.services_municipalities (municipality_id, city_service_type_id, count,'
                            '    service_load_min, service_load_mean, service_load_max, service_load_sum, reserve_resources_min,'
                            '    reserve_resources_mean, reserve_resources_max, reserve_resources_sum, evaluation_min, evaluation_mean,'
                            '    evaluation_max)'
                            '  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                            '  ON CONFLICT (municipality_id, city_service_type_id) DO UPDATE SET count = excluded.count,'
                            '    service_load_min = excluded.service_load_min, service_load_mean = excluded.service_load_mean,'
                            '    service_load_max = excluded.service_load_max, service_load_sum = excluded.service_load_sum,'
                            '    reserve_resources_min = excluded.reserve_resources_min, reserve_resources_mean = excluded.reserve_resources_mean,'
                            '    reserve_resources_max = excluded.reserve_resources_max, reserve_resources_sum = excluded.reserve_resources_sum,'
                            '    evaluation_min = excluded.evaluation_min, evaluation_mean = excluded.evaluation_mean,'
                            '    evaluation_max = excluded.evaluation_max',
                            (municipality, service_type_id, load['count'], load['min'], round(load['mean'], 2), load['max'], load['count'] * load['mean'],
                                    diff['min'], round(diff['mean']), diff['max'], diff['count'] * diff['mean'], evaluation['min'], round(evaluation['mean'], 2), evaluation['max']))
                except Exception:
                    log.error(f'Failed at service evaluation insertion on municipality: {municipality}, load: {load}, diff: {diff}')
                    raise
        
        if services['block'].nunique() > 0:
            load_describe = services.groupby('block')['Нормативная емкость'].describe()
            diff_describe = services.groupby('block')['diff'].describe()
            evaluation_describe = services.groupby('block')[coeff_name].describe()
            
            for block in load_describe.index:
                load = load_describe.loc[block]
                diff = diff_describe.loc[block]
                evaluation = evaluation_describe.loc[block]
                cur.execute('INSERT INTO provision.services_blocks (block_id, city_service_type_id, count,'
                        '    service_load_min, service_load_mean, service_load_max, service_load_sum, reserve_resources_min,'
                        '    reserve_resources_mean, reserve_resources_max, reserve_resources_sum, evaluation_min, evaluation_mean,'
                        '    evaluation_max)'
                        '  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        '  ON CONFLICT (block_id, city_service_type_id) DO UPDATE SET count = excluded.count,'
                        '    service_load_min = excluded.service_load_min, service_load_mean = excluded.service_load_mean,'
                        '    service_load_max = excluded.service_load_max, service_load_sum = excluded.service_load_sum,'
                        '    reserve_resources_min = excluded.reserve_resources_min, reserve_resources_mean = excluded.reserve_resources_mean,'
                        '    reserve_resources_max = excluded.reserve_resources_max, reserve_resources_sum = excluded.reserve_resources_sum,'
                        '    evaluation_min = excluded.evaluation_min, evaluation_mean = excluded.evaluation_mean,'
                        '    evaluation_max = excluded.evaluation_max',
                        (block, service_type_id, load['count'], load['min'], round(load['mean'], 2), load['max'], load['count'] * load['mean'],
                                diff['min'], round(diff['mean']), diff['max'], diff['count'] * diff['mean'], evaluation['min'],
                                round(evaluation['mean'], 2), evaluation['max']))

        # houses - services
        table_3['Нагрузка'] = table_3['Нагрузка'].apply(lambda x: round(x * normative['normative'] / 1000, 2))
        log.debug(f'houses-services: inserting "{service_type}" - {table_3.shape[0]} items')
        for house_id, (func_id, load) in table_3[['func_id', 'Нагрузка']].iterrows():
            if func_id != -1 and house_id != -1:
                cur.execute('INSERT INTO provision.houses_services (house_id, service_id, load) VALUES (%s, %s, %s)'
                        ' ON CONFLICT (house_id, service_id) DO UPDATE SET load = excluded.load', (house_id, func_id, load))
        
        cur.execute("UPDATE provision.normatives SET last_calculations = date_trunc('second', now()) WHERE city_service_type_id = %s", (service_type_id,))

if __name__ == '__main__':
    properties = Properties('localhost', 5432, 'city_db_final', 'postgres', 'postgres')
    properties_geometry = Properties('localhost', 5432, 'provision', 'postgres', 'postgres')
    city_name = 'Санкт-Петербург'
    public_transport_service_endpoint = 'http://10.32.1.61:8080/api.v2/isochrones'

    log.addHandler(logging.StreamHandler())
    log.handlers[-1].setFormatter(logging.Formatter('{asctime} [{levelname:^8}]: {message}', datefmt='%Y-%m-%d %H:%M:%S', style='{'))
    log.setLevel('DEBUG')

    parser = argparse.ArgumentParser(description='Inserts functional objects to the database')
    parser.add_argument('-hH', '--houses_db_addr', action='store', dest='houses_db_addr',
                        help=f'postgres host address for houses_db [default: {properties.db_addr}]', type=str)
    parser.add_argument('-hP', '--houses_db_port', action='store', dest='houses_db_port',
                        help=f'postgres port number for houses_db [default: {properties.db_port}]', type=int)
    parser.add_argument('-hd', '--houses_db_name', action='store', dest='houses_db_name',
                        help=f'postgres houses_db name [default: {properties.db_name}]', type=str)
    parser.add_argument('-hU', '--houses_db_user', action='store', dest='houses_db_user',
                        help=f'postgres user name for houses_db [default: {properties.db_user}]', type=str)
    parser.add_argument('-hW', '--houses_db_pass', action='store', dest='houses_db_pass',
                        help=f'postgres houses_db user\'s password [default: {properties.db_pass}]', type=str)
    parser.add_argument('-gH', '--geometry_db_addr', action='store', dest='geometry_db_addr',
                        help=f'postgres host address for geometry_db [default: {properties_geometry.db_addr}]', type=str)
    parser.add_argument('-gP', '--geometry_db_port', action='store', dest='geometry_db_port',
                        help=f'postgres port number for geometry_db [default: {properties_geometry.db_port}]', type=int)
    parser.add_argument('-gd', '--geometry_db_name', action='store', dest='geometry_db_name',
                        help=f'postgres geometry_db name [default: {properties_geometry.db_name}]', type=str)
    parser.add_argument('-gU', '--geometry_db_user', action='store', dest='geometry_db_user',
                        help=f'postgres user name for geometry_db [default: {properties_geometry.db_user}]', type=str)
    parser.add_argument('-gW', '--geometry_db_pass', action='store', dest='geometry_db_pass',
                        help=f'postgres geometry_db user\'s password [default: {properties_geometry.db_pass}]', type=str)
    parser.add_argument('-nts', '--no_transport_service', action='store_true', dest='nts',
                        help=f'do not wait for transport service to answer (in case when all geometry already loaded except those with empty features)')
    parser.add_argument('-c', '--city', action='store', dest='city', help=f'city to update provision [default: {city_name}]')
    parser.add_argument('-t', '--public_transport_service_endpoint', action='store', dest='public_transport_service_endpoint',
                        help=f'endpoint of the public transport service [default: {public_transport_service_endpoint}]')
    
    parser.add_argument('service_types', nargs='*', action='store', metavar='CITY_SERVICE_TYPE', default=[], type=str,
                        help='if set, update only given service types')

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
    if args.geometry_db_addr is not None:
        properties_geometry.db_addr = args.geometry_db_addr
    if args.geometry_db_port is not None:
        properties_geometry.db_port = args.geometry_db_port
    if args.geometry_db_name is not None:
        properties_geometry.db_name = args.geometry_db_name
    if args.geometry_db_user is not None:
        properties_geometry.db_user = args.geometry_db_user
    if args.geometry_db_pass is not None:
        properties_geometry.db_pass = args.geometry_db_pass
    if args.city is not None:
        city_name = args.city
    if args.public_transport_service_endpoint is not None:
        public_transport_service_endpoint = args.public_transport_service_endpoint

    log.info(f'Using houses database {properties.db_user}@{properties.db_addr}:{properties.db_port}/{properties.db_name}')
    log.info(f'Using geometry database {properties_geometry.db_user}@{properties_geometry.db_addr}:{properties_geometry.db_port}/{properties_geometry.db_name}')

    with properties.conn, properties.conn.cursor() as cur:
        cur.execute('SELECT id from cities where name = %s', (city_name,))
        res = cur.fetchone()
        city_id = res[0]
        if res is None:
            log.error(f'City with name "{city_name}" is missing in database. Exiting.')
            exit(1)
        cur.execute('SELECT st.name, n.normative, n.max_load, n.radius_meters, n.public_transport_time, n.service_evaluation,'
                '   n.house_evaluation FROM provision.normatives n'
                ' JOIN city_service_types st ON n.city_service_type_id = st.id')
        normatives = {
            service_type: {
                'normative': normative,
                'max_load': max_load,
                'radius_meters': radius,
                'public_transport_time': transport_time,
                'service_evaluation': service_evaluation,
                'house_evaluation': house_evaluation
            } for service_type, normative, max_load, radius, transport_time, service_evaluation, house_evaluation in cur.fetchall()
        }
        if len(args.service_types) == 0:
            cur.execute('SELECT name FROM city_service_types st'
                    '   JOIN provision.normatives n ON st.id = n.city_service_type_id'
                    ' WHERE st.id IN'
                    ' (SELECT distinct city_service_type_id FROM functional_objects f'
                    '       JOIN physical_objects p ON f.physical_object_id = p.id'
                    '   WHERE p.city_id = %s'
                    ' ) ORDER BY 1', (city_id,))
            args.service_types = list(itertools.chain.from_iterable(cur.fetchall()))
        else:
            service_types = []
            for service_type in args.service_types:
                cur.execute('SELECT EXISTS (SELECT 1 FROM city_service_types WHERE name = %s)', (service_type,))
                if cur.fetchone()[0]:
                    service_types.append(service_type)
                else:
                    log.warning('Requested service_type "{service_type}" is missing in the database, skipping')
            args.service_types = service_types
            del service_types
    
    log.info(f'Working with given city_service types: {", ".join(args.service_types)}')

    t_begin_all = time.time()
    for i, service_type in enumerate(args.service_types):
        if service_type not in normatives:
            log.warning(f'Service_type "{service_type}" is missing in normatives, skipping')
            continue
        log.info(f'Working with service_type "{service_type}" ({i:3} / {len(args.service_types):3})')
        log.info(f'Starting generation of table 2')
        t = time.time()
        t_begin_service_type = t
        table_2 = generate_table_2(properties.conn, properties_geometry.conn, service_type, normatives[service_type],
                wait_for_transport_service=not args.nts, public_transport_service_endpoint=public_transport_service_endpoint)
        delta = int(time.time() - t)
        log.info(f'Table 2 has finished in {delta // 3600}:{delta // 60 - delta // 3600 * 60:02}:{delta % 60:02}')

        log.info('Starting generation of table 1 and table 3')
        t = time.time()
        table_1, table_3 = generate_table_1_3(properties.conn, properties_geometry.conn, service_type, normatives[service_type],
                wait_for_transport_service=not args.nts)
        delta = int(time.time() - t)
        log.info(f'Table 1 and 3 have finished in {delta // 3600}:{delta // 60 - delta // 3600 * 60:02}:{delta % 60:02}')

        log.info(f'Starting processing the tables of service_type "{service_type}"')
        t = time.time()
        table_1, table_2, table_3 = process_tables(table_1, table_2, table_3, normatives[service_type])
        delta = int(time.time() - t)
        log.info(f'Processing finished in {delta // 3600}:{delta // 60 - delta // 3600 * 60:02}:{delta % 60:02}')

        log.info(f'Starting inserting the results of service_type "{service_type}" evaluation')
        ensure_tables(properties.conn)
        t = time.time()
        insert_results(properties.conn, table_1, table_2, table_3, service_type, normatives[service_type])
        delta = int(time.time() - t)
        log.info(f'Insertion finished in {delta // 3600}:{delta // 60 - delta // 3600 * 60:02}:{delta % 60:02}')

        delta = int(time.time() - t_begin_service_type)
        log.info(f'Service_type "{service_type}" is fully finished in {delta // 3600}:{delta // 60 - delta // 3600 * 60:02}:{delta % 60:02}')

    delta = int(time.time() - t_begin_all)
    log.info(f'Finished updating the provision of {len(args.service_types)} service_types in {delta // 3600}:{delta // 60 - delta // 3600 * 60:02}:{delta % 60:02}')


