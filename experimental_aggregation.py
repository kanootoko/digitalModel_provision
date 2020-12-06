import argparse
from thread_pool import ThreadPool
import psycopg2
import pandas as pd
try:
    import geopandas as gpd
except ModuleNotFoundError:
    class gpd: # type: ignore
        class GeoDataFrame:
            def __init__(_):
                raise NotImplementedError('Please install geopandas to use this')
    print('GeoPandas is missing, blocks initialization will end up with an error')
import json
import time
import itertools
from typing import Optional, List, Tuple, Dict, Union, Any

class Properties:
    def __init__(self, provision_db_addr: str, provision_db_port: int, provision_db_name: str, provision_db_user: str, provision_db_pass: str,
            houses_db_addr: str, houses_db_port: int, houses_db_name: str, houses_db_user: str, houses_db_pass: str):
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
    def houses_conn(self):
        if self._houses_conn is None or self._houses_conn.closed:
            self._houses_conn = psycopg2.connect(self.houses_conn_string)
        return self._houses_conn
            
    @property
    def provision_conn(self):
        if self._provision_conn is None or self._provision_conn.closed:
            self._provision_conn = psycopg2.connect(self.provision_conn_string)
        return self._provision_conn

    def close(self):
        if self.houses_conn is not None:
            self._houses_conn.close()
        if self._provision_conn is not None:
            self._provision_conn.close()

def ensure_tables(provision_conn: psycopg2.extensions.connection) -> None:
    with provision_conn.cursor() as cur:
        cur.execute('CREATE TABLE IF NOT EXISTS blocks_soc_groups ('
                ' block_id int UNIQUE NOT NULL,'
                ' social_groups jsonb NOT NULL'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS blocks_services ('
                ' block_id int UNIQUE NOT NULL,'
                ' services jsonb NOT NULL'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS municipalities_soc_groups ('
                ' municipality_id int UNIQUE NOT NULL,'
                ' social_groups jsonb NOT NULL'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS municipalities_services ('
                ' municipality_id int UNIQUE NOT NULL,'
                ' services jsonb NOT NULL'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS districts_soc_groups ('
                ' district_id int UNIQUE NOT NULL,'
                ' social_groups jsonb NOT NULL'
                ')'
        )
        cur.execute('CREATE TABLE IF NOT EXISTS districts_services ('
                ' district_id int UNIQUE NOT NULL,'
                ' services jsonb NOT NULL'
                ')'
        )
        provision_conn.commit()

def truncate_tables(provision_conn: psycopg2.extensions.connection) -> None:
    with provision_conn.cursor() as cur:
        cur.execute('TRUNCATE TABLE IF EXISTS blocks_soc_groups')
        cur.execute('TRUNCATE TABLE IF EXISTS blocks_services')
        cur.execute('TRUNCATE TABLE IF EXISTS municipalities_soc_groups')
        cur.execute('TRUNCATE TABLE IF EXISTS municipalities_services')
        cur.execute('TRUNCATE TABLE IF EXISTS districts_soc_groups')
        cur.execute('TRUNCATE TABLE IF EXISTS districts_services')
        provision_conn.commit()

def get_social_groups_block(houses: gpd.GeoDataFrame, block_id: int, geometry, provision_conn: Optional[psycopg2.extensions.connection] = None, update = False) -> pd.Series:
    if not update and provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('SELECT social_groups FROM blocks_soc_groups where block_id = %s', (block_id,))
            tmp = cur.fetchall()
            if len(tmp) != 0:
                return pd.Series(tmp[0][0], name='social_group', dtype=int)
    groups = houses[houses['geometry'].within(geometry)].dropna().groupby('social_group')['number'].sum().astype(int)
    groups = groups[groups > 0]
    if provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('INSERT INTO blocks_soc_groups (block_id, social_groups) VALUES (%s, %s) ON CONFLICT (block_id) DO UPDATE SET social_groups = %s', (block_id, *([json.dumps(groups.to_dict())] * 2)))
            provision_conn.commit()
    return pd.Series(groups, dtype=int)

def get_social_groups_municipality(blocks: Union[gpd.GeoDataFrame, pd.DataFrame], municipality: str, provision_conn: Optional[psycopg2.extensions.connection] = None, update = False) -> pd.Series:
    if not update and provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('SELECT social_groups FROM municipalities_soc_groups where municipality_id = (SELECT id FROM municipalities WHERE full_name = %s)', (municipality,))
            tmp = cur.fetchall()
            if len(tmp) != 0:
                return pd.Series(tmp[0][0], name='social_group', dtype=int)
    groups: Dict[str, int] = dict()
    for social_groups in blocks[blocks['municipality'] == municipality]['social_groups'].dropna():
        for social_group, cnt in social_groups.items():
            groups[social_group] = groups.get(social_group, 0) + cnt
    if provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('INSERT INTO municipalities_soc_groups (municipality_id, social_groups)'
                    ' VALUES ((SELECT id FROM municipalities WHERE full_name = %s), %s) ON CONFLICT (municipality_id) DO UPDATE SET social_groups = %s', (municipality, *([json.dumps(groups)] * 2)))
            provision_conn.commit()
    return pd.Series(groups, name='social_group', dtype=int)

def get_social_groups_district(blocks: Union[gpd.GeoDataFrame, pd.DataFrame], district: str, provision_conn: Optional[psycopg2.extensions.connection] = None, update = False) -> pd.Series:
    if not update and provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('SELECT social_groups FROM districts_soc_groups where district_id = (SELECT id FROM districts WHERE full_name = %s)', (district,))
            tmp = cur.fetchall()
            if len(tmp) != 0:
                return pd.Series(tmp[0][0], name='social_group', dtype=int)
    groups: Dict[str, int] = dict()
    for social_groups in blocks[blocks['district'] == district]['social_groups'].dropna():
        for social_group, cnt in social_groups.items():
            groups[social_group] = groups.get(social_group, 0) + cnt
    if provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('INSERT INTO districts_soc_groups (district_id, social_groups)'
                    ' VALUES ((SELECT id FROM districts WHERE full_name = %s), %s) ON CONFLICT (district_id) DO UPDATE SET social_groups = %s', (district, *([json.dumps(groups)] * 2)))
            provision_conn.commit()
    return pd.Series(groups, name='social_group', dtype=int)

city_social_groups: Optional[pd.Series] = None
def get_social_groups_city(blocks: pd.DataFrame) -> pd.Series:
    global city_social_groups
    if city_social_groups is None:
        groups: Dict[str, int] = dict()
        for social_groups in blocks['social_groups'].dropna():
            for social_group, cnt in social_groups.items():
                groups[social_group] = groups.get(social_group, 0) + cnt
        city_social_groups = pd.Series(groups, name='social_group', dtype=int)
    return city_social_groups

capacity_result = {
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

def get_services_block(block_id: int, geometry, provision_conn: Optional[psycopg2.extensions.connection] = None, update = False) -> pd.DataFrame:
    if not update and provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('SELECT services FROM blocks_services where block_id = %s', (block_id,))
            tmp = cur.fetchall()
            if len(tmp) != 0:
                return pd.DataFrame(tmp[0][0])
    servs = services_buildings[services_buildings['geometry'].within(geometry)].dropna()
    servs['result'] = servs['capacity'].apply(lambda capacity: capacity_result[int(capacity)])
    group = servs.groupby('type')['capacity']
    services = pd.DataFrame(group.count().astype(int))
    services.columns = ['cnt']
    services = services.join(group.sum().astype(int))
    services.columns = ['cnt', 'sum']
    services = services.join(servs.groupby('type')['result'].sum().astype(int))
    services.columns = ['cnt', 'sum', 'res']
    if provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('INSERT INTO blocks_services (block_id, services) VALUES (%s, %s) ON CONFLICT (block_id) DO UPDATE SET services = %s', (block_id, *([json.dumps(services.to_dict())] * 2)))
            provision_conn.commit()
    return services

def get_services_municipality(blocks: Union[gpd.GeoDataFrame, pd.DataFrame], municipality: str, provision_conn: Optional[psycopg2.extensions.connection] = None, update = False) -> pd.DataFrame:
    if not update and provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('SELECT services FROM municipalities_services where municipality_id = (SELECT id FROM municipalities WHERE full_name = %s)', (municipality,))
            tmp = cur.fetchall()
            if len(tmp) != 0:
                return pd.DataFrame(tmp[0][0])
    servs = dict()
    for services in blocks[blocks['municipality'] == municipality]['services'].dropna():
        for social_group, cnt in services['cnt'].items():
            if not social_group in servs:
                servs[social_group] = [0, 0, 0]
            servs[social_group][0] += cnt
        for social_group, sum in services['sum'].items():
            servs[social_group][1] += sum
        for social_group, res in services['res'].items():
            servs[social_group][2] += res
    res = pd.DataFrame(servs.values(), columns=('cnt', 'sum', 'res'), index=servs.keys())
    if provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('INSERT INTO municipalities_services (municipality_id, services) VALUES'
                    '((SELECT id FROM municipalities WHERE full_name = %s), %s) ON CONFLICT (municipality_id) DO UPDATE SET services = %s', (municipality, *([json.dumps(res.to_dict())] * 2)))
            provision_conn.commit()
    return res

def get_services_district(blocks: Union[gpd.GeoDataFrame, pd.DataFrame], district: str, provision_conn: Optional[psycopg2.extensions.connection] = None, update = False) -> pd.DataFrame:
    if not update and provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('SELECT services FROM districts_services where district_id = (SELECT id FROM districts WHERE full_name = %s)', (district,))
            tmp = cur.fetchall()
            if len(tmp) != 0:
                return pd.DataFrame(tmp[0][0])
    servs: Dict[str, List[int]] = dict()
    for services in blocks[blocks['district'] == district]['services'].dropna():
        for social_group, cnt in services['cnt'].items():
            if not social_group in servs:
                servs[social_group] = [0, 0, 0]
            servs[social_group][0] += cnt
        for social_group, sum in services['sum'].items():
            servs[social_group][1] += sum
        for social_group, res in services['res'].items():
            servs[social_group][2] += res
    res = pd.DataFrame(servs.values(), columns=('cnt', 'sum', 'res'), index=servs.keys())
    if provision_conn is not None:
        with provision_conn.cursor() as cur:
            cur.execute('INSERT INTO districts_services (district_id, services) VALUES'
                    '((SELECT id FROM districts WHERE full_name = %s), %s) ON CONFLICT (district_id) DO UPDATE SET services = %s', (district, *([json.dumps(res.to_dict())] * 2)))
            provision_conn.commit()
    return res


city_services: pd.DataFrame = None
def get_services_city(blocks: pd.DataFrame) -> pd.DataFrame:
    global city_services
    if city_services is None:
        servs: Dict[str, List[int]] = dict()
        for services in blocks['services'].dropna():
            for social_group, cnt in services['cnt'].items():
                if not social_group in servs:
                    servs[social_group] = [0, 0, 0]
                servs[social_group][0] += cnt
            for social_group, sum in services['sum'].items():
                servs[social_group][1] += sum
            for social_group, res in services['res'].items():
                servs[social_group][2] += res
        city_services = pd.DataFrame(servs.values(), columns=('cnt', 'sum', 'res'), index=servs.keys())
    return city_services

def get_needs(needs: pd.DataFrame, infrastructure: pd.DataFrame, social_group: str, living_situation: str, service: str) -> Tuple[int, int, int, int, float]:
    assert service in infrastructure['service'].unique(), f'Service "{service}" is missing in infrastructure'
    n = needs[needs['city_function'] == infrastructure[infrastructure['service'] == service]['function'].iloc[0]]
    n = n[(n['social_group'] == social_group) & (n['living_situation'] == living_situation)]
    if n.shape[0] == 0:
        # print(f'No needs found for social_group = {social_group}, living_situation = {living_situation}, service = {service}')
        return 0, 0, 0, 0, 0.0
    n = n.iloc[0]
    return n['walking'], n['transport'], n['car'], n['intensity'], n['significance']

def get_soc_groups_for_service(needs: pd.DataFrame, infrastructure: pd.DataFrame,
            service: str, living_situation: Optional[str] = None) -> List[str]:
    n = needs[needs['city_function'] == infrastructure[infrastructure['service'] == service]['function'].iloc[0]]
    if living_situation is not None:
        return list(n[n['living_situation'] == living_situation]['social_group'].unique())
    else:
        return list(n['social_group'].unique())

def get_balance(needs: pd.DataFrame, infrastructure: pd.DataFrame, social_groups: Optional[pd.Series],
            services: Optional[pd.Series], service: str, living_situation: str) -> float:
    if social_groups is None or services is None:
        return 0.0
    if service not in services.index:
        return 0.0
    relevants = get_soc_groups_for_service(needs, infrastructure, service, living_situation)
    people = social_groups[social_groups.index.isin(relevants)].sum()
    if people == 0:
        return 1.0
    capacity = services.loc[service]
    return capacity / people

def average_balance(walking_balance: float, transport_balance: float, car_balance: float) -> float:
    if car_balance == 0:
        if transport_balance == 0:
            return walking_balance
        elif walking_balance == 0:
            return transport_balance
        return walking_balance * 0.6 + transport_balance * 0.4
    elif transport_balance == 0:
        if car_balance == 0:
            return walking_balance
        elif walking_balance == 0:
            return car_balance
        return walking_balance * 0.6 + transport_balance * 0.4
    elif walking_balance == 0:
        if transport_balance == 0:
            return car_balance
        elif car_balance == 0:
            return transport_balance
        return transport_balance * 0.6 + car_balance * 0.4
    return walking_balance * 0.5 + transport_balance * 0.3 + car_balance * 0.2

def aggregate_inner(blocks: pd.DataFrame, needs: pd.DataFrame, infrastructure: pd.DataFrame, social_groups_count: Tuple[Optional[pd.Series], Optional[pd.Series], pd.Series],
            services_count: Tuple[Optional[pd.Series], Optional[pd.Series], pd.Series],
            social_groups: List[str], living_situations: List[str], services: List[str], return_debug_info: bool = False) -> Dict[str, Any]:
    cur_soc_groups_count: pd.Series = next(filter(lambda x: x is not None, social_groups_count))
    calculations = 0
    total_loyalty = 0.0
    loyalty = 0.0
    debug_info: List[Dict[str, Any]] = list()
    for social_group in social_groups:
        if social_group not in cur_soc_groups_count:
            continue
        soc_group_loyalty: Optional[float] = None
        total_soc_group_loyalty = 0.0
        soc_group_cnt = 0
        for living_situation in living_situations:
            for service in services:
                walking, transport, car, intensity, significance = get_needs(needs, infrastructure, social_group, living_situation, service)
                intensity /= 10 # type: ignore
                debug: Dict[str, Any] = dict()
                if return_debug_info:
                    debug['social_group'] = social_group
                    debug['living_situation'] = living_situation
                    debug['service'] = service
                    debug['walking_cost'] = int(walking)
                    debug['public_transport_cost'] = int(transport)
                    debug['personal_transport_cost'] = int(car)
                    debug['intensity'] = float(intensity)
                    debug['significance'] = float(significance)
                if walking == 0 and transport == 0 and car == 0 or intensity == 0 or significance == 0:
                    if return_debug_info:
                        debug['result'] = 'skipped'
                        debug_info.append(debug)
                    continue
                d_s_name: List[str] = ['-', '-', '-']
                d_s: List[Tuple[Optional[pd.Series], Optional[pd.Series]]] = [(None, None), (None, None), (None, None)]

                if walking == 0:
                    pass
                elif walking < 11 and social_groups_count[0] is not None and services_count[0] is not None:
                    d_s_name[0] = 'block'
                    d_s[0] = social_groups_count[0], services_count[0]['res']
                elif walking < 21 and social_groups_count[1] is not None and services_count[1] is not None:
                    d_s_name[0] = 'municipality'
                    d_s[0] = social_groups_count[1], services_count[1]['res']
                elif walking < 41:
                    d_s_name[0] = 'district'
                    d_s[0] = social_groups_count[2], services_count[2]['res']
                else:
                    d_s_name[0] = 'city'
                    d_s[0] = get_social_groups_city(blocks), get_services_city(blocks)['res']

                if transport == 0:
                    pass
                elif transport < 11 and social_groups_count[1] is not None and services_count[1] is not None:
                    d_s_name[1] = 'municipality'
                    d_s[1] = social_groups_count[1], services_count[1]['res']
                elif transport < 21:
                    d_s_name[1] = 'district'
                    d_s[1] = social_groups_count[2], services_count[2]['res']
                else:
                    d_s_name[1] = 'city'
                    d_s[1] = get_social_groups_city(blocks), get_services_city(blocks)['res']

                if car == 0:
                    pass
                elif 0 < car < 11 and social_groups_count[1] is not None and services_count[1] is not None:
                    d_s_name[2] = 'municipality'
                    d_s[2] = social_groups_count[1], services_count[1]['res']
                elif car < 21 and social_groups_count[1] is not None and services_count[1] is not None:
                    d_s_name[2] = 'district'
                    d_s[2] = social_groups_count[2], services_count[2]['res']
                else:
                    d_s_name[2] = 'city'
                    d_s[2] = get_social_groups_city(blocks), get_services_city(blocks)['res']
                balances = get_balance(needs, infrastructure, d_s[0][0], d_s[0][1], service, living_situation), \
                        get_balance(needs, infrastructure, d_s[1][0], d_s[1][1], service, living_situation), \
                        get_balance(needs, infrastructure, d_s[2][0], d_s[2][1], service, living_situation)
                if return_debug_info:
                    debug['balances_territories'] = d_s_name
                    debug['balances_raw'] = list(map(lambda x: round(x, 4), balances))
                if intensity < 0.5:
                    balances = balances[0] * 5, balances[1] * 5, balances[2] * 5
                else:
                    balances = balances[0] ** (intensity + 1) / 5 ** intensity, balances[1] ** (intensity + 1) / 5 ** intensity, \
                            balances[2] ** (intensity + 1) / 5 ** intensity

                loyalty = average_balance(*balances)
                if return_debug_info:
                    debug['balances'] = list(map(lambda x: round(x, 4), balances))
                    debug['loyalty_raw'] = round(loyalty, 4)
                # if 0.6 < significance <= 0.8:
                #     if loyalty <= 2:
                #         loyalty = 0
                #     elif loyalty <= 3:
                #         loyalty = 2
                #     elif loyalty <= 4:
                #         loyalty = 3
                # elif significance > 0.8:
                #     if loyalty <= 3:
                #         loyalty = 0
                #     elif loyalty <= 4:
                #         loyalty = 3

                if return_debug_info:
                    debug['loyalty'] = round(loyalty, 4)
                    debug_info.append(debug)

                calculations += 1
                soc_group_cnt += 1
                total_soc_group_loyalty += loyalty
                if soc_group_loyalty is None or loyalty <= soc_group_loyalty:
                    soc_group_loyalty = loyalty
            
        if soc_group_loyalty is not None:
            if len(social_groups) != 1:
                loyalty += soc_group_loyalty * cur_soc_groups_count.loc[social_group] / cur_soc_groups_count.sum()
                total_loyalty += total_soc_group_loyalty / soc_group_cnt * cur_soc_groups_count.loc[social_group] / cur_soc_groups_count.sum()
            else:
                loyalty = soc_group_loyalty
                total_loyalty = total_soc_group_loyalty
    res = {'loyalty': round(loyalty, 4) if loyalty is not None else 0.0,
            'alternative_loyalty': round(total_loyalty, 4) if total_loyalty is not None else 0.0, 'calculations': calculations}
    if return_debug_info:
        res['debug_info'] = debug_info # type: ignore
    return res

def aggregate(needs: pd.DataFrame, infrastructure: pd.DataFrame, blocks: Union[pd.DataFrame, gpd.GeoDataFrame], target: Union[str, int], social_group: Optional[str] = None,
            living_situation: Optional[str] = None, city_service: Optional[str] = None, return_debug_info: bool = False) -> Dict[str, Any]:
    if target in blocks['district'].unique():
        social_groups_count = (None, None, get_social_groups_district(blocks, target)) # type: ignore
        services = (None, None, get_services_district(blocks, target)) # type: ignore
    elif target in blocks['municipality'].unique():
        social_groups_count = (None, get_social_groups_municipality(blocks, target), get_social_groups_district(blocks, blocks[blocks['municipality'] == target]['district'].iloc[0])) # type: ignore
        services = (None, get_services_municipality(blocks, target), get_services_district(blocks, blocks[blocks['municipality'] == target]['district'].iloc[0])) # type: ignore
    elif target in blocks.index:
        social_groups_count = (blocks.loc[target]['social_groups'], get_social_groups_municipality(blocks, blocks.loc[target]['municipality'].iloc[0]), get_social_groups_district(blocks, blocks[blocks['district'] == target]['district'].iloc[0]))
        services = (blocks.loc[target]['services'], get_services_municipality(blocks, blocks.loc[target]['municipality'].iloc[0]), get_services_district(blocks, blocks.loc[target]['district'].iloc[0]))
    else:
        raise Exception(f'{target} is neither a block, municipality or district')
    if social_group is None:
        social_groups = list(needs['social_group'].unique())
    else:
        social_groups = [social_group]
    if living_situation is None:
        if social_group is not None:
            living_situations = list(needs[needs['social_group'] == social_group]['living_situation'].unique())
        else:
            living_situations = list(needs['living_situation'].unique())
    else:
        living_situations = [living_situation]
    if city_service is None:
        if social_group is not None:
            city_services = list(infrastructure[infrastructure['function'].isin(needs[needs['social_group'] == social_group]['city_function'].unique())]['service'].dropna().unique())
        else:
            city_services = list(infrastructure['service'].dropna().unique())
    else:
        city_services = [city_service]
    res = aggregate_inner(blocks, needs, infrastructure, social_groups_count, services, social_groups, living_situations, city_services, return_debug_info)
    if return_debug_info:
        res['debug_info'] = {'target': target, 'social_group': social_group, 'living_situation': living_situation,
                'city_service': city_service, 'calculations': res['debug_info']}
    return res

if __name__ == '__main__':
    # Default properties settings
    properties = Properties(
            'localhost', 5432, 'provision', 'postgres', 'postgres', 
            'localhost', 5432, 'citydb', 'postgres', 'postgres'
    )
    target = '-'

    parser = argparse.ArgumentParser(
        description='Calculates service cnts after geometry is loaded')
    parser.add_argument('-pH', '--provision_db_addr', action='store', dest='provision_db_addr',
                        help=f'provision database address [default: {properties.provision_db_addr}]', type=str)
    parser.add_argument('-pP', '--provision_db_port', action='store', dest='provision_db_port',
                        help=f'provision database port number [default: {properties.provision_db_port}]', type=int)
    parser.add_argument('-pd', '--provision_db_name', action='store', dest='provision_db_name',
                        help=f'provision database database name [default: {properties.provision_db_name}]', type=str)
    parser.add_argument('-pU', '--provision_db_user', action='store', dest='provision_db_user',
                        help=f'provision database user name [default: {properties.provision_db_user}]', type=str)
    parser.add_argument('-pW', '--provision_db_pass', action='store', dest='provision_db_pass',
                        help=f'provision database user password [default: {properties.provision_db_pass}]', type=str)
    parser.add_argument('-hH', '--houses_db_addr', action='store', dest='houses_db_addr',
                        help=f'houses database address [default: {properties.houses_db_addr}]', type=str)
    parser.add_argument('-hP', '--houses_db_port', action='store', dest='houses_db_port',
                        help=f'houses database port number [default: {properties.houses_db_port}]', type=int)
    parser.add_argument('-hd', '--houses_db_name', action='store', dest='houses_db_name',
                        help=f'houses database database name [default: {properties.houses_db_name}]', type=str)
    parser.add_argument('-hU', '--houses_db_user', action='store', dest='houses_db_user',
                        help=f'houses database user name [default: {properties.houses_db_user}]', type=str)
    parser.add_argument('-hW', '--houses_db_pass', action='store', dest='houses_db_pass',
                        help=f'houses database user password [default: {properties.houses_db_pass}]', type=str)
    parser.add_argument('-t', '--target', action='store', dest='target',
                        help=f'calculation target district or municipality [default: {target}]', type=str)
    parser.add_argument('-T', '--truncate', action='store_true', dest='truncate',
                        help='truncate tables and restart the calculations',)
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
    if args.target is not None:
        target = args.target

    print('Loading data from database')
    blocks: gpd.GeoDataFrame = gpd.GeoDataFrame.from_postgis('SELECT b.id, b.geometry, m.full_name as municipality, d.full_name as district FROM'
            ' blocks b LEFT JOIN municipalities m ON m.id = b.municipality_id LEFT JOIN districts d ON d.id = m.district_id',
            properties.houses_conn, 'geometry').set_index('id')
    print(f'Loaded {len(blocks.index)} blocks, {len(blocks["municipality"].dropna().unique())} municipalities and {len(blocks["district"].dropna().unique())} districts')
    municipalities_geom: gpd.GeoDataFrame = gpd.GeoDataFrame.from_postgis('SELECT id, full_name as name, geometry, population FROM municipalities', properties.houses_conn, 'geometry').set_index('id')
    districts_geom: gpd.GeoDataFrame = gpd.GeoDataFrame.from_postgis('SELECT id, full_name as name, geometry, population FROM districts', properties.houses_conn, 'geometry').set_index('id')
    services_buildings: gpd.GeoDataFrame = gpd.GeoDataFrame.from_postgis('SELECT p.id as id, ST_Centroid(p.geometry) as geometry, f.capacity, st.name as type FROM buildings b'
                ' JOIN physical_objects p ON b.physical_object_id = p.id'
                ' JOIN phys_objs_fun_objs pf ON p.id = pf.phys_obj_id'
                ' JOIN functional_objects f ON f.id = pf.fun_obj_id'
                ' JOIN service_types st on f.service_type_id = st.id', properties.houses_conn, 'geometry').set_index('id')
    print(f'Loaded {services_buildings.shape[0]} services of {len(services_buildings["type"].unique())} types')
    with properties.houses_conn.cursor() as cur:
        houses: gpd.GeoDataFrame = gpd.GeoDataFrame.from_postgis('SELECT id, ST_Centroid(geometry) AS geometry FROM houses', properties.houses_conn, 'geometry').set_index('id')
        print(f'{houses.shape[0]} houses loaded')
        cur.execute('SELECT ss.house_id, s.name AS social_group, ss.number FROM social_structure ss JOIN social_groups s ON s.id = ss.social_group_id')
        houses = houses.join(pd.DataFrame(cur.fetchall(), columns=('id', 'social_group', 'number')).set_index('id'))
        print(f'{houses.shape[0]} houses together with social groups data')

        cur.execute('SELECT s.name, l.name, f.name, n.walking, n.public_transport, n.personal_transport, n.intensity FROM needs n'
                ' JOIN social_groups s ON s.id = n.social_group_id'
                ' JOIN living_situations l ON l.id = n.living_situation_id'
                ' JOIN city_functions f ON f.id = n.city_function_id')
        needs = pd.DataFrame(cur.fetchall(), columns=('social_group', 'living_situation', 'city_function', 'walking', 'transport', 'car', 'intensity'))
        cur.execute('SELECT s.name, f.name, v.significance FROM values v'
                ' JOIN social_groups s ON s.id = v.social_group_id'
                ' JOIN city_functions f ON f.id = v.city_function_id')
        tmp = pd.DataFrame(cur.fetchall(), columns=('social_group', 'city_function', 'significance'))
        needs = needs.merge(tmp, on=['social_group', 'city_function'], how='inner')


        cur.execute('SELECT i.name, f.name, s.name from city_functions f JOIN infrastructure_types i ON i.id = f.infrastructure_type_id'
                ' JOIN service_types s ON s.city_function_id = f.id ORDER BY i.name,f.name,s.name;')
        infrastructure = pd.DataFrame(cur.fetchall(), columns=('infrastructure', 'function', 'service'))

    if target == 'everything':
        is_district = True
        targets = list(districts_geom['name'])
    elif target in list(blocks['district'].unique()):
        is_district = True
        targets = [target]
    elif target in list(blocks['municipality'].unique()):
        is_district = False
        targets = [target]
    elif target in ('-', 'additional'):
        is_district = False
        targets = []
    else:
        print(f'Unknown target: {target}')
        exit(1)

    ensure_tables(properties.provision_conn)
    if args.truncate:
        truncate_tables(properties.provision_conn)
    if target != '-':
        tp = ThreadPool(16, [lambda: ('provision_conn', psycopg2.connect(properties.provision_conn_string))],
                {'provision_conn': lambda conn: conn.close()}, max_size=16)
        for target in targets:
            if is_district:
                municipalities: List[str] = list(blocks[blocks['district'] == target]['municipality'].unique())
            else:
                municipalities = [target]
            print(f'Running through {len(municipalities)} municipalities in "{target}"')
            for municipality in municipalities:
                municipality_blocks = blocks[blocks['municipality'] == municipality]
                print(f'Running through {municipality_blocks.shape[0]} blocks in "{municipality}" (area={municipalities_geom[municipalities_geom["name"] == municipality]["geometry"].iloc[0].area * 10000:6.4f})')
                municipality_ss = pd.Series(name=('social_group'), dtype=int)
                municipality_services = pd.Series(name='type', dtype=int)
                for id, (geometry, _, _) in municipality_blocks.iterrows():
                    print(f'{time.ctime()}: block #{id}, area={geometry.area * 10000:8.6f}')
                    # soc_groups_sum = get_social_groups_block(houses, id, geometry, properties.provision_conn)
                    # services_sum = get_services_block(houses, id, geometry, properties.provision_conn)
                    tp.execute(get_social_groups_block, (houses, id, geometry))
                    tp.execute(get_services_block, (id, geometry))
                    # block_services = services_buildings[services_buildings['geometry'].within(geometry)].dropna()
                    # municipality_ss = municipality_ss.add(soc_groups_sum, fill_value=0)
                    # municipality_services = municipality_services.add(services_sum, fill_value=0)
                # municipality_ss = municipality_ss.dropna()
                # municipality_services = municipality_services.dropna()
                # print(municipality_ss)
                # print(municipality_services)
        if args.target in ('everything', 'additional'):
            print('additional blocks without municipalities:')
            for id, (geometry, _, _) in blocks[blocks['municipality'].isna()].iterrows():
                print(f'{time.ctime()}: block #{id}, area={geometry.area * 10000:8.6f}')
                # soc_groups_sum = get_social_groups_block(houses, id, geometry, properties.provision_conn)
                # services_sum = get_services_block(houses, id, geometry, properties.provision_conn)
                tp.execute(get_social_groups_block, (houses, id, geometry))
                tp.execute(get_services_block, (id, geometry))
        tp.join()

    with properties.provision_conn.cursor() as cur:
        cur.execute('SELECT s.block_id, ss.social_groups, s.services FROM blocks_soc_groups ss JOIN blocks_services s on s.block_id = ss.block_id')
        blocks = blocks.join(pd.DataFrame(cur.fetchall(), columns=('id', 'social_groups', 'services')).set_index('id'))

    blocks_old = blocks
    blocks = blocks_old[blocks_old.index.isin(blocks_old[['social_groups', 'services']].dropna().index)]
    if blocks.shape[0] != blocks_old.shape[0]:
        print(f'Warning! {blocks_old.shape[0] - blocks.shape[0]} blocks of {blocks_old.shape[0]} are missing'
                'services and/or social structure information. Consider running script with "-t everything" parameter')
        print(blocks_old[~blocks_old.index.isin(blocks.index)])
    else:
        print(f'All {blocks.shape[0]} blocks have services and social structure calculated')
    del blocks_old

    # print(f'city social groups:\n{city_social_groups}')
    # print(f'city services:\n{city_services}')

    for municipality in list(blocks['municipality'].dropna().unique()):
        get_services_municipality(municipality, properties.provision_conn)
        get_social_groups_municipality(municipality, properties.provision_conn)
    for district in list(blocks['district'].dropna().unique()):
        get_services_district(blocks, district, properties.provision_conn)
        get_social_groups_district(blocks, district, properties.provision_conn)

    social_group = 'Студенты и учащиеся'
    living_situations = 'Типичный рабочий день'
    service = 'Аптека'
    district = 'Василеостровский район'
    municipalitity = 'Муниципальное образование №7'

    from pprint import pprint
    pprint(aggregate(needs, infrastructure, blocks, district, social_group, None, service, True))
