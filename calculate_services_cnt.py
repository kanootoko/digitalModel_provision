import psycopg2
import argparse
import traceback
import time
import json
from typing import Tuple, List, Optional, Union
import threading
from multiprocessing import Pipe, Value
from multiprocessing.connection import Connection

try:
    import shapely, geopandas as gpd
    use_shapely = True
except ModuleNotFoundError:
    class gpd: # type: ignore
        class GeoDataFrame:
            def __init__(_):
                raise NotImplementedError('Please install geopandas to use this')
    use_shapely = False

from thread_pool import ThreadPool

class Properties:
    def __init__(
            self, provision_db_addr: str, provision_db_port: int, provision_db_name: str, provision_db_user: str, provision_db_pass: str,
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

def print_stats(pipe: Connection, start_time: float, done_now: Value, done_total: Value, timeout: Union[int, float] = 10) -> None:
    while not (pipe.poll() or pipe.closed):
        passed = int(time.time() - start_time)
        if passed == 0:
            passed = 1
        print(f'Time passed: {passed // 3600:3} hours, {passed // 60 % 60:2} minutes, {passed % 60:2} seconds. Done {done_total.value} totally,'  
                f' {done_now.value} for this session ({done_now.value / passed:7.3f} per second avg.)')
        time.sleep(timeout)
    if not pipe.closed:
        pipe.recv()

def count_service(house: Union[Tuple[float, float, float], Tuple[float, float]], service: str, t: int,
        avail_type: str, provision_conn: psycopg2.extensions.connection, houses_conn: Optional[psycopg2.extensions.cursor] = None,
        services_buildings: Optional[gpd.GeoDataFrame] = None, done_now: Optional[Value] = None, done_total: Optional[Value] = None) -> List[int]:
    assert houses_conn is not None or services_buildings is not None, 'connection to houses database or services_buildings must be provided'
    if t == 0:
        return []
    with provision_conn.cursor() as cur_provision:
        if done_total is not None:
            done_total.value += 1
        cur_provision.execute('SELECT services_list from service_counts where service_name = %s AND latitude = %s AND longitude = %s AND time = %s AND availability_type = %s',
                (service, house[0], house[1], t, avail_type))
        tmp = cur_provision.fetchall()
        if len(tmp) != 0:
            return tmp[0][0]
        cur_provision.execute(f'SELECT ST_AsGeoJSON(geometry) from {avail_type} WHERE latitude = %s AND longitude = %s AND time = %s', (house[0], house[1], t))
        tmp = cur_provision.fetchall()
        if len(tmp) != 0:
            geom = tmp[0][0]
        elif t >= 40:
            cur_provision.execute('SELECT services_list from service_counts where service_name = %s AND latitude = %s AND longitude = %s AND time > 40 AND availability_type = %s LIMIT 1',
                    (service, house[0], house[1], avail_type))
            tmp = cur_provision.fetchall()
            if len(tmp) != 0:
                return tmp[0][0]
            cur_provision.execute(f'SELECT ST_AsGeoJSON(geometry) from {avail_type} WHERE time = %s', (t,))
            tmp = cur_provision.fetchall()
            if len(tmp) != 0:
                geom = tmp[0][0]
            else:
                # print(f'Absolutely no {avail_type} geometry found for time={t}')
                return []
        else:
            # print(f'No {avail_type} geometry found for house#{house[2] if len(house) == 3 else "?"} ({house[0]}, {house[1]}) and time={t}') # type: ignore
            return []
        # print(f'counting services for "{service}" for house#{house[2] if len(house) == 3 else "?"} ({house[0]}, {house[1]} for time={t} by {avail_type}') # type: ignore
        if services_buildings is not None:
            servs = services_buildings[services_buildings['type'] == service]
            servs = servs[servs['geometry'].within(shapely.geometry.shape(json.loads(geom)))].dropna()
            services = list(servs.index)
        else:
            with houses_conn.cursor() as cur_houses: # type: ignore
                cur_houses.execute(f'SELECT phys_id FROM all_services WHERE service_type = %s AND ST_WITHIN(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326))', (service, geom,))

                services = list(map(lambda x: x[0], cur_houses.fetchall()))
        cur_provision.execute('INSERT INTO service_counts (service_name, house_id, latitude, longitude, time, availability_type, service_count, services_list)'
                ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', (service, house[2] if len(house) == 3 else None, house[0], house[1], # type: ignore
                        t, avail_type, len(services), json.dumps(services)))
        provision_conn.commit()
        if done_now is not None:
            done_now.value += 1
    return services

def count_services(services_buildings: Optional[gpd.GeoDataFrame], services: List[str], houses: List[Tuple[float, float]],
        walking: List[int], public: List[int], car: List[int], threads: int) -> None:
    pipe = Pipe()
    done_now = Value('i', 0)
    done_total = Value('i', 0)
    info_thread = threading.Thread(target=lambda: print_stats(pipe[1], time.time(), done_now, done_total, 10))
    info_thread.start()
    properties.close()
    if services_buildings is not None:
        tp = ThreadPool(threads, [lambda: ('provision_conn', psycopg2.connect(properties.provision_conn_string)),
                    lambda: ('services_buildings', services_buildings), lambda: ('done_now', done_now), lambda: ('done_total', done_total)],
                {'provision_conn': lambda conn: conn.close()}, max_size=threads + 2)
    else:
        tp = ThreadPool(threads, [lambda: ('provision_conn', psycopg2.connect(properties.provision_conn_string)),
                    lambda: ('houses_conn', psycopg2.connect(properties.houses_conn_string)),
                    lambda: ('done_now', done_now), lambda: ('done_total', done_total)],
                {'provision_conn': lambda conn: conn.close()}, max_size=threads + 2)
    try:
        for house in houses:
            print(f'Counting for house ({house[0]}:{house[1]})')
            for service in services:
                for t in walking:
                    tp.execute(count_service, (house, service, t, 'walking'))
                for t in public:
                    tp.execute(count_service, (house, service, t, 'transport'))
                for t in car:
                    tp.execute(count_service, (house, service, t, 'car'))
    except KeyboardInterrupt:
        print('Stopping ThreadPool')
        tp.stop()
    except Exception as ex:
        print(ex)
        traceback.print_exc()
        print('Exception occured, stopping')
    finally:
        print('Stopping info thread')
        pipe[0].send('stop')
        print('Waiting for the ThreadPool to join')
        tp.join()
        info_thread.join()


if __name__ == '__main__':
    # Default properties settings

    properties = Properties(
            'localhost', 5432, 'provision', 'postgres', 'postgres', 
            'localhost', 5432, 'citydb', 'postgres', 'postgres'
    )

    parser = argparse.ArgumentParser(
        description='Calculates service counts after geometry is loaded')
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
    parser.add_argument('-g', '--geometry_utility', action='store', dest='geometry_utility',
                        help=f'geometry testing utility: "phapely" (if installed) or "postgis" [default: shapely]', type=str, default='shapely')
    parser.add_argument('-t', '--threads', action='store', dest='threads',
                        help=f'threads (processes) number for calculations [default: 8]', type=int, default=8)
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
    if args.geometry_utility == 'postgis':
        use_shapely = False
    elif args.geometry_utility == 'shapely':
        if not use_shapely:
            print('Geometry utility is set to shapely, but geopandas is missing. Install shapely and geopandas before launching')
            exit(1)
    else:
        print(f'Unknown geometry utility type: {args.geometry_utility}, exiting')
        exit(1)

    with properties.provision_conn.cursor() as cur:
        cur.execute('CREATE TABLE IF NOT EXISTS service_counts ('
                'id serial PRIMARY KEY NOT NULL,'
                'service_name varchar NOT NULL,'
                'house_id integer,'
                'latitude float NOT NULL,'
                'longitude float NOT NULL,'
                'time integer NOT NULL,'
                'availability_type varchar(30) NOT NULL,'
                'service_count integer NOT NULL,'
                'services_list jsonb NOT NULL,'
                'UNIQUE (service_name, latitude, longitude, time, availability_type)'
                ')')

        properties.provision_conn.commit()

        cur.execute('SELECT DISTINCT walking FROM needs ORDER BY 1')
        walking_time: List[int] = list(filter(lambda x: x != 0, map(lambda y: y[0], cur.fetchall())))

        cur.execute('SELECT DISTINCT public_transport FROM needs ORDER BY 1')
        public_transport_time: List[int] = list(filter(lambda x: x != 0, map(lambda y: y[0], cur.fetchall())))

        cur.execute('SELECT DISTINCT personal_transport FROM needs ORDER BY 1')
        car_time: List[int] = list(filter(lambda x: x != 0, map(lambda y: y[0], cur.fetchall())))


    with properties.houses_conn.cursor() as cur:
        cur.execute(f'SELECT ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float, ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float, id FROM houses where district_id = (SELECT id from districts where full_name = %s)', ('Петроградский район',))
        houses: List[Tuple[float, float]] = list(cur.fetchall())

        cur.execute('SELECT DISTINCT name from service_types')
        services: List[str] = list(map(lambda x: x[0], cur.fetchall()))

        cur.execute('CREATE MATERIALIZED VIEW IF NOT EXISTS all_services AS SELECT p.id, p.geometry, p.description, f.name, f.opening_hours, f.website,'
                '    f.phone, f.infrastructure_type_id, b.address, b.year_construct, b.year_repair,'
                '    b.floors, b.height, b.basement_area, b.project_type, bt.name AS basement_type, ft.name AS floor_type,'
                '    pc.name AS pollution_category, f.capacity'
                ' FROM functional_objects f'
                '    JOIN service_types st ON f.service_type_id = st.id'
                '    JOIN phys_objs_fun_objs pf ON f.id = pf.fun_obj_id'
                '    JOIN physical_objects p ON pf.phys_obj_id = p.id'
                '    JOIN buildings b ON p.id = b.physical_object_id'
                '    LEFT JOIN pollution_categories pc ON p.pollution_category_id = pc.id'
                '    LEFT JOIN basement_types bt ON b.basement_type_id = bt.id'
                '    LEFT JOIN floor_types ft ON b.floor_type_id = ft.id')
        cur.execute('REFRESH MATERIALIZED VIEW all_services')

        services_buildings: Optional[gpd.GeoDataFrame]
        if use_shapely:
            services_buildings = gpd.GeoDataFrame.from_postgis('SELECT phys_id as id, ST_Centroid(geometry) as geometry, capacity, service_type as type FROM all_services',
                    properties.houses_conn, 'geometry').set_index('id')
        else:
            services_buildings = None
        
    
    print(f'Loaded {len(houses)} houses, {len(walking_time)} walking times, {len(public_transport_time)} public_transport times and {len(car_time)} car times')
    print(f'Working with {len(services)} services')
    time.sleep(2)


    start_time = time.time()
    try:
        count_services(services_buildings, services, houses, walking_time, public_transport_time, car_time, args.threads)
    except Exception as ex:
        print(f'Exception occured! {ex}')
        traceback.print_exc()
    finally:
        print(f'The process took {time.time() - start_time:.2f} seconds') #. Inserted {done_now} entities, {done_total} done totally')