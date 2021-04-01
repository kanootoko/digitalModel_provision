import json, argparse, psycopg2
from queue import Empty
from threading import Thread, Lock
from typing import Any, Dict, Set, Tuple, List, Callable, Optional, Union
from os import environ
from multiprocessing import Queue
import traceback
import requests
import threading
import time
import os

class Properties:
    def __init__(
            self, provision_db_addr: str, provision_db_port: int, provision_db_name: str, provision_db_user: str, provision_db_pass: str,
            houses_db_addr: str, houses_db_port: int, houses_db_name: str, houses_db_user: str, houses_db_pass: str,
            walking_api_endpoint: str, transport_api_endpoint: str, car_api_endpoint: str):
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

        self.walking_api_endpoint = walking_api_endpoint
        self.transport_api_endpoint = transport_api_endpoint
        self.car_api_endpoint = car_api_endpoint

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
        if self.houses_conn is not None and not self._houses_conn.closed:
            self._houses_conn.close()
        if self._provision_conn is not None and not self._provision_conn.closed:
            self._provision_conn.close()

class Stats:
    def __init__(self, total: int, summary: int):
        self.done_summary = summary
        self.done = 0
        self.errors = 0
        self.current = 0
        self.total = total
        self.lock = Lock()

    def inc_done(self) -> None:
        with self.lock:
            self.done += 1
            self.done_summary += 1

    def inc_current(self) -> None:
        with self.lock:
            self.current += 1
    
    def inc_errors(self) -> None:
        with self.lock:
            self.errors += 1
    
    def get_done(self) -> int:
        with self.lock:
            return self.done
    
    def get_current(self) -> int:
        with self.lock:
            return self.current

    def get_errors(self) -> int:
        with self.lock:
            return self.errors

    def get_summary(self) -> int:
        with self.lock:
            return self.done_summary

    def get_total(self) -> int:
        return self.total

    def get_all(self) -> Tuple[int, int, int, int, int]:
        return (self.done, self.errors, self.current, self.done_summary, self.total)

properties: Properties

def _get_walking(lat: float, lan: float, t: Union[int, List[int]], conn: psycopg2.extensions.connection,
        walking_endpoint: str, timeout: int = 360, stats: Optional[Stats] = None) -> Dict[int, Dict[str, Any]]:
    if isinstance(t, int):
        t = [t]
    if stats is not None:
        done_now, errors, current, summary, total = stats.get_all()
        for t_cur in t:
            print(f'{time.ctime()}: getting walking   ({lat:<6}, {lan:<6}, {t_cur:2}): {summary:<6} of {total:<6} are done ({done_now:<6} now, {current:<6} current, {errors:<5} errors)')
    walking: Dict[int, Dict[str, Any]] = dict()
    with conn.cursor() as cur:
        for feature in requests.get(walking_endpoint.format(lat=lat, lng=lan, t=f'[{",".join(map(str, t))}]'), timeout=timeout).json()['features']:
        # tmp = requests.get(walking_endpoint.format(lat=lat, lng=lan, t=f'[{",".join(map(str, t))}]'), timeout=timeout).json()
        # print(tmp)
        # for feature in tmp['features']:
            walking[feature['properties']['time']] = feature['geometry']
            cur.execute('INSERT INTO walking (latitude, longitude, time, geometry) VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)) ON CONFLICT DO NOTHING',
                    (lat, lan, feature['properties']['time'], json.dumps(feature['geometry'])))
        conn.commit()
        # print(walking)
    return walking

def get_walking(lat: float, lan: float, t: int, conn: psycopg2.extensions.connection, walking_endpoint: str, timeout: int = 20) -> Dict[str, Any]:
    lat, lan = round(lat, 6), round(lan, 6)
    with conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(geometry) FROM walking WHERE latitude = %s AND longitude = %s AND time = %s', (lat, lan, t))
        res = cur.fetchall()
        if len(res) != 0:
            return res[0][0]
        try:
            geom = _get_walking(lat, lan, t, conn, walking_endpoint, timeout)[t]
        except requests.exceptions.ConnectTimeout:
            cur.execute('SELECT geometry, ST_Distance(geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) AS min_distance'
                    'FROM walking WHERE time = %s ORDER BY 2 LIMIT 1', (lat, lan, t))
            geom = json.loads(cur.fetchone())
            if len(res) == 0:
                return {'type': 'Polygon', 'coordinates': []}
            return geom
        return geom

def _get_transport(lat: float, lan: float, t: Union[int, List[int]], conn: psycopg2.extensions.connection,
        transport_endpoint: str, timeout: int = 240, stats: Optional[Stats] = None) -> Dict[int, Dict[str, Any]]:
    if isinstance(t, int):
        t = [t]
    transport: Dict[int, Dict[str, Any]] = dict()
    with conn.cursor() as cur:
        for t_cur in t:
            if stats is not None:
                done_now, errors, current, summary, total = stats.get_all()
                print(f'{time.ctime()}: getting transport ({lat:<6}, {lan:<6}, {t_cur:2}): {summary:<6} of {total:<6} are done ({done_now:<6} now, {current:<6} current, {errors:<5} errors)')
            data = requests.post(transport_endpoint, timeout=timeout, json=
                {
                    'source': [lan, lat],
                    'cost': t_cur * 60,
                    'day_time': 46800,
                    'mode_type': 'pt_cost'
                }
            ).json()
            if 'features' not in data:
                print(f'ERROR! "features" is not found in data from transport model service\ndata:\n{data}')
                geom = {'type': 'Polygon', 'coordinates': []}
            if len(data['features']) == 0:
                geom = {'type': 'Polygon', 'coordinates': []}
            elif len(data['features']) > 1:
                cur.execute('SELECT ST_AsGeoJSON(ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')', data['features'])) + ']))')
                geom = json.loads(cur.fetchall()[0][0])
            else:
                geom = data['features'][0]['geometry']
            transport[t_cur] = geom
            if 'features' in data:
                cur.execute('INSERT INTO transport (latitude, longitude, time, geometry) VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)) ON CONFLICT DO NOTHING',
                        (lat, lan, t_cur, json.dumps(geom)))
        conn.commit()
    return transport

def get_transport(lat: float, lan: float, t: int, conn: psycopg2.extensions.connection, transport_endpoint: str, timeout: int = 20) -> Dict[str, Any]:
    lat, lan = round(lat, 6), round(lan, 6)
    with conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(geometry) FROM transport WHERE latitude = %s AND longitude = %s AND time = %s', (lat, lan, t))
        res = cur.fetchall()
        if len(res) != 0:
            return res[0][0]
        try:
            geom = _get_transport(lat, lan, t, conn, transport_endpoint, timeout)[t]
        except requests.exceptions.ConnectTimeout:
            cur.execute('SELECT geometry, ST_Distance(geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) AS min_distance'
                    'FROM transport WHERE time = %s ORDER BY 2 LIMIT 1', (lat, lan, t))
            geom = json.loads(cur.fetchone())
            if len(res) == 0:
                return {'type': 'Polygon', 'coordinates': []}
        return geom

def _get_car(lat: float, lan: float, t: Union[int, List[int]], conn: psycopg2.extensions.connection, car_endpoint: str,
        timeout: int = 240, stats: Optional[Stats] = None) -> Dict[int, Dict[str, Any]]:
    if isinstance(t, int):
        t = [t]
    car: Dict[int, Dict[str, Any]] = dict()
    with conn.cursor() as cur:
        for t_cur in t:
            if stats is not None:
                done_now, errors, current, summary, total = stats.get_all()
                print(f'{time.ctime()}: getting car ({lat:<6}, {lan:<6}, {t_cur:2}): {summary:<6} of {total:<6} are done ({done_now:<6} now, {current:<6} current, {errors:<5} errors)')
            data = requests.post(car_endpoint, timeout=timeout, json=
                {
                    'source': [lan, lat],
                    'cost': t_cur * 60,
                    'day_time': 46800,
                    'mode_type': 'car_cost'
                }
            ).json()
            if 'features' not in data:
                print(f'ERROR! "features" is not found in data from transport model service\ndata:\n{data}')
                geom = {'type': 'Polygon', 'coordinates': []}
            if len(data['features']) == 0:
                geom = {'type': 'Polygon', 'coordinates': []}
            elif len(data['features']) > 1:
                print(f'Car availability is more than 1 ({len(data["features"])}) poly: ({lat}, {lan}, {t_cur})')
                cur.execute('SELECT ST_AsGeoJSON(ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')', data['features'])) + ']))')
                geom = json.loads(cur.fetchall()[0][0])
            else:
                geom = data['features'][0]['geometry']
            car[t_cur] = geom
            if 'features' in data:
                cur.execute('INSERT INTO car (latitude, longitude, time, geometry) VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)) ON CONFLICT DO NOTHING',
                        (lat, lan, t_cur, json.dumps(geom)))
        conn.commit()
    return car

def get_car(lat: float, lan: float, t: int, conn: psycopg2.extensions.connection, car_endpoint: str, timeout: int = 20) -> Dict[str, Any]:
    lat, lan = round(lat, 6), round(lan, 6)
    with conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(geometry) FROM car WHERE latitude = %s AND longitude = %s AND time = %s', (lat, lan, t))
        res = cur.fetchall()
        if len(res) != 0:
            return res[0][0]
        try:
            geom = _get_car(lat, lan, t, conn, car_endpoint, timeout)[t]
        except requests.exceptions.ConnectTimeout:
            cur.execute('SELECT geometry, ST_Distance(geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) AS min_distance'
                    'FROM car WHERE time = %s ORDER BY 2 LIMIT 1', (lat, lan, t))
            geom = json.loads(cur.fetchone())
            if len(res) == 0:
                return {'type': 'Polygon', 'coordinates': []}
        return geom


class ThreadPoolThread(threading.Thread):
    def __init__(self, inputQueue: Queue, done: Set[Tuple[float, float, int]],
            conn: psycopg2.extensions.connection, endpoint: str, request_timeout: int = 240, timeout: int = 10, stats: Optional[Stats] = None):
        super().__init__()
        self.inputQueue = inputQueue
        self._stopping = False
        self.done = done
        self.conn = conn
        self.endpoint = endpoint
        self.request_timeout = request_timeout
        self.timeout = timeout
        self.stats = stats

    def stop(self):
        self._stopping = True

    def run(self):
        while not self._stopping:
            try:
                val = self.inputQueue.get(True, timeout=self.timeout)
                task, lat, lan, t = val
                if isinstance(t, list):
                    for _ in range(len(t)):
                        self.stats.inc_current()
                else:
                    self.stats.inc_current()
                errors = 0
                while True:
                    try:
                        task(lat, lan, t, self.conn, self.endpoint, self.request_timeout, self.stats)
                        if isinstance(t, list):
                            for t_cur in t:
                                self.stats.inc_done()
                                self.done.add((lat, lan, t_cur))
                        else:
                            self.stats.inc_done()
                            self.done.add((lat, lan, t))
                        break
                    except Exception as ex:
                        errors += 1
                        # if not (isinstance(ex, requests.exceptions.ConnectTimeout)):
                        #     traceback.print_exc()
                        if self._stopping:
                            break
                        if isinstance(ex, psycopg2.Error):
                            self.conn.rollback()
                        if errors > 4:
                            if isinstance(ex, requests.exceptions.ConnectTimeout):
                                print(f'Request failed, dropping task (Timeout): error #{errors} / 5')
                            else:
                                print(f'Request failed, dropping task (Exception: {repr(ex)}): error #{errors} / 5')
                            self.stats.inc_errors()
                            break
                        else:
                            if isinstance(ex, requests.exceptions.ConnectTimeout):
                                print(f'Request failed, waiting 20 seconds and trying again (Timeout): error #{errors} / 5')
                            else:
                                print(f'Request failed, waiting 20 seconds and trying again (Exception: {repr(ex)}): error #{errors} / 5')
                            time.sleep(20)
            except Empty:
                pass

class ThreadPool:
    def __init__(self, n: int, done: Set[Tuple[float, float, int]], conn: psycopg2.extensions.connection, endpoint: str,
            request_timeout: int = 240, timeout: int = 10, stats: Optional[Stats] = None):
        self.n = n
        self.done = done
        self.conn = conn
        self.tasks_queue: Queue = Queue(n * 2)
        self.threads: List[ThreadPoolThread] = []
        self.endpoint = endpoint
        self.request_timeout = request_timeout
        self.timeout = timeout
        self.stats = stats

    def start(self):
        for _ in range(self.n):
            self.threads.append(ThreadPoolThread(self.tasks_queue, self.done, self.conn, self.endpoint, self.request_timeout, self.timeout, stats=self.stats))
            self.threads[-1].start()

    def add(self, task: Callable[[float, float, int, psycopg2.extensions.connection, str, int, Stats], str], lat: float, lan: float, t: int):
        self.tasks_queue.put((task, lat, lan, t))
        
    def stop(self):
        for thread in self.threads:
            thread.stop()
        try:
            while True:
                self.tasks_queue.get_nowait()
        except Empty:
            pass
            
    def join(self):
        for thread in self.threads:
            thread.join()

    def is_alive(self) -> bool:
        return len(list(filter(ThreadPoolThread.is_alive, self.threads))) != 0


class WalkingThread(threading.Thread):
    def __init__(self, houses: List[Tuple[float, float]], times: List[int], threads: int = 1):
        super().__init__()
        self.houses = houses
        self.times = times
        self._stopping = False
        self.threads = threads

    def run(self):
        start_time = time.time()
        done: Set[Tuple[float, float, int]] = set()

        with psycopg2.connect(properties.provision_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT latitude, longitude, time FROM walking')
                done = set(cur.fetchall())

            stats = Stats(len(self.houses) * len(self.times), len(done))
            print(f'Starting walking  : #houses = {len(self.houses)}, times = {self.times}, (ready: {len(done)})')
            thread_pool = ThreadPool(self.threads, done, conn, properties.walking_api_endpoint, 480, stats=stats)
            thread_pool.start()
            for lat, lan in self.houses:
                if self._stopping:
                    break
                need_times: List[int] = []
                for t in self.times:
                    if (lat, lan, t) in done:
                        stats.inc_current()
                    else:
                        need_times.append(t)
                if len(need_times) != 0:
                    thread_pool.add(_get_walking, lat, lan, need_times)
            while thread_pool.is_alive():
                if self._stopping:
                    print('Stopping walking')
                    thread_pool.stop()
                    thread_pool.join()
                    break
                time.sleep(10)
            print(f'{time.ctime()}: walking is done: {stats.get_done()} in {time.time() - start_time:5.3f} seconds ({stats.get_done() / (time.time() - start_time):.2f} per second). {stats.get_errors()} errors')

    def stop(self):
        self._stopping = True

class TransportThread(threading.Thread):
    def __init__(self, houses: List[Tuple[float, float]], times: List[int], threads: int = 1):
        super().__init__()
        self.houses = houses
        self.times = times
        self._stopping = False
        self.threads = threads

    def run(self):
        start_time = time.time()
        done: Set[Tuple[float, float, int]] = set()
        
        with psycopg2.connect(properties.provision_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT latitude, longitude, time FROM transport')
                done = set(cur.fetchall())

            stats = Stats(len(self.houses) * len(self.times), len(done))
            print(f'Starting transport: #houses = {len(self.houses)}, times = {self.times} (ready: {len(done)})')
            thread_pool = ThreadPool(self.threads, done, conn, properties.transport_api_endpoint, stats=stats)
            thread_pool.start()
            for t in self.times:
                if self._stopping:
                    break
                for lat, lan in self.houses:
                    if self._stopping:
                        break
                    if (lat, lan, t) in done:
                        stats.inc_current()
                        continue
                    thread_pool.add(_get_transport, lat, lan, t)
            while thread_pool.is_alive():
                time.sleep(10)
                if self._stopping:
                    print('Stopping transport')
                    thread_pool.stop()
                    thread_pool.join()
                    break
        print(f'{time.ctime()}: transport is done: {stats.get_done()} in {time.time() - start_time:5.3f} seconds ({stats.get_done() / (time.time() - start_time):.2f} per second). {stats.get_errors()} errors')

    def stop(self):
        self._stopping = True

class CarThread(threading.Thread):
    def __init__(self, houses: List[Tuple[float, float]], times: List[int], threads: int = 1):
        super().__init__()
        self.houses = houses
        self.times = times
        self._stopping = False
        self.threads = threads

    def run(self):
        start_time = time.time()
        done: Set[Tuple[float, float, int]] = set()
        
        with psycopg2.connect(properties.provision_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT latitude, longitude, time FROM car')
                done = set(cur.fetchall())
            
            stats = Stats(len(self.houses) * len(self.times), len(done))
            print(f'Starting car: #houses = {len(self.houses)}, times = {self.times} (ready: {len(done)})')
            thread_pool = ThreadPool(self.threads, done, conn, properties.car_api_endpoint, stats=stats)
            thread_pool.start()
            for t in self.times:
                if self._stopping:
                    break
                for lat, lan in self.houses:
                    if self._stopping:
                        break
                    if (lat, lan, t) in done:
                        stats.inc_current()
                        continue
                    thread_pool.add(_get_car, lat, lan, t)
            while thread_pool.is_alive():
                time.sleep(10)
                if self._stopping:
                    print('Stopping car')
                    thread_pool.stop()
                    thread_pool.join()
                    break
        print(f'{time.ctime()}: car is done: {stats.get_done()} in {time.time() - start_time:5.3f} seconds ({stats.get_done() / (time.time() - start_time):.2f} per second). {stats.get_errors()} errors')

    def stop(self):
        self._stopping = True

if __name__ == '__main__':
    
    # Default properties settings

    properties = Properties('localhost', 5432, 'provision', 'postgres', 'postgres',
            'localhost', 5432, 'citydb', 'postgres', 'postgres',
            'http://10.32.1.62:5002/pedastrian_walk/isochrones?x_from={lng}&y_from={lat}&times={t}',
            'http://10.32.1.61:8080/api.v2/isochrones', 'http://10.32.1.61:8080/api.v2/isochrones')
    transport_threads = 1
    walking_threads = 1
    car_threads = 1

    # CLI Arguments

    parser = argparse.ArgumentParser(
        description='Starts the collection of polygons of avaliability')
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
    parser.add_argument('-w', '--walking_threads', action='store', dest='walking_threads',
                        help=f'number of threads to request walking geometry [default: {walking_threads}]', type=int)
    parser.add_argument('-t', '--transport_threads', action='store', dest='transport_threads',
                        help=f'number of threads to request transport geometry [default: {transport_threads}]', type=int)
    parser.add_argument('-c', '--car_threads', action='store', dest='car_threads',
                        help=f'number of threads to request car geometry [default: {car_threads}]', type=int)
    parser.add_argument('-aW', '--walking_model_api', action='store', dest='walking_model_api',
                        help=f'url of endpoint of walking model [default: {properties.walking_api_endpoint}]', type=str)
    parser.add_argument('-aT', '--transport_model_api', action='store', dest='transport_model_api',
                        help=f'url of endpoint of public transport model [default: {properties.transport_api_endpoint}]', type=str)
    parser.add_argument('-aC', '--car_model_api', action='store', dest='car_model_api',
                        help=f'url of endpoint of personal transport model [default: {properties.car_api_endpoint}]', type=str)
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
    if args.walking_threads is not None:
        walking_threads = args.walking_threads
    if args.transport_threads is not None:
        transport_threads = args.transport_threads
    if args.car_threads is not None:
        car_threads = args.car_threads
    if args.walking_model_api is not None:
        properties.walking_api_endpoint = args.walking_model_api
    if args.transport_model_api is not None:
        properties.transport_api_endpoint = args.transport_model_api
    if args.car_model_api is not None:
        properties.car_api_endpoint = args.car_model_api
    
    with properties.houses_conn.cursor() as cur:
        cur.execute('SELECT DISTINCT walking from needs order by 1')
        walking_time = list(filter(lambda x: x != 0, map(lambda y: y[0], cur.fetchall())))

        cur.execute('SELECT DISTINCT public_transport from needs order by 1')
        public_transport_time = list(filter(lambda x: x != 0, map(lambda y: y[0], cur.fetchall())))

        cur.execute('SELECT DISTINCT personal_transport from needs order by 1')
        car_time = list(filter(lambda x: x != 0, map(lambda y: y[0], cur.fetchall())))

        cur.execute(f'SELECT DISTINCT ROUND(ST_X(ST_Centroid(geometry))::numeric, 6)::float, ROUND(ST_Y(ST_Centroid(geometry))::numeric, 6)::float FROM houses')
        houses: List[Tuple[float, float]] = list(map(lambda x: (x[0], x[1]), cur.fetchall()))

    with properties.provision_conn.cursor() as cur:
        cur.execute('CREATE EXTENSION IF NOT EXISTS postgis')
        for table_name in ('walking', 'transport', 'car'):
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS {table_name} ('
                '   latitude float NOT NULL,'
                '   longitude float NOT NULL,'
                '   time int NOT NULL,'
                '   geometry geometry,'
                '   primary key(latitude, longitude, time)'
                ')'
            )
        properties.provision_conn.commit()

    print(f'Using postgres connections:')
    print(f'\t\thouses: {properties.houses_db_user}@{properties.houses_db_addr}:{properties.houses_db_port}/{properties.houses_db_name},')
    print(f'\t\tprovision: {properties.provision_db_user}@{properties.provision_db_addr}:{properties.provision_db_port}/{properties.provision_db_name},')

    walking = None
    transport = None
    car = None
    Queue(0)

    if walking_threads != 0:
        walking = WalkingThread(houses, walking_time, walking_threads)
    if transport_threads != 0:
        transport = TransportThread(houses, public_transport_time, transport_threads)
    if car_threads != 0:
        car = CarThread(houses, car_time, car_threads)

    try:
        if walking is not None:
            walking.start()
        if transport is not None:
            transport.start()
        if car is not None:
            car.start()
        
        if walking is not None:
            walking.join()
        if transport is not None:
            transport.join()
        if car is not None:
            car.join()
    except:
        print('Stopping threads')
        for thread in (walking, transport, car):
            if thread is not None and thread.is_alive():
                thread.stop()
        for thread in (walking, transport, car):
            if thread is not None and thread.is_alive():
                thread.join()
