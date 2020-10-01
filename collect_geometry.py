import json, argparse, psycopg2
from queue import Empty
from threading import Thread, Lock
from typing import Any, Set, Tuple, List, Callable, Optional, Union
from os import environ
from multiprocessing import Queue
import traceback
import requests
import threading
import time
import sqlite3
import os

class Properties:
    def __init__(self, db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, transport_model_api_endpoint: str):
        self.db_addr = db_addr
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.transport_model_api_endpoint = transport_model_api_endpoint

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

SQLITE3_FILENAME = 'geometry.sqlite3'

def get_walking(lat: float, lan: float, t: int, stats: Stats, timeout=60):
    done_now, errors, current, summary, total = stats.get_all()
    print(f'{time.ctime()}: getting walking   ({lat:<6}, {lan:<6}, {t:2}): {summary:<6} of {total:<6} are done ({done_now:<6} now, {current:<6} current, {errors:<5} errors)')
    data = json.dumps(
            requests.get(f'http://galton.urbica.co/api/foot/?lng={lat}&lat={lan}&radius=5&cellSize=0.1&intervals={t}',
                    timeout=timeout).json()['features'][0]['geometry']
    )
    return data


def get_transport(lat: float, lan: float, t: int, stats: Stats, conn: psycopg2.extensions.connection, timeout=180):
    cur: psycopg2.extensions.cursor = conn.cursor()
    try:
        done_now, errors, current, summary, total = stats.get_all()
        print(f'{time.ctime()}: getting transport ({lat:<6}, {lan:<6}, {t:2}): {summary:<6} of {total:<6} are done ({done_now:<6} now, {current:<6} current, {errors:<5} errors)')
        data = requests.post(properties.transport_model_api_endpoint, timeout=timeout, json=
            {
                'source': [lan, lat],
                'cost': t * 60,
                'day_time': 46800,
                'mode_type': 'pt_cost'
            }
        ).json()
        cur.execute('SELECT ST_AsGeoJSON(ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')', data['features'])) + ']))')
        data = cur.fetchall()[0][0]
    except Exception as ex:
        if 'ARRAY[]' in str(ex):
            print(f'Getting transport failed, empty geometry')
            conn.rollback()
            data = 'null'
        else:
            raise
    finally:
        cur.close()
    return data


class ThreadPoolThread(threading.Thread):
    def __init__(self, inputQueue: Queue, # Queue[Tuple[Callable[[Tuple[float, float, int]], str], float, float, int]],
            outputQueue: Queue, done: Set[Tuple[float, float, int]], stats: Stats, timeout: int = 10, **kwargs): # Queue[Tuple[float, float, int, str]]
        super().__init__()
        self.inputQueue = inputQueue
        self.outputQueue = outputQueue
        self._stopping = False
        self.done = done
        self.stats = stats
        self.timeout = timeout
        if 'conn' in kwargs:
            self.conn = conn

    def stop(self):
        self._stopping = True

    def run(self):
        while not self._stopping:
            try:
                val = self.inputQueue.get(True, timeout=self.timeout)
                self.stats.inc_current()
                task, lat, lan, t = val
                errors = 0
                while True:
                    try:
                        if 'conn' in dir(self):
                            self.outputQueue.put((lat, lan, t, task(lat, lan, t, self.stats, conn=self.conn)))
                        else:
                            self.outputQueue.put((lat, lan, t, task(lat, lan, t, self.stats)))
                        self.stats.inc_done()
                        self.done.add((lat, lan, t))
                        break
                    except requests.exceptions.ConnectTimeout as ex:
                        errors += 1
                        if self._stopping:
                            break
                        if errors > 4:
                            print(f'Request failed, dropping task (Timeout): error #{errors} / 5')
                            self.stats.inc_errors()
                            break
                        else:
                            print(f'Request failed, waiting 20 seconds and trying again (Timeout): error #{errors} / 5')
                            time.sleep(20)
                    except Exception as ex:
                        errors += 1
                        traceback.print_exc()
                        if self._stopping:
                            break
                        if errors > 4:
                            print(f'Request failed, dropping task (Exception: {repr(ex)}): error #{errors} / 5')
                            self.stats.inc_errors()
                            break
                        else:
                            print(f'Request failed, waiting 20 seconds and trying again (Exception: {repr(ex)}): error #{errors} / 5')
                            time.sleep(20)
            except Empty:
                pass


class SavingThread(threading.Thread):
    def __init__(self, walking_queue: Queue, transport_queue: Queue): # Queue[Tuple[float, float, int, str]
        super().__init__()
        self.walking_queue = walking_queue
        self.transport_queue = transport_queue
        self._stopping = False

    def run(self):
        with sqlite3.connect(SQLITE3_FILENAME) as conn_sl3:
            cur_sl3 = conn_sl3.cursor()
            while not self._stopping:
                try:
                    res = []
                    res.append(self.walking_queue.get(timeout=5))
                    l = self.walking_queue.qsize()
                    try:
                        while len(res) < max(15, l):
                            res.append(self.walking_queue.get(timeout=5))
                    except Empty:
                        pass
                    for lat, lan, t, geometry in res:
                        try:
                            cur_sl3.execute(f"insert into walking   (latitude, longitude, time, geometry) values ({lat}, {lan}, {t}, '{geometry}')")
                            print(f"insert into walking   (latitude, longitude, time, geometry) values ({lat:<6}, {lan:<6}, {t:2}, '...')")
                        except sqlite3.IntegrityError:
                            print(f"insert into walking   (latitude, longitude, time, geometry) values ({lat:<6}, {lan:<6}, {t:2}, '...') FAILED")
                except Empty:
                    pass
                print('Done saving walking, now transport')
                try:
                    res = []
                    res.append(self.transport_queue.get(timeout=5))
                    l = self.transport_queue.qsize()
                    try:
                        while len(res) < max(15, l):
                            res.append(self.transport_queue.get(timeout=5))
                    except Empty:
                        pass
                    for lat, lan, t, geometry in res:
                        try:
                            cur_sl3.execute(f"insert into transport (latitude, longitude, time, geometry) values ({lat}, {lan}, {t}, '{geometry}')")
                            print(f"insert into transport (latitude, longitude, time, geometry) values ({lat:<6}, {lan:<6}, {t:2}, '...')")
                        except sqlite3.IntegrityError:
                            print(f"insert into transport (latitude, longitude, time, geometry) values ({lat:<6}, {lan:<6}, {t:2}, '...') FAILED")
                except Empty:
                    pass
                print('Done saving transport, commiting')
                conn_sl3.commit()
        try:
            while True:
                self.walking_queue.get_nowait()
        except Empty:
            pass
        try:
            while True:
                self.transport_queue.get_nowait()
        except Empty:
            pass
        print(f'{time.ctime()}: Saving is finished')

    def stop(self):
        self._stopping = True

class ThreadPool:
    def __init__(self, n: int, resultQueue: Queue, done: Set[Tuple[float, float, int]], stats: Stats, **kwargs):
        self.n = n
        self.stats = stats
        self.done = done
        self.tasks_queue: Queue = Queue() # Queue[Tuple[Callable[[float, float, int], str], float, float, int]]
        self.results_queue = resultQueue
        self.threads: List[ThreadPoolThread] = []
        if 'conn' in kwargs:
            self.conn = kwargs['conn']

    def start(self):
        for _ in range(self.n):
            if 'conn' in dir(self):
                self.threads.append(ThreadPoolThread(self.tasks_queue, self.results_queue, self.done, self.stats, conn=self.conn))
            else:
                self.threads.append(ThreadPoolThread(self.tasks_queue, self.results_queue, self.done, self.stats))
            self.threads[-1].start()

    def add(self, task: Callable[[float, float, int], str], lat: float, lan: float, t: int):
        self.tasks_queue.put((task, lat, lan, t))
        
    def stop(self):
        for thread in self.threads:
            thread.stop()
        try:
            while True:
                self.tasks_queue.get_nowait()
        except Empty:
            pass
        try:
            while True:
                self.results_queue.get_nowait()
        except Empty:
            pass
    
    def join(self):
        for thread in self.threads:
            thread.join()

    def is_alive(self) -> bool:
        return len(list(filter(ThreadPoolThread.is_alive, self.threads))) != 0


class WalkingThread(threading.Thread):
    def __init__(self, houses: List[Tuple[float, float]], times: List[int], result_queue: Queue, threads: int = 1):
        super().__init__()
        self.houses = houses
        self.times = times
        self.result_queue = result_queue
        self._stopping = False
        self.threads = threads

    def run(self):
        start_time = time.time()
        done: Set[Tuple[float, float, int]] = set()

        with sqlite3.connect(SQLITE3_FILENAME) as conn_sl3:
            cur_sl3 = conn_sl3.cursor()
            done = set(cur_sl3.execute('select latitude, longitude, time from walking').fetchall())

        stats = Stats(len(self.houses) * len(self.times), len(done))
        print(f'Starting walking  : #houses = {len(self.houses)}, times = {self.times}, (ready: {len(done)})')
        thread_pool = ThreadPool(self.threads, self.result_queue, done, stats)
        thread_pool.start()
        for lat, lan in self.houses:
            if self._stopping:
                break
            for t in self.times:
                if self._stopping:
                    break
                if (lat, lan, t) in done:
                    stats.inc_current()
                    continue
                thread_pool.add(get_walking, lat, lan, t)
        while thread_pool.is_alive():
            time.sleep(10)
            if self._stopping:
                print('Stopping walking')
                thread_pool.stop()
                thread_pool.join()
                break
        print(f'{time.ctime()}: walking is done: {stats.get_done()} in {time.time() - start_time:5.3f} seconds. {stats.get_errors()} errors')

    def stop(self):
        self._stopping = True

class TransportThread(threading.Thread):
    def __init__(self, houses: List[Tuple[float, float]], times: List[int], result_queue: Queue, threads: int = 1):
        super().__init__()
        self.houses = houses
        self.times = times
        self.result_queue = result_queue
        self._stopping = False
        self.threads = threads

    def run(self):
        start_time = time.time()
        done: Set[Tuple[float, float, int]] = set()
        with sqlite3.connect(SQLITE3_FILENAME) as conn_sl3:
            cur_sl3 = conn_sl3.cursor()
            done = set(cur_sl3.execute('select latitude, longitude, time from transport').fetchall())
        with psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
                f' user={properties.db_user} password={properties.db_pass}') as conn:

            stats = Stats(len(self.houses) * len(self.times), len(done))
            print(f'Starting transport: #houses = {len(self.houses)}, times = {self.times} (ready: {len(done)})')
            thread_pool = ThreadPool(self.threads, self.result_queue, done, stats, conn=conn)
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
                    thread_pool.add(get_transport, lat, lan, t)
            while thread_pool.is_alive():
                time.sleep(10)
                if self._stopping:
                    print('Stopping transport')
                    thread_pool.stop()
                    thread_pool.join()
                    break
        print(f'{time.ctime()}: transport is done: {stats.get_done()} in {time.time() - start_time:5.3f} seconds. {stats.get_errors()} errors')

    def stop(self):
        self._stopping = True

if __name__ == '__main__':
    
    # Default properties settings

    properties = Properties('localhost', 5432, 'citydb', 'postgres', 'postgres', 'http://10.32.1.61:8080/api.v2/isochrones')
    transport_threads = 1
    walking_threads = 1

    # CLI Arguments

    parser = argparse.ArgumentParser(
        description='Starts the collection of polygons of avaliability')
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
    parser.add_argument('-w', '--walking_threads', action='store', dest='walking_threads',
                        help=f'number of threads to request walking geometry [default: {walking_threads}]', type=int)
    parser.add_argument('-t', '--transport_threads', action='store', dest='transport_threads',
                        help=f'number of threads to request transport geometry [default: {transport_threads}]', type=int)
    parser.add_argument('-T', '--transport_model_api', action='store', dest='transport_model_api',
                        help=f'url of endpoint of transport model [default: {properties.transport_model_api_endpoint}]', type=str)
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
    if args.walking_threads is not None:
        walking_threads = args.walking_threads
    if args.transport_threads is not None:
        transport_threads = args.transport_threads
    
    with psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
            f' user={properties.db_user} password={properties.db_pass}') as conn:
        cur: psycopg2.extensions.cursor = conn.cursor()

        cur.execute('select walking from needs group by walking order by 1')
        walking_time = list(filter(lambda x: x != 0, map(lambda y: y[0], cur.fetchall())))

        cur.execute('select public_transport from needs group by public_transport order by 1')
        public_transport_time = list(filter(lambda x: x != 0, map(lambda y: y[0], cur.fetchall())))

        cur.execute(f'SELECT distinct ROUND(ST_X(ST_Centroid(geometry))::numeric, 3)::float, ROUND(ST_Y(ST_Centroid(geometry))::numeric, 3)::float FROM houses')
        houses: List[Tuple[float, float]] = list(map(lambda x: (x[0], x[1]), cur.fetchall()))

    if not os.path.isfile(SQLITE3_FILENAME):
        with open(SQLITE3_FILENAME, 'wb'):
            pass

    with sqlite3.connect(SQLITE3_FILENAME) as conn_sl3:
        cur_sl3 = conn_sl3.cursor()
        cur_sl3.execute(
            'create table if not exists walking ('
            '   latitude float not null,'
            '   longitude float not null,'
            '   time int not null,'
            '   geometry varchar,'
            '   primary key(latitude, longitude, time)'
            ')'
        )
        cur_sl3.execute(
            'create table if not exists transport ('
            '   latitude float not null,'
            '   longitude float not null,'
            '   time int not null,'
            '   geometry varchar,'
            '   primary key(latitude, longitude, time)'
            ')'
        )

    print(f'Using postgres connection: {properties.db_user}@{properties.db_addr}:{properties.db_port}/{properties.db_name}')

    saving = SavingThread(Queue(), Queue())
    saving.start()

    walking = WalkingThread(houses, walking_time, saving.walking_queue, walking_threads)
    transport = TransportThread(houses, public_transport_time, saving.transport_queue, transport_threads)

    try:
        walking.start()
        transport.start()
        
        walking.join()
        transport.join()
    except:
        print('Stopping threads')
        if walking.is_alive():
            walking.stop()
        if transport.is_alive():
            transport.stop()
        if saving.is_alive():
            saving.stop()

        if walking.is_alive():
            walking.join()
        if transport.is_alive():
            transport.join()
        if saving.is_alive():
            saving.join()
