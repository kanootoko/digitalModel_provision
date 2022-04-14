import threading
from typing import Any, Callable, Dict, List, Optional, Union

import psycopg2
import requests
from loguru import logger

# CREATE EXTENSION IF NOT EXISTS postgis;

# CREATE TABLE transport (
#   latitude numeric(8,6) NOT NULL,
#   longitude numeric(8,6) NOT NULL,
#   time int NOT NULL,
#   geometry geometry NOT NULL,
#   PRIMARY KEY(latitude, longitude, time)
# )

# CREATE TABLE car (
#   latitude numeric(8,6) NOT NULL,
#   longitude numeric(9,6) NOT NULL,
#   time int NOT NULL,
#   geometry geometry NOT NULL,
#   PRIMARY KEY(latitude, longitude, time)
# )

# CREATE TABLE walking (
#   latitude numeric(8,6) NOT NULL,
#   longitude numeric(9,6) NOT NULL,
#   time int NOT NULL,
#   geometry geometry NOT NULL,
#   PRIMARY KEY(latitude, longitude, time)
# )

try:
    import simplejson as json
except ModuleNotFoundError:
    import json  # type: ignore

def _execute_after(func: Callable[[], Any], log_text: str) -> threading.Thread:
    def f() -> None:
        logger.info(f'Launching {log_text} in thread {threading.get_ident()}')
        try:
            func()
        except Exception as ex:
            logger.error(f'Error on {log_text} in thread {threading.get_ident()}: {ex!r}')
        else:
            logger.info(f'Finished  {log_text} in thread {threading.get_ident()}')
    thread = threading.Thread(target=f)
    thread.start()
    return thread

def _get_public_transport_internal(latitude: float, longitude: float, t: Union[int, List[int]], conn: 'psycopg2.connection',
        public_transport_endpoint: str, _city: str, timeout: int = 240) -> Union[Dict[str, Any], Dict[int, Dict[str, Any]]]:
    if isinstance(t, int):
        times = [t]
    else:
        times = t
    result: Union[Dict[str, Any], Dict[int, Dict[str, Any]]] = {} # type: ignore
    with conn, conn.cursor() as cur:
        for t_cur in times:
            data = requests.post(public_transport_endpoint, timeout=timeout, headers={'Accept-encoding': 'gzip,deflat'}, json=
                {
                    'source': [longitude, latitude],
                    'cost': t * 60,
                    'day_time': 46800,
                    'mode_type': 'pt_cost'
                }
            ).json()
            if 'features' not in data:
                logger.error(f'Public transport download ({latitude}, {longitude}, {t}) failed: "features" is not found in data from transport model service'
                        f' ({public_transport_endpoint}\ndata:\n{data}')
                geom = {'type': 'Polygon', 'coordinates': []}
            if len(data['features']) == 0:
                logger.warning(f'Public transport download ({latitude}, {longitude}, {t}) : "features" is empty')
                geom = {'type': 'Polygon', 'coordinates': []}
            elif len(data['features']) > 1:
                cur.execute('SELECT ST_AsGeoJSON(ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')',
                        data['features'])) + ']), 4) LIMIT 1')
                geom = json.loads(cur.fetchone()[0]) # type: ignore
            else:
                geom = data['features'][0]['geometry']
            if isinstance(t, int):
                result = geom
            else:
                result[t_cur] = geom # type: ignore
            if len(geom['coordinates']) != 0:
                cur.execute('INSERT INTO transport (latitude, longitude, time, geometry) VALUES'
                        ' (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))'
                        ' ON CONFLICT (latitude, longitude, time) DO UPDATE SET geometry=excluded.geometry',
                        (latitude, longitude, t_cur, json.dumps(geom)))
    return result

def _get_transport_alternative_internal(latitude: float, longitude: float, t: Union[int, List[int]], conn: 'psycopg2.connection',
        transport_endpoint: str, city: str, timeout: int = 240) -> Union[Dict[str, Any], Dict[int, Dict[str, Any]]]:
    if isinstance(t, int):
        times = [t]
    else:
        times = t
    result: Union[Dict[str, Any], Dict[int, Dict[str, Any]]] = {} # type: ignore
    with conn, conn.cursor() as cur:
        for t_cur in times:
            data = requests.get(transport_endpoint.format(latitude=latitude, longitude=longitude, time=t_cur,
                    city=city), timeout=timeout, headers={'Accept-encoding': 'gzip,deflat'}).json()
            if 'features' not in data:
                logger.error(f'Public transport download ({latitude}, {longitude}, {t}) failed: "features" is not found in data from new transport model service'
                        f' ({transport_endpoint}\ndata:\n{data}')
                geom = {'type': 'Polygon', 'coordinates': []}
            if len(data['features']) == 0:
                logger.warning(f'Public transport download ({latitude}, {longitude}, {t}) : "features" is empty in new transport model')
                geom = {'type': 'Polygon', 'coordinates': []}
            elif len(data['features']) > 1:
                cur.execute('SELECT ST_AsGeoJSON(ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')',
                        data['features'])) + ']), 4) LIMIT 1')
                geom = json.loads(cur.fetchone()[0]) # type: ignore
            else:
                geom = data['features'][0]['geometry']
            if isinstance(t, int):
                result = geom
            else:
                result[t_cur] = geom # type: ignore
            if len(geom['coordinates']) != 0:
                cur.execute('INSERT INTO transport (latitude, longitude, time, geometry) VALUES'
                        ' (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))'
                        ' ON CONFLICT (latitude, longitude, time) DO UPDATE SET geometry=excluded.geometry',
                        (latitude, longitude, t_cur, json.dumps(geom)))
    return result

def _get_personal_transport_internal(latitude: float, longitude: float, t: Union[int, List[int]], conn: 'psycopg2.connection',
        personal_transport_endpoint: str, _city: str, timeout: int = 240) -> Union[Dict[str, Any], Dict[int, Dict[str, Any]]]:
    if isinstance(t, int):
        times = [t]
    else:
        times = t
    result: Union[Dict[str, Any], Dict[int, Dict[str, Any]]] = {} # type: ignore
    with conn, conn.cursor() as cur:
        for t_cur in times:
            data = requests.post(personal_transport_endpoint, timeout=timeout, headers={'Accept-encoding': 'gzip,deflat'}, json=
                {
                    'source': [longitude, latitude],
                    'cost': t * 60,
                    'day_time': 46800,
                    'mode_type': 'car_cost'
                }
            ).json()
            if 'features' not in data:
                logger.error(f'Personal transport download ({latitude}, {longitude}, {t}) failed: "features" is not found in data from transport model service'
                        f' ({personal_transport_endpoint}\ndata:\n{data}')
                geom = {'type': 'Polygon', 'coordinates': []}
            if len(data['features']) == 0:
                logger.warning(f'Personal transport download ({latitude}, {longitude}, {t}) : "features" is empty')
                geom = {'type': 'Polygon', 'coordinates': []}
            elif len(data['features']) > 1:
                logger.warning(f'Personal transport availability has more than 1 ({len(data["features"])}) poly: ({latitude}, {longitude}, {t_cur})')
                cur.execute('SELECT ST_AsGeoJSON(ST_UNION(ARRAY[' + ',\n'.join(map(lambda x: f'ST_GeomFromGeoJSON(\'{json.dumps(x["geometry"])}\')',
                        data['features'])) + '])) LIMIT 1')
                geom = json.loads(cur.fetchone()[0]) # type: ignore
            else:
                geom = data['features'][0]['geometry']
            if isinstance(t, int):
                result = geom
            else:
                result[t_cur] = geom # type: ignore
            if len(geom['coordinates']) != 0:
                cur.execute('INSERT INTO car (latitude, longitude, time, geometry) VALUES'
                        ' (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))'
                        ' ON CONFLICT (latitude, longitude, time) DO UPDATE SET geometry=excluded.geometry',
                        (latitude, longitude, t_cur, json.dumps(geom)))
    return result

def _get_walking_internal(latitude: float, longitude: float, t: Union[int, List[int]], conn: 'psycopg2.connection',
        walking_endpoint: str, city: str, timeout: int = 360, multiple_times_allowed: bool = False) -> Union[Dict[str, Any], Dict[int, Dict[str, Any]]]:
    if isinstance(t, int):
        times = [t]
    else:
        times = t
    result: Union[Dict[str, Any], Dict[int, Dict[str, Any]]]
    if multiple_times_allowed:
        if isinstance(t, int):
            result = requests.get(walking_endpoint.format(latitude=latitude, longitude=longitude, time=f'[{times[0]}]', city=city), timeout=timeout,
                    headers={'Accept-encoding': 'gzip,deflat'}).json()['features'][0]['geometry']
        else:
            result = {} # type: ignore
            for feature in requests.get(walking_endpoint.format(latitude=latitude, longitude=longitude, time=f'[{",".join((str(t_cur) for t_cur in times))}]', city=city),
                        timeout=timeout, headers={'Accept-encoding': 'gzip,deflat'}).json()['features']:
                result[feature['properties']['time']] = feature['geometry']
    else:
        if isinstance(t, int):
            result = requests.get(walking_endpoint.format(latitude=latitude, longitude=longitude, time=times[0], city=city), timeout=timeout,
                    headers={'Accept-encoding': 'gzip,deflat'}).json()['features'][0]['geometry']
        else:
            result = {} # type: ignore
            for t_cur in times:
                result[t_cur] = requests.get(walking_endpoint.format(latitude=latitude, longitude=longitude, time=t_cur, city=city), timeout=timeout, # type: ignore
                        headers={'Accept-encoding': 'gzip,deflat'}).json()['features'][0]['geometry']
    with conn, conn.cursor() as cur:
        if isinstance(t, int):
            cur.execute('INSERT INTO walking (latitude, longitude, time, geometry) VALUES'
                    ' (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))'
                    ' ON CONFLICT (latitude, longitude, time) DO UPDATE SET geometry=excluded.geometry',
                        (latitude, longitude, times[0], json.dumps(result)))
        else:
            for t_cur, geometry in result.items(): # type: ignore
                cur.execute('INSERT INTO walking (latitude, longitude, time, geometry) VALUES'
                        ' (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))'
                        ' ON CONFLICT (latitude, longitude, time) DO UPDATE SET geometry=excluded.geometry',
                        (latitude, longitude, t_cur, json.dumps(geometry)))
    return result

def get_public_transport(latitude: float, longitude: float, t: int, conn: 'psycopg2.connection',
        public_transport_endpoint: str, city: Optional[str] = None, timeout: int = 20, raise_exceptions: bool = False,
        get_public_transport_internal: Callable[[float, float, Union[int, List[int]], 'psycopg2.connection', str, str, int],
                Union[Dict[str, Any], Dict[int, Dict[str, Any]]]] = _get_public_transport_internal,
        download_geometry_after_timeout: bool = False) -> Dict[str, Any]:
    latitude, longitude = round(latitude, 6), round(longitude, 6)
    with conn, conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(geometry) FROM transport WHERE latitude = %s AND longitude = %s AND time = %s', (latitude, longitude, t))
        res = cur.fetchone()
        if res is not None:
            return json.loads(res[0])
    try:
        return get_public_transport_internal(latitude, longitude, t, conn, public_transport_endpoint, city or '', timeout) # type: ignore
    except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as ex:
        if download_geometry_after_timeout:
            _execute_after(lambda: _get_public_transport_internal(latitude, longitude, t, conn, public_transport_endpoint, city or '', timeout * 20),
                    f'public_transport_download ({latitude}, {longitude}, {t})')
        if raise_exceptions:
            raise TimeoutError(ex)
        else:
            logger.warning(f'Public transport geometry download ({latitude}, {longitude}, {t}) failed with timeout')
        with conn, conn.cursor() as cur:
            cur.execute('SELECT ST_AsGeoJSON(geometry) FROM transport WHERE time = %s'
                    ' ORDER BY ST_Distance(geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) LIMIT 1', (latitude, longitude, t))
            res = cur.fetchone()
        if res is None:
            return {'type': 'Polygon', 'coordinates': []}
        return json.loads(res[0])
    except Exception as ex:
        if raise_exceptions:
            raise
        logger.error(f'Public transport download ({latitude}, {longitude}, {t}) failed (exception): {ex!r}')
        return {'type': 'Polygon', 'coordinates': []}

def get_personal_transport(latitude: float, longitude: float, t: int, conn: 'psycopg2.connection',
        personal_transport_endpoint: str, city: Optional[str] = None, timeout: int = 20, raise_exceptions: bool = False,
        get_personal_transport_internal: Callable[[float, float, Union[int, List[int]], 'psycopg2.connection', str, str, int],
                Union[Dict[str, Any], Dict[int, Dict[str, Any]]]] = _get_personal_transport_internal,
        download_geometry_after_timeout: bool = False) -> Dict[str, Any]:
    latitude, longitude = round(latitude, 6), round(longitude, 6)
    with conn, conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(geometry) FROM car WHERE latitude = %s AND longitude = %s AND time = %s', (latitude, longitude, t))
        res = cur.fetchone()
        if res is not None:
            return json.loads(res[0])
    try:
        return get_personal_transport_internal(latitude, longitude, t, conn, personal_transport_endpoint, city or '', timeout) # type: ignore
    except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as ex:
        if download_geometry_after_timeout:
            if download_geometry_after_timeout:
                _execute_after(lambda: _get_personal_transport_internal(latitude, longitude, t, conn, personal_transport_endpoint, city or '', timeout * 20),
                        f'personal_transport_download ({latitude}, {longitude}, {t})')
        if raise_exceptions:
            raise TimeoutError(ex)
        else:
            logger.warning(f'Personal transport geometry download ({latitude}, {longitude}, {t}) failed with timeout')
        with conn, conn.cursor() as cur:
            cur.execute('SELECT ST_AsGeoJSON(geometry) FROM car WHERE time = %s'
                    ' ORDER BY ST_Distance(geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) LIMIT 1', (latitude, longitude, t))
            res = cur.fetchone()
        if res is None:
            return {'type': 'Polygon', 'coordinates': []}
        return json.loads(res[0])
    except Exception as ex:
        logger.error(f'Personal transport download ({latitude}, {longitude}, {t}) failed (exception): {ex!r}')
        if raise_exceptions:
            raise
        return {'type': 'Polygon', 'coordinates': []}

def get_walking(latitude: float, longitude: float, t: int, conn: 'psycopg2.connection', walking_endpoint: str,
        city: Optional[str] = None, timeout: int = 20, multiple_times_allowed: bool = False, raise_exceptions: bool = False,
        download_geometry_after_timeout: bool = False) -> Dict[str, Any]:
    latitude, longitude = round(latitude, 6), round(longitude, 6)
    with conn.cursor() as cur:
        cur.execute('SELECT ST_AsGeoJSON(geometry) FROM walking WHERE latitude = %s AND longitude = %s AND time = %s LIMIT 1', (latitude, longitude, t))
        res = cur.fetchone()
        if res is not None:
            return json.loads(res[0])
    try:
        return _get_walking_internal(latitude, longitude, t, conn, walking_endpoint, city or '', timeout, multiple_times_allowed) # type: ignore
    except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as ex:
        if download_geometry_after_timeout:
            thread = threading.Thread(target=lambda: _get_walking_internal(latitude, longitude, t, conn, walking_endpoint, city or '', timeout * 20))
            thread.start()
        if raise_exceptions:
            raise TimeoutError(ex)
        else:
            logger.warning(f'Walking geometry download ({latitude}, {longitude}, {t}) failed with timeout')
        return {'type': 'Polygon', 'coordinates': []}
    except Exception as ex:
        if raise_exceptions:
            raise
        else:
            logger.warning(f'Walking geometry download ({latitude}, {longitude}, {t}) failed with exception: {ex!r}')
        logger.error(f'Walking geometry download for ({latitude}, {longitude}, {t}) failed: {ex!r}')
        with conn, conn.cursor() as cur:
            cur.execute('SELECT ST_AsGeoJSON(geometry), ST_Distance(geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) AS min_distance'
                    'FROM walking WHERE time = %s ORDER BY 2 LIMIT 1', (latitude, longitude, t))
            res = cur.fetchone()
        if res is None:
            return {'type': 'Polygon', 'coordinates': []}
        return json.loads(res[0])

class CollectGeometry:
    def __init__(self, conn: 'psycopg2.connection', public_transport_endpoint: str,
            personal_transport_endpoint: str, walking_endpoint: str, walking_endpoint_allow_multiple_times: bool = False,
            timeout: int = 20, raise_exceptions: bool = False, download_geometry_after_timeout: bool = False,
            get_public_transport_func: Callable[
                    [float, float, int, 'psycopg2.connection', str, Optional[str], int, bool,
                        Callable[[float, float, Union[int, List[int]], 'psycopg2.connection', str, str, int],
                            Union[Dict[str, Any], Dict[int, Dict[str, Any]]]
                        ],
                    bool],
                Dict[str, Any]] = get_public_transport,
            get_personal_transport_func: Callable[
                    [float, float, int, 'psycopg2.connection', str, Optional[str], int, bool,
                        Callable[[float, float, Union[int, List[int]], 'psycopg2.connection', str, str, int],
                            Union[Dict[str, Any], Dict[int, Dict[str, Any]]]
                        ],
                    bool],
                Dict[str, Any]] = get_personal_transport,
            get_walking_func: Callable[[float, float, int, 'psycopg2.connection', str, Optional[str], int, bool, bool, bool], Dict[str, Any]] = get_walking,
            use_alternative_public_transport: bool = False, use_alternative_personal_transport: bool = False
        ):
        self.conn = conn
        self.public_transport_endpoint = public_transport_endpoint
        self.personal_transport_endpoint = personal_transport_endpoint
        self.walking_endpoint = walking_endpoint
        self.walking_endpoint_allow_multiple_times = walking_endpoint_allow_multiple_times
        self.timeout = timeout
        self.raise_exceptions = raise_exceptions
        self.download_geometry_after_timeout = download_geometry_after_timeout
        self.get_personal_transport_func = get_personal_transport_func
        self.get_public_transport_func = get_public_transport_func
        self.get_walking_func = get_walking_func
        if use_alternative_public_transport:
            self.public_transport_internal = _get_transport_alternative_internal
        else:
            self.public_transport_internal = _get_public_transport_internal # type: ignore
        if use_alternative_personal_transport:
            self.personal_transport_internal = _get_transport_alternative_internal
        else:
            self.personal_transport_internal = _get_personal_transport_internal # type: ignore

    def get_walking(self, latitude: float, longitude: float, t: int, city: Optional[str] = None) -> Dict[str, Any]:
        return self.get_walking_func(latitude, longitude, t, self.conn, self.walking_endpoint, city, self.timeout,
                self.walking_endpoint_allow_multiple_times, self.raise_exceptions, self.download_geometry_after_timeout)

    def get_public_transport(self, latitude: float, longitude: float, t: int, city: Optional[str] = None) -> Dict[str, Any]:
        return self.get_public_transport_func(latitude, longitude, t, self.conn, self.public_transport_endpoint,
                city, self.timeout, self.raise_exceptions, self.public_transport_internal, self.download_geometry_after_timeout)

    def get_personal_transport(self, latitude: float, longitude: float, t: int, city: Optional[str] = None) -> Dict[str, Any]:
        return self.get_personal_transport_func(latitude, longitude, t, self.conn, self.personal_transport_endpoint,
                city, self.timeout, self.raise_exceptions, self.personal_transport_internal, self.download_geometry_after_timeout)

# walking_urbica = 'https://galton.urbica.co/api/foot/?lng={x}&lat={y}&radius=5&cellSize=0.1&intervals={t}'
# walking_local = 'http://10.32.1.65:5000/mobility_analysis/isochrones?x_from={latitude}&y_from={longitude}&travel_type=walk&times={time}&city={city}'

# public_transport_new = 'http://10.32.1.65:5000/mobility_analysis/isochrones?x_from={latitude}&y_from={longitude}&travel_time={time}&city={city}&travel_type=public_transport'
# personal_transport_new = 'http://10.32.1.65:5000/mobility_analysis/isochrones?x_from={latitude}&y_from={longitude}&travel_time={time}&city={city}&travel_type=personal_transport'

# public_transport_local = 'http://10.32.1.61:8080/api.v2/isochrones'
# personal_transport_local = 'http://10.32.1.61:8080/api.v2/isochrones'
