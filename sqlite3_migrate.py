import argparse
import psycopg2, sqlite3
import time

class Properties:
    def __init__(self, db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str):
        self.db_addr = db_addr
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass

if __name__ == '__main__':
    
    # Default properties settings

    properties = Properties('localhost', 5432, 'provision', 'postgres', 'postgres')
    sqlite3_filename = 'geometry.sqlite3'
    skip_walking = False
    skip_transport = False
    skip_car = False

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
    parser.add_argument('-f', '--sqlite3_filename', action='store', dest='sqlite3_filename',
                        help=f'path to sqlite3 database to migrate from [default: {sqlite3_filename}]', type=str)
    parser.add_argument('-nW', '--no_walking', action='store_true', dest='no_walking',
                        help=f'skip walking data migration')
    parser.add_argument('-nT', '--no_transport', action='store_true', dest='no_transport',
                        help=f'skip transport data migration')
    parser.add_argument('-nC', '--no_car', action='store_true', dest='no_car',
                        help=f'skip car data migration')
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
    if args.sqlite3_filename is not None:
        sqlite3_filename = args.sqlite3_filename
    if args.no_walking is not None:
        skip_walking = args.no_walking
    if args.no_transport is not None:
        skip_transport = args.no_transport
    if args.no_car is not None:
        skip_car = args.no_car
    
    done_walking = 0
    done_transport = 0
    done_car = 0
    with psycopg2.connect(f'host={properties.db_addr} port={properties.db_port} dbname={properties.db_name}'
            f' user={properties.db_user} password={properties.db_pass}') as conn:
        with sqlite3.connect(sqlite3_filename) as conn_sl3:
            cur: psycopg2.extensions.cursor = conn.cursor()
            cur_sl3 = conn_sl3.cursor()
            cur.execute(
                'CREATE EXTENSION if not exists postgis'
            )
            for table_name in ('walking', 'transport', 'car'):
                cur.execute(
                    f'CREATE TABLE if not exists {table_name} ('
                    '   latitude float not null,'
                    '   longitude float not null,'
                    '   time int not null,'
                    '   geometry geometry,'
                    '   primary key(latitude, longitude, time)'
                    ')'
                )
            cur.execute(
                'CREATE TABLE if not exists aggregation_municipality ('
                '   id serial NOT NULL,'
                '   living_situation_id integer references living_situations(id),'
                '   social_group_id integer references social_groups(id),'
                '   city_function_id integer references city_functions(id),'
                '   municipality_id integer references municipalities(id) NOT NULL,'
                '   avg_intensity float NOT NULL,'
                '   avg_significance float NOT NULL,'
                '   avg_provision float NOT NULL,'
                '   time_done timestamp NOT NULL,'
                '   UNIQUE(living_situation_id, social_group_id, city_function_id, municipality_id)'
                ')'
            )
            cur.execute(
                'CREATE TABLE if not exists aggregation_district ('
                '   id serial NOT NULL,'
                '   living_situation_id integer references living_situations(id),'
                '   social_group_id integer references social_groups(id),'
                '   city_function_id integer references city_functions(id),'
                '   district_id integer references districts(id) NOT NULL,'
                '   avg_intensity float NOT NULL,'
                '   avg_significance float NOT NULL,'
                '   avg_provision float NOT NULL,'
                '   time_done timestamp NOT NULL,'
                '   UNIQUE(living_situation_id, social_group_id, city_function_id, district_id)'
                ')'
            )
            cur.execute(
                'CREATE TABLE if not exists aggregation_house ('
                '   id serial NOT NULL,'
                '   living_situation_id integer references living_situations(id),'
                '   social_group_id integer references social_groups(id),'
                '   city_function_id integer references city_functions(id),'
                '   latitude float NOT NULL,'
                '   longitude float NOT NULL,'
                '   avg_intensity float NOT NULL,'
                '   avg_significance float NOT NULL,'
                '   avg_provision float NOT NULL,'
                '   time_done timestamp NOT NULL,'
                '   UNIQUE(living_situation_id, social_group_id, city_function_id, latitude, longitude)'
                ')'
            )
            cur.execute(
                'CREATE TABLE if not exists atomic ('
                '   id serial NOT NULL,'
                '   latitude float NOT NULL,'
                '   longitude float NOT NULL,'
                '   walking int NOT NULL,'
                '   transport int NOT NULL,'
                '   intensity int NOT NULL,'
                '   significance int NOT NULL,'
                '   provision_value float NOT NULL'
                ')'
            )
            start_time = time.time()
            if not skip_walking:
                print('Migrating walking')
                cur_sl3.execute('select latitude, longitude, time, geometry from walking')
                for lat, lan, t, geom in cur_sl3:
                    cur.execute('insert into walking (latitude, longitude, time, geometry) values (%s, %s, %s, ST_GeomFromGeoJSON(%s)) ON CONFLICT DO NOTHING', (lat, lan, t, geom))
                    if done_walking % 1000 == 0:
                        print(f'Walking in progress: {done_walking}')
                    done_walking += 1
            if not skip_transport:
                print('Migrating transport')
                cur_sl3.execute('select latitude, longitude, time, geometry from transport')
                for lat, lan, t, geom in cur_sl3:
                    if geom == 'null':
                        continue
                    cur.execute('insert into transport (latitude, longitude, time, geometry) values (%s, %s, %s, ST_GeomFromGeoJSON(%s)) ON CONFLICT DO NOTHING', (lat, lan, t, geom))
                    if done_transport % 1000 == 0:
                        print(f'Transport in progress: {done_transport}')
                    done_transport += 1
            if not skip_car:
                print('Migrating car')
                cur_sl3.execute('select latitude, longitude, time, geometry from car')
                for lat, lan, t, geom in cur_sl3:
                    if geom == 'null':
                        continue
                    cur.execute('insert into car (latitude, longitude, time, geometry) values (%s, %s, %s, ST_GeomFromGeoJSON(%s)) ON CONFLICT DO NOTHING', (lat, lan, t, geom))
                    if done_car % 1000 == 0:
                        print(f'car in progress: {done_car}')
                    done_car += 1
                    
    print(f'Totally migrated walking ({done_walking}), transport ({done_transport}) and car ({done_car}) in {time.time() - start_time:.2f} seonds')