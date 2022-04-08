# Provision API Server

## Description

This is API server for calculating provision values (atomic and agregated) based on given city functions,
  living situations, social groups and data from postgres database with houses
  
## Preparation before launching (both Docker and host machine)

1. install postgres DBMS and postgis extension
2. use `city_db_final` database scheme with data, including `provision` schema
3. use `provision` database with at least table "transport"
4. install python3 (tested on python 3.10)
5. clone this repository
6. launch `pip3 install -r requirements.txt`

## Launching on host machine

1. open terminal in cloned repository
2. configure database and service parameters with environment variables or add parameters to next command
3. run with `python provision_api.py`

## Configuration by environment variables

Parameters can be configured with environment variables:

* PROVISION_API_PORT - api_port - port to run the api server [default: _80_]
* HOUSES_DB_ADDR - houses_db_addr - address of the postgres with houses [default: _localhost_] (string)
* HOUSES_DB_PORT - houses_db_port - port of the postgres with houses [default: _5432_] (int)
* HOUSES_DB_NAME - houses_db_name - name of the postgres database with houses [default: _city\_db\_final_] (string)
* HOUSES_DB_USER - houses_db_user - user name for database with houses [default: _postgres_] (string)
* HOUSES_DB_PASS - houses_db_pass - user password for database with houses [default: _postgres_] (string)
* PROVISION_DB_ADDR - provision_db_addr - address of the postgres with provision [default: _localhost_] (string)
* PROVISION_DB_PORT - provision_db_port - port of the postgres with provision [default: _5432_] (int)
* PROVISION_DB_NAME - provision_db_name - name of the postgres database with provision [default: _provision_] (string)
* PROVISION_DB_USER - provision_db_user - user name for database with provision [default: _postgres_] (string)
* PROVISION_DB_PASS - provision_db_pass - user password for database with provision [default: _postgres_] (string)
* PROVISION_DEFAULT_CITY - default_city - name of a city to work with by default
* PROVISION_MONGO_URL - mongo_url - optional url to mongo database to write logs in "logs" collection
* PROVISION_DISABLE_DB_ENDPOINTS - no_db_endpoints - set to any value except "0", "f", "false" or "no" to disable /api/db/... endpoints group

## Configuration by CLI Parameters

Command line arguments configuration is also avaliable (overrides environment variables configuration)

* -p,--port \<int\> - api_port
* -hH,--houses_db_addr \<str\> - houses_db_addr
* -hP,--houses_db_port \<int\> - houses_db_port
* -hN,--houses_db_name \<str\> - houses_db_name
* -hU,--houses_db_user \<str\> - houses_db_user
* -hW,--houses_db_pass \<str\> - houses_db_pass
* -pH,--provision_db_addr \<str\> - provision_db_addr
* -pP,--provision_db_port \<int\> - provision_db_port
* -pN,--provision_db_name \<str\> - provision_db_name
* -pU,--provision_db_user \<str\> - provision_db_user
* -pW,--provision_db_pass \<str\> - provision_db_pass
* -C,--default_city \<str\> - default_city
* -m,--mongo_url \<str\> - mongo_url
* -nDE,--no_db_endpoints - no_db_endpoints
* -D,--debug - launch in debug mode (available only by CLI)

## Building Docker image (the other way is to use Docker repository: kanootoko/digitalmodel_provision:2022-04-08)

1. open terminal in cloned repository
2. run `git submodule update --init` to download module for /api/db/... endpoints group
3. (temporary) open submodule directory and change branch to the latest version: `cd df_saver_cli && git checkout kanootoko && cd ..`
4. build image with `docker build --tag kanootoko/digitalmodel_provision:2022-04-08 .`
5. run image with postgres server running on host machine on default port 5432
    1. For windows: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=host.docker.internal -e PROVISION_DB_ADDR=host.docker.internal --name provision_api kanootoko/digitalmodel_provision:2022-04-08`
    2. For Linux: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) -e PROVISION_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) --name provision_api kanootoko/digitalmodel_provision:2022-04-08`  
      Ensure that:
        1. _/etc/postgresql/\<version\>/main/postgresql.conf_ contains uncommented setting `listen_addresses = '*'` so app could access postgres from Docker network
        2. _/etc/postgresql/\<version\>/main/pg\_hba.conf_ contains `host all all 0.0.0.0/0 md5` so login could be performed from anywhere (you can set docker container address instead of 0.0.0.0)
        3. command `ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1` returns ip address  
        If config files are not found, `sudo -u postgres psql -c 'SHOW config_file'` should say where they are

## Usage

After the launch you can find api avaliable at localhost:port/ . In example given it will be localhost with port 8080.

## Endpoints

Endpoints are documented in russian at [documentation](documentation.docx).  
  
At this moment there are endpoints:

* **/api**: returns HAL description of API provided.
* **/api/provision_v3/ready**: returns the list of calculated service types with the number of them.
* **/api/provision_v3/services**: returns the list of conctere services with their provision evaluation. Takes `service` and `location` as optional parameters.  
  `service` can be one of the services calculated (by name or by id), `location` is a district or municipality by full or short name.
* **/api/provision_v3/service/{service_id}**: returns the provision evaluation of a given service. If not found, service name = "Not found" and response status is 404.
* **/api/provision_v3/service/{service_id}/houses**: returns the list of houses that contain the given service in their normaive availability zone.
* **/api/provision_v3/service/{service_id}/availability_zone**: returns the geometry of availability zone of the service by its normatives.
* **/api/provision_v3/houses**: returns the list of houses with their services provision. At least one of the `service` and `location` parameters must be set.
  `location` can be municipality or district given as short or full name, `service` - service type given by id or by name.  
  `everything` parameter must be set to get all houses information in the city.
* **/api/provision_v3/house/{house_id}**: returns the service types provision evaluation of a given house. If house is not found, address = "Not found" and
  response status is 404. `service` can be set by name or id to get information of ont particular service type.
* **/api/provision_v3/house/{house_id}/services**: returns the list of services that are contained by the given living house's normative availability zones.
* **/api/provision_v3/house/{house_id}/availability_zone**: returns the geometry of availability zone around the house for the given service type.
* **/api/provision_v3/prosperity/{districts,municipalities,blocks}**: returns the prosperity value of administrative units, municipalities or blocks.
  Takes `social_group`, `service_type`/`city_function`/`infrastructure`, `district`/`municipality`/`block` and `provision_only` as optional parameters.  
  If location is set, returns prosperity of municipalities of a given location, default - all of them. Option `mean` will return an average value.  
  If social group is set, returns prosperity only of a given social group, default - all of them. Option `mean` will return an average value.  
  If service type / city function / infrastructure type is set, returns prosperity only of a given choice, default - all of them. Option `mean`
    will return an average value.  
  If `provision_only` is set, skips prosperity, significance and social_group in result.
* **/api/list/social_groups**: returns list of social groups. If you specify a city_function and/or living_situation, only relative social groups will be returned.
* **/api/list/city_functions**: returns a list of city functions. If you specify a social_group and/or living_situation, only relative city functions will be returned.
* **/api/list/service_types**: returns a list of service types. If you specify a social_group and/or living_situation, only relarive services will be returned.
* **/api/list/living_situations**: returns a list of living situations. If you specify a social_group and/or city_function,
  only relative living situations will be returned.
* **/api/relevance/social_groups**: returns a list of social groups. If you specify city_function as a parameter, the output will be limited to social groups
  relevant to this city function, and significane will be returned for each of them. If you specify both city_function and living_situation, then
  intensity will be returned too.
* **/api/relevance/service_types**: returns a list of service types available. If you specify social_group as a parameter, output will be limited to service types
  relevant to this social group, and significane will be returned for each of them. If you specify both social_group and living_situation, then
  intensity will be returned too.
* **/api/relevance/city_functions**: returns a list of city functions available. If you specify social_group as a parameter, output will be limited to city functions
  relevant to this social group, and significane will be returned for each of them. If you specify both social_group and living_situation, then
  intensity will be returned too.
* **/api/relevance/living_situations**: returns a list of living situations. If you specify social_group, the output will be limited to living situations relative
  to the given social group and intensity will be returned for each of them. If the city_function parameter is also specified, significance will be returned
  in params section.
* **/api/list/infrastructures**: returns a hierarchy of infrastructure types with city functions list for each of them and with service types list for each city funtion.
* **/api/list/districts**: returns a list of administrative units.
* **/api/list/municipalities**: returns a list of municipalities.

Every endpoint that takes `social_group`, `city_function`, `living_situation`, `service_type`, `infrastructure`, `district` or `municipality` as parameters can
  work with all of: names, codes or database id.
