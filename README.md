# Provision API Server

## Description

This is API server for calculating provision values (atomic and agregated) based on given city functions,
  living situations, social groups and data from postgres database with houses
  
## Preparation before launching (both Docker and host machine)

1. install postgres database and postgis extension
2. fill database with city data (`houses` matview and `social_groups`, `city_functions`, `living_situations`,
  `municipalities`, `districts` tables are used)
3. install python3 (3.8 recommended) and modules: flask, flask_compress, psycopg2, pandas, numpy, requests
4. clone this repository
5. download geometry for current houses ([collect_geometry_help.md](collect_geometry_help.md))

## Launching on host machine

1. open terminal in cloned repository
2. run with `python provision_api.py`

## Configuration by environment variables

Parameters can be configured with environment variables:

* PROVISION_API_PORT - api_port - port to run the api server [default: _80_]
* PROVISION_DB_ADDR - db_addr - address of the postgres with provision [default: _localhost_] (string)
* PROVISION_DB_PORT - db_port - port of the postgres with provision [default: _5432_] (int)
* PROVISION_DB_NAME - db_name - name of the postgres database with provision [default: _provision_] (string)
* PROVISION_DB_USER - db_user - user name for database [default: _postgres_] (string)
* PROVISION_DB_PASS - db_pass - user password for database [default: _postgres_] (string)
* TRANSPORT_MODEL_ADDR - tranaport_model_endpoint - address of the transport model endpoint [default: _http://10.32.1.61:8080/api.v2/isochrones_]
* PROVISION_SKIP_AGGREGATION - skip calculation of provision aggregation (trigger)

## Configuration by CLI Parameters

Command line arguments configuration is also avaliable (overrides environment variables configuration)

* -p,--port \<int\> - api_port
* -H,--db_addr \<str\> - db_addr
* -P,--db_port \<int\> - db_port
* -N,--db_name \<str\> - db_name
* -U,--db_user \<str\> - db_user
* -W,--db_pass \<str\> - db_pass
* -T,--transport_model_endpoint \<str\>
* -S,--skip_aggregation - skip_evaluation

## Building Docker image (the other way is to use Docker repository: kanootoko/digitalmodel_provision:2020-10-01)

1. open terminal in cloned repository
2. build image with `docker build --tag kanootoko/digitalmodel_provision:2020-10-01 .`
3. run image with postgres server running on host machine on default port 5432
    1. For windows: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=host.docker.internal -e PROVISION_DB_ADDR=host.docker.internal --name provision_api kanootoko/digitalmodel_provision:2020-10-01`
    2. For Linux: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) -e PROVISION_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) --name provision_api kanootoko/digitalmodel_provision:2020-10-01`  
      Ensure that:
        1. _/etc/postgresql/12/main/postgresql.conf_ contains uncommented setting `listen_addresses = '*'` so app could access postgres from Docker network
        2. _/etc/postgresql/12/main/pg_hba.conf_ contains `host all all 0.0.0.0/0 md5` so login could be performed from anywhere (you can set docker container address instead of 0.0.0.0)
        3. command `ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1` returns ip address  
        If config files are not found, `sudo -u postgres psql -c 'SHOW config_file'` should say where they are

## Usage

After the launch you can find api avaliable at localhost:port/ . In example given it will be localhost with port 8080.  
For a normal usage you will need a working transport model avaliable.

## Endpoints

At this moment there are some main endpoints:

* **/api**: returns HAL description of API provided.
* **/api/provision/atomic**: takes parameters by query. You must set `soc_group` for social group,
  `function` for city function, `situation` for living situation and `point` for coordinates of the house.
  Point format is `latitude,longitude`.
* **/api/provision/aggregated**: takes parameters by query. You should set at least something in: `soc_group` for social group,
  `function` for city function, `situation` for living situation and `region` for district and `municipality` for municipality.
* **/api/list/social_groups**: returns list of social groups
* **/api/list/city_functions**: returns list of city functions available (also can take social_group as parameter to return only living situations valued by this group)
* **/api/list/living_situations**: returns list of city functions
* **/api/list/regions**: returns list of districts
* **/api/list/municipalities**: returns list of municipalities

### /api

Output format:

```json
{
  "_links": {
    "aggregated-provision": {
      "href": "/api/provision/aggregated{?soc_group,situation,function,region,municipality,"
    },
    "atomic_provision": {
      "href": "/api/provision/atomic{?soc_group,situation,function,point}",
      "templated": true
    },
    "get-houses": {
      "href": "/api/houses{?firstPoint,secondPoint}",
      "templated": true
    },
    "list-city_functions": {
      "href": "/api/list/city_functions{?soc_group}",
      "templated": true
    },
    "list-living_situations": {
      "href": "/api/list/living_situations"
    },
    "list-municipalities": {
      "href": "/api/list/municipalities"
    },
    "list-regions": {
      "href": "/api/list/regions"
    },
    "list-social_groups": {
      "href": "/api/list/social_groups"
    },
    "self": {
      "href": "/api"
    }
  },
  "version": ":version"
}
```

:version - string representing date in format "YYYY-MM-DD"

### /api/list/social_groups

Output format:

```json
{
  "_embedded": {
    "social_groups": [
      "Младенцы (0-1)",
      "Дети до-детсадовского возраста (1-3)",
      "Дети до-дошкольного возраста (3-7)",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/social_groups"
    }
  }
}
```

### /api/list/city_functions

Output format:

```json
{
  "_embedded": {
    "city_functions": [
      "Жилье",
      "Мусор",
      "Перемещение по городу",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/city_functions"
    }
  }
}
```

### /api/list/living_situations

Output format:

```json
{
  "_links": {
    "self": {
      "href": "/api/list/living_situations"
    }
  },
  "_embedded": {
    "living_situations": [
      "Типичный нерабочий день",
      "Типичный рабочий день",
      "Свидание",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/living_situations"
    }
  }
}
```

### /api/list/regions

Output format:

```json
{
  "_embedded": {
    "regions": [
      "Выборгский район",
      "Петродворцовый район",
      "Приморский район",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/regions"
    }
  }
}
```

### /api/list/municipalities

Output format:

```json
{
  "_embedded": {
    "municipalities": [
      "поселок Смолячково",
      "поселок Молодежное",
      "муниципальный округ Адмиралтейский округ",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/municipalities"
    }
  }
}
```

### /api/provision/atomic

Output format:

```json
{
  "_embedded": {
    "parameters": {
      "intensity": ":intensity",
      "personal_transport_time_cost": ":personal_cost",
      "significance": ":significance",
      "transport_time_cost": ":transport_cost",
      "walking_time_cost": ":walking_cost"
    },
    "provision_result": ":provision_result",
    "services": {
      "аптека": [
        {
          "availability": ":service_availability",
          "point": [
            ":service_latitude",
            ":service_longitude"
          ],
          "power": ":service_power",
          "service_id": ":service_id",
          "service_name": ":service_name",
          "transport_dist": ":service_transport_bool",
        },
        <...>
      ]
    },
    "walking_geometry": {
        <geojson>
    },
    "transport_geometry": {
        <geojson>
    }
```

:provision_result - float from 0.0 to 5.0
:intensity - integer from 1 to 10
:significance, :service_availability - float from 0.0 to 1.0
:personal_cost, :transport_cost, :walking_cost - integers representing minutes
:service_id - integer
:service_name - string
:service_latitude, :service_longitude - float representing coordinates
:service_power - integer from 1 to 5
:service_transport_bool - boolean, 1 if service is avaliable by transport but not by walking
