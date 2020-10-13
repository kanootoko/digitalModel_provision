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
* TRANSPORT_MODEL_ADDR - tranaport_model_endpoint - address of the transport model endpoint [default: _<http://10.32.1.61:8080/api.v2/isochrones>_]
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

## Building Docker image (the other way is to use Docker repository: kanootoko/digitalmodel_provision:2020-10-13)

1. open terminal in cloned repository
2. build image with `docker build --tag kanootoko/digitalmodel_provision:2020-10-13 .`
3. run image with postgres server running on host machine on default port 5432
    1. For windows: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=host.docker.internal -e PROVISION_DB_ADDR=host.docker.internal --name provision_api kanootoko/digitalmodel_provision:2020-10-13`
    2. For Linux: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) -e PROVISION_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) --name provision_api kanootoko/digitalmodel_provision:2020-10-13`  
      Ensure that:
        1. _/etc/postgresql/12/main/postgresql.conf_ contains uncommented setting `listen_addresses = '*'` so app could access postgres from Docker network
        2. _/etc/postgresql/12/main/pg_hba.conf_ contains `host all all 0.0.0.0/0 md5` so login could be performed from anywhere (you can set docker container address instead of 0.0.0.0)
        3. command `ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1` returns ip address  
        If config files are not found, `sudo -u postgres psql -c 'SHOW config_file'` should say where they are

## Usage

After the launch you can find api avaliable at localhost:port/ . In example given it will be localhost with port 8080.  
For a normal usage you will need a working transport model avaliable.

## Endpoints

At this moment there are endpoints:

* **/api**: returns HAL description of API provided.
* **/api/provision/atomic**: returns atomic provision value, walking, public transport and personal transport availability geometry,
  and services inside of them. Takes parameters by query. You must set `soc_group` for social group,
  `function` for city function, `situation` for living situation and `point` for coordinates of the house.
  Point format is `latitude,longitude`.
* **/api/provision/aggregated**: returns the aggregated provision value. Takes parameters by query. You should set at least something in: `soc_group` for social group,
  `function` for city function, `situation` for living situation and `region` for district and `municipality` for municipality.
* **/api/provision/ready/regions**: returns the list of already aggregated by districts provision values.
  Takes parameters by query. You can set `soc_group`, `function`, `situation` or `district` parameter to specify the request.
* **/api/provision/ready/municipalities**: returns the list of already aggregated by municipalities provision values.
  Takes parameters by query. You can set `soc_group`, `function`, `situation` or `municipality` parameter to specify the request.
* **/api/list/social_groups**: returns list of social groups (also can take function as a parameter to return only social_groups which value given city function)
* **/api/list/city_functions**: returns list of city functions available (also can take social_group as a parameter to return only living situations valued by this group)
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
    "car_geometry": {
      <geojson>
    },
    "parameters": {
      "intensity": ":intensity",
      "personal_transport_time_cost": ":personal_cost",
      "significance": ":significance",
      "transport_time_cost": ":transport_cost",
      "walking_time_cost": ":walking_cost"
    },
    "provision_result": ":provision_result",
    "services": {
      "service_type": [
        {
          "address": "service_address",
          "availability": ":service_availability",
          "point": [
            ":service_latitude",
            ":service_longitude"
          ],
          "power": ":service_power",
          "service_id": ":service_id",
          "service_name": ":service_name",
          "availability_type": ":service_availability_type"
        },
        <...>
      ]
    },
    "walking_geometry": {
        <geojson>
    },
    "transport_geometry": {
        <geojson>
    },
    "_links": {
      "self": {
        "href": "/api/provision/atomic"
    }
  }
```

:provision_result - float from 0.0 to 5.0  
:intensity, :significance - integer from 1 to 10  
:service_availability - float from 0.0 to 1.0  
:personal_cost, :transport_cost, :walking_cost - integers representing minutes  
:service_type - string, type of service representing city function  
:service_id - integer  
:service_address, :service_name - string  
:service_latitude, :service_longitude - float representing coordinates  
:service_power - integer from 1 to 5  
:service_availability_type - string, one of the "walking", "transport" or "car"

### /api/provision/aggregated

Output format:

```json
{
  "_embedded": {
    "params": {
      "function": ":function",
      "region": ":district",
      "situation": ":situation",
      "soc_group": ":soc_group"
    },
    "result": {
      "intensity": ":intensity",
      "provision": ":provision",
      "significance": ":significance",
      "time_done": ":time_done"
    }
  },
  "_links": {
    "self": {
      "href": "/api/provision/aggregated"
    }
  }
}
```

:function, :district, :situation, :soc_group - string  
:intensity, :significance - integer from 1 to 10  
:provision - float from 0.0 to 5.0  
:time_done - time when aggregation was completed

### /api/provision/ready/regions

Output format:

```json
{
  "_embedded": {
    "params": {
      "function": ":function",
      "region": ":district",
      "situation": ":situation",
      "soc_group": ":soc_group"
    },
    "result": [
      {
        "city_function": ":res_function",
        "region": ":res_district",
        "living_situation": ":res_situation",
        "provision": ":provision",
        "social_group": ":res_soc_group"
      }, <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/provision/ready/regions"
    }
  }
}
```

:function, :res_function - string representing city function. :function can be null in case it was not set in request, otherwise they are the same  
:district, :res_district - string representing district. :district can be null in case it was not set in request, otherwise they are the same  
:soc_group, :res_soc_group - string representing social group. :soc_group can be null in case it was not set in request, otherwise they are the same  
:situation, :res_situation - string representing social group. :situation can be null in case it was not set in request, otherwise they are the same
:provision - float from 0.0 to 5.0  

### /api/provision/ready/municipalities

Output format:

```json
{
  "_embedded": {
    "params": {
      "function": ":function",
      "municipality": ":municipality",
      "situation": ":situation",
      "soc_group": ":soc_group"
    },
    "result": [
      {
        "city_function": ":res_function",
        "municipality": ":res_municipality",
        "living_situation": ":res_situation",
        "provision": ":provision",
        "social_group": ":res_soc_group"
      }, <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/provision/ready/municipalities"
    }
  }
}
```

:function, :res_function - string representing city function. :function can be null in case it was not set in request, otherwise they are the same  
:municipality, :res_municipality - string representing municipality. :municipality can be null in case it was not set in request, otherwise they are the same  
:soc_group, :res_soc_group - string representing social group. :soc_group can be null in case it was not set in request, otherwise they are the same  
:situation, :res_situation - string representing social group. :situation can be null in case it was not set in request, otherwise they are the same  
:provision - float from 0.0 to 5.0
