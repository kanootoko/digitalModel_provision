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

## Building Docker image (the other way is to use Docker repository: kanootoko/digitalmodel_provision:2020-11-17)

1. open terminal in cloned repository
2. build image with `docker build --tag kanootoko/digitalmodel_provision:2020-11-17 .`
3. run image with postgres server running on host machine on default port 5432
    1. For windows: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=host.docker.internal -e PROVISION_DB_ADDR=host.docker.internal --name provision_api kanootoko/digitalmodel_provision:2020-11-17`
    2. For Linux: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) -e PROVISION_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) --name provision_api kanootoko/digitalmodel_provision:2020-11-17`  
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
  and services inside of them. Takes parameters by query. You must set `social_group` for social group,
  `city_function` for city city_function, `living_situation` for living living_situation and `point` for coordinates of the house.
  Point format is `latitude,longitude`.  
  Also you can set some of the calculation parameters by setting values: *walking_time_cost*, *transport_time_cost*, *personal_transport_time_cost*,
  *walking_availability*, *significance*, *intensity*, *public_transport_availability_multiplier*, *personal_transport_availability_multiplier*, *max_target_s*,
  *target_s_divider*, *coeff_multiplier*.
* **/api/provision/aggregated**: returns the aggregated provision value. Takes parameters by query. You should set at least something in: `social_group` for
  social group or "all", `city_function` for city city_function or "all, `living_situation` for living living_situation or "all", `region` for district,
  municipality or house (format: latitude,longitude). `region` can be also "inside_\<district\>" to list all of the municipalities inside the given district
* **/api/provision/ready/houses**: returns the list of already aggregated by houses provision values.
  Takes parameters by query. You can set `social_group`, `city_function`, `living_situation` or `house` parameter to specify the request.
* **/api/provision/ready/districts**: returns the list of already aggregated by districts provision values.
  Takes parameters by query. You can set `social_group`, `city_function`, `living_situation` or `district` parameter to specify the request.
* **/api/provision/ready/municipalities**: returns the list of already aggregated by municipalities provision values.
  Takes parameters by query. You can set `social_group`, `city_function`, `living_situation` or `municipality` parameter to specify the request.
* **/api/list/social_groups**: returns list of social groups. If you specify a city_function and/or living_situation, only relative social groups will be returned
* **/api/list/city_functions**: returns a list of city functions. If you specify a social_group and/or living_situation,
  only relative city functions will be returned
* **/api/list/living_situations**: returns a list of living situations. If you specify a social_group and/or city_function,
  only relative living situations will be returned
* **/api/relevance/social_groups**: returns a list of social groups. If you specify city_function as a parameter, the output will be limited to social groups
  relevant to this city function, and significane will be returned for each of them. If you specify both city_function and living_situation, then
  intensity will be returned too.
* **/api/relevance/city_functions**: returns a list of city functions available. If you specify social_group as a parameter, output will be limited to city functions
  relevant to this social group, and significane will be returned for each of them. If you specify both social_group and living_situation, then
  intensity will be returned too.
* **/api/relevance/living_situations**: returns a list of living situations. If you specify social_group, the output will be limited to living situations relative
  to the given social group and intensity will be returned for each of them. If the city_function parameter is also specified, significance will be returned
  in params section
* **/api/list/infrastructures**: returns a list of infrastructures with functions list for each of them and with services list for each funtion
* **/api/list/districts**: returns a list of districts
* **/api/list/municipalities**: returns a list of municipalities
* **/api/houses**: returns coordinates of houses inside the square of `firstPoint` and `secondPoint` parameters coordinates.

### /api

Output format:

```json
{
  "_links": {
    "aggregated-provision": {
      "href": "/api/provision/aggregated{?social_group,living_situation,city_function,region}",
      "templated": true
    },
    "atomic_provision": {
      "href": "/api/provision/atomic{?social_group,living_situation,city_function,point}",
      "templated": true
    },
    "get-houses": {
      "href": "/api/houses{?firstPoint,secondPoint}",
      "templated": true
    },
    "list-city_functions": {
      "href": "/api/list/city_functions{?social_group}",
      "templated": true
    },
    "list-districts": {
      "href": "/api/list/districts"
    },
    "list-infrastructures": {
      "href": "/api/list/infrastructures"
    },
    "list-living_situations": {
      "href": "/api/list/living_situations{?social_group}",
      "templated": true
    },
    "list-municipalities": {
      "href": "/api/list/municipalities"
    },
    "list-social_groups": {
      "href": "/api/list/social_groups{?city_function}",
      "templated": true
    },
    "ready_aggregations_districts": {
      "href": "/api/provision/ready/districts{?social_group,living_situation,city_function,district}",
      "templated": true
    },
    "ready_aggregations_houses": {
      "href": "/api/provision/ready/houses{?social_group,living_situation,city_function,house}",
      "templated": true
    },
    "ready_aggregations_municipalities": {
      "href": "/api/provision/ready/municipalities{?social_group,living_situation,city_function,municipality}",
      "templated": true
    },
    "relevant-city_functions": {
      "href": "/api/relevance/city_functions{?social_group,living_situation}",
      "templated": true
    },
    "relevant-living_situations": {
      "href": "/api/relevance/living_situations{?social_group,city_function}",
      "templated": true
    },
    "relevant-social_groups": {
      "href": "/api/relevance/social_groups{?city_function,living_situation}",
      "templated": true
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
    "params": {
      "city_function": ":city_function"
    },
    "social_groups": [
      "Младенцы (0-1)",
      "Дети до-детсадовского возраста (1-3)",
      "Дети до-дошкольного возраста (3-7)",
      "Дети младшего школьного возраста (7-11)",
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

:city_function - string, one of the city functions; or null if not specified in request

### /api/list/city_functions

Output format:

```json
{
  "_embedded": {
    "city_functions": [
      "Жилье",
      "Образование",
      "Здравоохранение",
      "Религия",
      "Продовольствие",
      <...>
    ],
    "params": {
      "social_group": ":social_group"
    }
  },
  "_links": {
    "self": {
      "href": "/api/list/city_functions"
    }
  }
}
```

:social_group - string, one of the social groups; or null if not specified in request

### /api/list/living_situations

Output format:

```json
{
  "_embedded": {
    "living_situations": [
      "Типичный нерабочий день",
      "Типичный рабочий день",
      "Свидание",
      <...>
    ],
    "params": {
      "social_group": ":social_group"
    },
  },
  "_links": {
    "self": {
      "href": "/api/list/living_situations"
    }
  }
}
```

:social_group - string, one of the social groups; or null if not specified in request

### /api/list/districts

Output format:

```json
{
  "_embedded": {
    "districts": [
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

### /api/list/infrastructures

```json
{
  "_embedded": {
    "infrastructures": [
      {
        "name": ":infrastructure_name",
        "functions": [
          {
            "name": ":function_name",
            "services": [
              ":service_name",
              <...>
            ]
          },
          <...>
        ]
      },
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/infrastructures"
    }
  }
}
```

:infrastructure_name - string  
:function_name - string, one of the city_functions  
:service_name - string, one of the service names

### /api/relevance/social_groups

```json
{
  "_embedded": {
    "params": {
      "city_function": ":city_function",
      "living_situation": ":living_situation"
    },
    "social_groups": [
      {
        "intensity": ":intensity",
        "significance": ":significance",
        "social_group": ":social_group"
      },
      <...>
    ]
  }
}
```

:city_function, :living_situation, :social_group - string, one of the city functions, living situations or social groups or null  
:significance - float from 0.0 to 1.0, only if :city_function is present  
:intensity - integer from 0 to 5, only if :city_function and :living_situation is present

### /api/relevance/city_functions

```json
{
  "_embedded": {
    "params": {
      "living_situation": ":living_situation",
      "social_group": ":social_group"
    },
    "city_functions": [
      {
        "city_function": ":city_function",
        "infrastructure": ":infrastructure",
        "intensity": 10,
        "significance": 1
      },
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/relevance/city_functions"
    }
  }
}
```

:city_function, :living_situation, :social_group - string, one of the city functions, living situations or social groups or null  
:significance - :significance - float from 0.0 to 1.0  
:intensity - integer from 0 to 5, only if :social_group and :living_situation is present

### /api/relevance/living_situations

```json
{
  "_embedded": {
    "params": {
      "city_function": ":city_function",
      "significance": ":significance",
      "social_group": ":social_group"
    },
    "living_situations": [
      {
        "intensity": ":intensity",
        "living_situation": ":living_situation"
      },
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/relevance/living_situations"
    }
  }
}
```

:city_function, :living_situation, :social_group - string, one of the city functions, living situations or social groups or null  
:significance - :significance - float from 0.0 to 1.0  
:intensity - integer from 0 to 5, only if :social_group and :city_function is present

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
:intensity - integer from 1 to 10  
:service_availability, :significance - float from 0.0 to 1.0  
:personal_cost, :transport_cost, :walking_cost - integers representing minutes  
:service_type - string, type of service representing city city_function  
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
      "city_function": ":request_city_function",
      "launch_aggregation": ":launch_aggregation",
      "living_situation": ":request_living_situation",
      "region": ":request_region",
      "social_group": ":request_social_group",
      "where_type": ":where_type"
    },
    "provision": [
      {
        "params": {
          "city_function": ":result_city_function",
          "living_situation": ":result_living_situation",
          "region": ":result_region",
          "social_group": ":result_social_group"
        },
        "result": {
          "intensity": ":result_intensity",
          "provision": ":result_provision",
          "significance": ":result_significance",
          "time_done": ":time_done"
        }
      },
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/provision/aggregated"
    }
  }
}
```

:request_city_function, :request_living_situation, :request_social_group - string of city function, living situation or social_group, or null or "all"  
:request_region - district, municipality, house coordinates or "inside_\<district\>" from request  
:where_type - "municipalities", "districts" or "house" depending on request region  
:launch_aggregation - boolean from request  
:city_function, :living_situation, :social_group - string of city function, living situation or social_group  
:result_region - district, municipality or house coordinates  
:result_intensity, :result_significance - integer from 1 to 10  
:result_provision - float from 0.0 to 5.0  
:time_done - time when aggregation was completed

### /api/provision/ready/regions

Output format:

```json
{
  "_embedded": {
    "params": {
      "city_function": ":city_function",
      "region": ":district",
      "living_situation": ":living_situation",
      "social_group": ":social_group"
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

:city_function, :res_function - string representing city city_function. :city_function can be null in case it was not set in request, otherwise they are the same  
:district, :res_district - string representing district. :district can be null in case it was not set in request, otherwise they are the same  
:social_group, :res_soc_group - string representing social group. :social_group can be null in case it was not set in request, otherwise they are the same  
:living_situation, :res_situation - string representing social group. :living_situation can be null in case it was not set in request, otherwise they are the same  
:provision - float from 0.0 to 5.0  

### /api/provision/ready/municipalities

Output format:

```json
{
  "_embedded": {
    "params": {
      "city_function": ":city_function",
      "municipality": ":municipality",
      "living_situation": ":living_situation",
      "social_group": ":social_group"
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

:city_function, :res_function - string representing city city_function. :city_function can be null in case it was not set in request, otherwise they are the same  
:municipality, :res_municipality - string representing municipality. :municipality can be null in case it was not set in request, otherwise they are the same  
:social_group, :res_soc_group - string representing social group. :social_group can be null in case it was not set in request, otherwise they are the same  
:living_situation, :res_situation - string representing social group. :living_situation can be null in case it was not set in request, otherwise they are the same  
:provision - float from 0.0 to 5.0

### /api/houses

```json
{
  "_embedded": {
    "params": {
      "firstCoord": ":firstCoord",
      "secondCoord": ":secondCoord"
    },
    "houses": [
    [,
        ":house_latitude",
        ":house_longitude"
      ],
    ]
  },
  "_links": {
    "self": {
      "href": "/api/houses"
    }
  }
}
```

:firstCoord, :secondCoord - points in format `latitude,longitude`
:house_latitude, :house_longitude - float with precision of 3 digits after a point
