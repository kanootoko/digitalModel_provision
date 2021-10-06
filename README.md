# Provision API Server

## Description

This is API server for calculating provision values (atomic and agregated) based on given city functions,
  living situations, social groups and data from postgres database with houses
  
## Preparation before launching (both Docker and host machine)

1. install postgres database and postgis extension
2. fill database with city data (`houses` matview and `social_groups`, `city_functions`, `living_situations`,
  `municipalities`, `districts`, `needs`, `infrastructure_types` tables are used)
3. install python3 (3.8 recommended) and modules: flask, flask_compress, psycopg2, pandas, numpy, requests
4. clone this repository
5. download geometry for current houses ([collect_geometry_help.md](collect_geometry_help.md), currently unavailable)

## Launching on host machine

1. open terminal in cloned repository
2. run with `python provision_api.py`

## Configuration by environment variables

Parameters can be configured with environment variables:

* PROVISION_API_PORT - api_port - port to run the api server [default: _80_]
* HOUSES_DB_ADDR - db_addr - address of the postgres with provision [default: _localhost_] (string)
* HOUSES_DB_PORT - db_port - port of the postgres with provision [default: _5432_] (int)
* HOUSES_DB_NAME - db_name - name of the postgres database with provision [default: _provision_] (string)
* HOUSES_DB_USER - db_user - user name for database [default: _postgres_] (string)
* HOUSES_DB_PASS - db_pass - user password for database [default: _postgres_] (string)

## Configuration by CLI Parameters

Command line arguments configuration is also avaliable (overrides environment variables configuration)

* -p,--port \<int\> - api_port
* -hH,--db_addr \<str\> - db_addr
* -hP,--db_port \<int\> - db_port
* -hN,--db_name \<str\> - db_name
* -hU,--db_user \<str\> - db_user
* -hW,--db_pass \<str\> - db_pass

## Building Docker image (the other way is to use Docker repository: kanootoko/digitalmodel_provision:2021-10-05)

1. open terminal in cloned repository
2. build image with `docker build --tag kanootoko/digitalmodel_provision:2021-10-05 .`
3. run image with postgres server running on host machine on default port 5432
    1. For windows: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=host.docker.internal -e PROVISION_DB_ADDR=host.docker.internal --name provision_api kanootoko/digitalmodel_provision:2021-10-05`
    2. For Linux: `docker run --publish 8080:8080 -e PROVISION_API_PORT=8080 -e HOUSES_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) -e PROVISION_DB_ADDR=$(ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1) --name provision_api kanootoko/digitalmodel_provision:2021-10-05`  
      Ensure that:
        1. _/etc/postgresql/\<version\>/main/postgresql.conf_ contains uncommented setting `listen_addresses = '*'` so app could access postgres from Docker network
        2. _/etc/postgresql/\<version\>/main/pg\_hba.conf_ contains `host all all 0.0.0.0/0 md5` so login could be performed from anywhere (you can set docker container address instead of 0.0.0.0)
        3. command `ip -4 -o addr show docker0 | awk '{print $4}' | cut -d "/" -f 1` returns ip address  
        If config files are not found, `sudo -u postgres psql -c 'SHOW config_file'` should say where they are

## Usage

After the launch you can find api avaliable at localhost:port/ . In example given it will be localhost with port 8080.  
For a normal usage you will need a working transport model avaliable.

## Endpoints

At this moment there are endpoints:

* **/api**: returns HAL description of API provided.
* **/api/provision_v3/ready**: returns the list of calculated service types with the number of them.
* **/api/provision_v3/services**: returns the list of conctere services with their provision evaluation. Takes `service` and `location` as optional parameters.
  `service` can be one of the services calculated (by name or by id), `location` is a district or municipality by full or short name.
* **/api/provision_v3/service/{service_id}**: returns the provision evaluation of a given service. If not found, service name = "Not found" and response status is 404.
* **/api/provision_v3/houses**: returns the list of houses with their services provision. At least one of the `service` and `location` parameters must be set.
  `location` can be municipality or district given as short or full name, `service` - service type given by id or by name.
* **/api/provision_v3/house/{house_id}**: returns the service types provision evaluation of a given house. If house is not found, address = "Not found" and
  response status is 404. `service` can be set by name or id to get information of ont particular service type.
* **/api/provision_v3/house_services/{house_id}/**: returns the list of services of given service type around the given living house. Can take radius as parameter.
* **/api/provision_v3/prosperity/municipalities**: returns the prosperity value of municipalities. Takes `social_group`, `service`/`city_function`/`infrastructure`,
  `district`/`municipality` and `provision_only` as optional parameters. If `district` is set, returns prosperity of municipalities of a given district.
  Each pameter except of `provision_only` is "all" if  not given - that means that every column will be returned. Each parameter except of `provision_only`
  can be set to "mean" to return mean value of the column.
* **/api/provision_v3/prosperity/districts**: returns the prosperity value of districts. Takes social_group, `service`/`city_function`/`infrastructure`,
  `district` and `provision_only` as optional parameters. Each pameter except of `provision_only` is "all" if not given - that means that every column will
  be returned. Each parameter except of `provision_only` can be set to "mean" to return mean value of the column.
* **/api/list/social_groups**: returns list of social groups. If you specify a city_function and/or living_situation,
  only relative social groups will be returned.
* **/api/list/city_functions**: returns a list of city functions. If you specify a social_group and/or living_situation,
  only relative city functions will be returned.
* **/api/list/services**: returns a list of services. If you specify a social_group and/or living_situation,
  only relarive services will be returned.
* **/api/list/living_situations**: returns a list of living situations. If you specify a social_group and/or city_function,
  only relative living situations will be returned.
* **/api/relevance/social_groups**: returns a list of social groups. If you specify city_function as a parameter, the output will be limited to social groups
  relevant to this city function, and significane will be returned for each of them. If you specify both city_function and living_situation, then
  intensity will be returned too.
* **/api/relevance/city_functions**: returns a list of city functions available. If you specify social_group as a parameter, output will be limited to city functions
  relevant to this social group, and significane will be returned for each of them. If you specify both social_group and living_situation, then
  intensity will be returned too.
* **/api/relevance/living_situations**: returns a list of living situations. If you specify social_group, the output will be limited to living situations relative
  to the given social group and intensity will be returned for each of them. If the city_function parameter is also specified, significance will be returned
  in params section.
* **/api/list/infrastructures**: returns a list of infrastructures with functions list for each of them and with services list for each funtion.
* **/api/list/districts**: returns a list of districts.
* **/api/list/municipalities**: returns a list of municipalities.
* **/api/houses**: returns coordinates of houses inside the square of `firstPoint` and `secondPoint` parameters coordinates.

Every endpoint that takes `social_group`, `city_function`, `living_situation`, `service`, `infrastructure`, `district` or `municipality` as parameters can
  use both word form and database id of entity.

### /api

```json
{
  "_links": {
    "list-city_functions": {
      "href": "/api/list/city_functions/{?social_group,living_situation}",
      "templated": true
    },
    "list-city_hierarchy": {
      "href": "/api/list/city_hierarchy/{?include_blocks,location}",
      "templated": true
    },
    "list-districts": {
      "href": "/api/list/districts/"
    },
    "list-infrastructures": {
      "href": "/api/list/infrastructures/"
    },
    "list-living_situations": {
      "href": "/api/list/living_situations/{?social_group,service}",
      "templated": true
    },
    "list-municipalities": {
      "href": "/api/list/municipalities/"
    },
    "list-services": {
      "href": "/api/list/services/{?social_group,living_situation}",
      "templated": true
    },
    "list-social_groups": {
      "href": "/api/list/social_groups/{?service,living_situation}",
      "templated": true
    },
    "provision_v3_ready": {
      "href": "/api/provision_v3/ready/"
    },
    "provision_v3_services": {
      "href": "/api/provision_v3/services/{?service,location}",
      "templated": true
    },
    "provision_v3_service": {
      "href": "/api/provision_v3/service/{service_id}",
      "templated": true
    },
    "provision_v3_houses" : {
      "href": "/api/provision_v3/houses{?service,location}",
      "templated": true
    },
    "provision_v3_house_service_types" : {
      "href": "/api/provision_v3/house/{house_id}/{?service}",
      "templated": true
    },
    "provision_v3_house_services": {
      "href": "/api/provision_v3/house_services/{house_id}/{?service}",
      "templated": true
    },
    "provision_v3_prosperity_districts": {
      "href": "/api/provision_v3/prosperity/districts/{?district,municipality,block,service,city_function,infrastructure,social_group,provision_only}",
      "templated": true
    },
    "provision_v3_prosperity_municipalities": {
      "href": "/api/provision_v3/prosperity/municipalities/{?district,municipality,block,service,city_function,infrastructure,social_group,provision_only}",
      "templated": true
    },
    "provision_v3_prosperity_blocks": {
      "href": "/api/provision_v3/prosperity/blocks/{?district,municipality,block,service,city_function,infrastructure,social_group,provision_only}",
      "templated": true
    },
    "relevant-city_functions": {
      "href": "/api/relevance/city_functions/{?social_group,living_situation}",
      "templated": true
    },
    "relevant-living_situations": {
      "href": "/api/relevance/living_situations/{?social_group,service}",
      "templated": true
    },
    "relevant-social_groups": {
      "href": "/api/relevance/social_groups/{?service,living_situation}",
      "templated": true
    },
    "self": {
      "href": "/api/"
    }
  },
  "version": ":version"
}
```

:version - string representing date in format "YYYY-MM-DD"

### /api/list/social_groups

```json
{
  "_embedded": {
    "params": {
      "city_function": ":service",
      "living_situation": ":living_situation"
    },
    "social_groups": [
      ":social_group",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/social_groups/"
    }
  }
}
```

:social_group - string, one of the social groups  
:service - string, one of the services; or null if not specified in request  
:living_situation - string, one of the living situations; or null if not specified in request

### /api/list/city_functions

```json
{
  "_embedded": {
    "city_functions": [
      ":city_function",
      <...>
    ],
    "params": {
      "living_situation": ":living_situation",
      "social_group": ":social_group"
    }
  },
  "_links": {
    "self": {
      "href": "/api/list/city_functions/"
    }
  }
}
```

:city_function - string, one of the city functions  
:living_situation - string, one of the living situations; or null if not specified in request  
:social_group - string, one of the social groups; or null if not specified in request

### /api/list/services

```json
{
  "_embedded": {
    "services": [
      ":service",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/services/"
    }
  }
}
```

:service - string, name of service

### /api/list/living_situations

```json
{
  "_embedded": {
    "living_situations": [
      ":living_situation",
      <...>
    ],
    "params": {
      "city_function": ":service",
      "social_group": ":social_group"
    }
  },
  "_links": {
    "self": {
      "href": "/api/list/living_situations/"
    }
  }
}
```

:living_situation - string, one of the living situations
:service - string, one of the services; or null if not specified in request  
:social_group - string, one of the social groups; or null if not specified in request

### /api/list/districts

```json
{
  "_embedded": {
    "districts": [
      ":district_name",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/districts/"
    }
  }
}
```

:district_name - string, name of district

### /api/list/municipalities

```json
{
  "_embedded": {
    "municipalities": [
      ":municipality_name",
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/list/municipalities/"
    }
  }
}
```

:municipality_name - string, name of municipality

### /api/list/city_hierarchy

```json
{
  "_embedded": {
    "districts": [
      {
        "id": ":district_id",
        "full_name": ":district_full_name",
        "short_name": ":district_short_name",
        "population": ":district_population",
        "municipalities": [
          {
            "id": ":municipality_id",
            "full_name": ":municipality_full_name",
            "short_name": ":municipality_short_name",
            "population": ":municipality_population",
            "blocks": [
              {
                "id": ":block_id",
                "population": ":block_population"
              },
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
      "href": "/api/list/city_hierarchy/"
    }
  }
}
```

:district_id, :municipality_id, :block_id - int, id of district, municipality or block in database  
:district_full_name, :district_short_name, :municipality_full_name, :municipality_short_name - string, full or short name of district or municipality in database  
:district_population, :municipality_population - int, population of district or municipality  
:block_population - int or null if not defined - population of block
"blocks" block is missing if "include_blocks" parameter is not set

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
      "href": "/api/list/infrastructures/"
    }
  }
}
```

:infrastructure_name - string, name of infrastructure  
:function_name - string, one of the city_functions  
:service_name - string, one of the service names

### /api/relevance/social_groups

```json
{
  "_embedded": {
    "params": {
      "service": ":service",
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
  "_links": {
    "self": {
      "href": "/api/relevance/social_groups/"
    }
  }
}
```

:service, :living_situation, :social_group - string, one of the city functions, living situations or social groups or null  
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
        "intensity": ":intensity",
        "significance": ":significance"
      },
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/relevance/city_functions/"
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
      "href": "/api/relevance/living_situations/"
    }
  }
}
```

:city_function, :living_situation, :social_group - string, one of the services, living situations or social groups or null  
:significance - :significance - float from 0.0 to 1.0  
:intensity - integer from 0 to 5, only if :social_group and :city_function is present

### /api/provision_v3/ready

```json
{
  "_embedded": {
    "ready": [
      {
        "service": ":service_name",
        "count": :service_count
      }, 
      <...>
    ]
  }, 
  "_links": {
    "self": {
      "href": "/api/provision_v3/ready/"
    }
  }
}
```

:service_name - string representing one of the servise types  
:service_count - int above zero, number of evaluated services of this service type

### /api/provision_v3/services

```json
{
  "_embedded": {
    "parameters": {
      "response_services_count": ":response_services_count",
      "service": ":service_type"
    },
    "services": [
      {
        "service_id": ":service_id",
        "district": ":district_short_name",
        "municipality": ":municipality_short_name",
        "block": ":block_id",
        "address": ":address",
        "service_name": ":service_name",
        "service_type": ":service_type",
        "houses_in_access": ":houses",
        "people_in_access": ":people",
        "service_load": ":service_load",
        "needed_capacity": ":needed_capacity",
        "reserve_resource": ":service_resource",
        "evaluation": ":evaluation"
      },
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/provision_v3/services/",
      "service_info": {
        "href": "/api/provision_v3/service/{service_id}/",
        "templated": true
      }
    }
  }
}
```

:service_id - int, id of functional_object in database  
:response_services_count - int, size of returned "services" array  
:district_short_name, :municipality_short_name, :address - string (or null)  
:block_id - integer (or null)  
:service_type - string representing one of the service types (missing in "services" section if
  `service` parameter was given in request and is null in "parameters" section otherwise)  
:houses, :people - int above or equal to zero  
:service_load, :needed_capacity, :service_resource - int. service_load >= needed_capacity, needed_capacity + service_resource = service_maximum_load  
:evaluation - int from 0 to 10

### /api/provision_v3/prosperity/municipalities/

```json
{
  "_embedded": {
    "parameters": {
      "district": ":district_request",
      "location_type": "municipalities",
      "municipality": ":municipality_request",
      "service": ":service_reqeust",
      "social_group": ":social_group"
    },
    "prosperity": [
      {
        "municipality": ":municipality",
        "service": ":service",
        "social_group": ":social_group",
        "significance": ":significance",
        "provision": ":provision",
        "prosperity": ":prosperity"
      },
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/provision_v3/prosperity/municipalities/"
    }
  }
}
```

:district_request, :municipality_request - string, district and municipality set in request (municipality can have value "all")  
:service_request - string, service in request (can have value "all")  
:municipality - string, municipality  
:service - string, service  
:social_group - string, social group. If `social_group` request parameter is set to "all", it is skipped fully  
:significance - float from 0.0 to 1.0, significance of a given service to a social group (or mean of all social groups if `social_group` is set to "all")  
:provision - float from 0.0 to 10.0, provision of a given service in municipality  
:prosperity - float, prosperity value of a municipality

### /api/provision_v3/prosperity/districts

```json
{
  "_embedded": {
    "parameters": {
      "district": ":district_request",
      "location_type": "municipalities",
      "municipality": null,
      "service": ":service_reqeust",
      "social_group": ":social_group"
    },
    "prosperity": [
      {
        "municipality": ":municipality",
        "service": ":service",
        "social_group": ":social_group",
        "significance": ":significance",
        "provision": ":provision",
        "prosperity": ":prosperity"
      },
      <...>
    ]
  },
  "_links": {
    "self": {
      "href": "/api/provision_v3/prosperity/districts/"
    }
  }
}
```

:district_request - string, district set in request (can have value "all")  
:service_request - string, service in request (can have value "all")  
:municipality - string, municipality  
:service - string, service  
:social_group - string, social group. If `social_group` request parameter is set to "all", it is skipped fully  
:significance - float from 0.0 to 1.0, significance of a given service to a social group (or mean of all social groups if `social_group` is set to "all")  
:provision - float from 0.0 to 10.0, provision of a given service in municipality  
:prosperity - float, prosperity value of a municipality
