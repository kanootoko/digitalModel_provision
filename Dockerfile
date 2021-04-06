FROM python:3.8

RUN pip3 install flask flask_compress psycopg2 pandas numpy requests gevent

COPY provision_api.py /
COPY calculate_services_cnt.py /
COPY thread_pool.py /
COPY experimental_aggregation.py /

ENTRYPOINT [ "python3", "/provision_api.py" ]