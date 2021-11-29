FROM python:3.8

RUN pip3 install flask flask_compress psycopg2 pandas numpy requests gevent simplejson

COPY provision_api.py /

COPY collect_geometry.py /

ENTRYPOINT [ "python3", "/provision_api.py" ]