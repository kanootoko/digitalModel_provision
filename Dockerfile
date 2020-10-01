FROM python:3.8

# RUN mkdir /python_libs

# COPY libs/*.whl /python_libs/

# RUN pip3 install /python_libs/*.whl

RUN pip3 install flask flask_compress psycopg2 pandas numpy requests

COPY provision_api.py /

ENTRYPOINT [ "python3", "/provision_api.py" ]