FROM python:3.8

COPY requirements.txt /
RUN pip3 install -r requirements.txt

RUN mkdir df_saver_cli
COPY df_saver_cli/saver.p[y] /df_saver_cli/
COPY df_saver_cli/requirements.tx[t] /df_saver_cli/
RUN pip3 install -r /df_saver_cli/requirements.txt 2>/dev/null || echo "NO df_saver_cli FOUND"

COPY provision_api.py /

COPY collect_geometry.py /
COPY mongolog.py /

ENTRYPOINT [ "python3", "/provision_api.py" ]