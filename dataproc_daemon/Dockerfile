FROM python:latest
MAINTAINER Bob De Schutter <bob.deschutter@vente-exclusive.com>

COPY dataproc-daemon.py /usr/src/app/

ENV GC_PROJECT_ID <my-project_id>
ENV GOOGLE_APPLICATION_CREDENTIALS /path/to/service-account-keyfile.json

RUN pip install google-api-python-client && \
    pip install google-cloud && \
    rm -rf /root/.cache/pip/*

CMD ["python", "/usr/src/app/dataproc-daemon.py"]
