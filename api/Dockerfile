FROM python:3
ENV PYTHONUNBUFFERED 1
ENV EUPPAPI_IN_DOCKER 1
RUN mkdir /euppapi
WORKDIR /euppapi
ADD requirements-docker.txt /euppapi
RUN pip install -r requirements-docker.txt
ADD . / ./
