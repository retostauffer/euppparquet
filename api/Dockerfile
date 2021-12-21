FROM python:3
ENV PYTHONUNBUFFERED 1
RUN mkdir /app
WORKDIR /app
ADD requirements-docker.txt /app
RUN pip install -r requirements-docker.txt
ADD . / ./
