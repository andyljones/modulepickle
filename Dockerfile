FROM python:3.7-slim

RUN pip install cloudpickle && \
    rm -rf ~/.cache

WORKDIR /code