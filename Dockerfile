## Notes
# This is a stripped-down version of my production container, which should explain why it's not based on
# Alpine Linux like it could be.

FROM python:3.7-slim

RUN pip install cloudpickle && \
    rm -rf ~/.cache

WORKDIR /code