FROM python:3.13-alpine

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ARG RABBITMQ_URL
ENV RABBITMQ_URL=$RABBITMQ_URL
ARG RABBITMQ_PUBLISHER_CONFIRMS
ENV RABBITMQ_PUBLISHER_CONFIRMS=$RABBITMQ_PUBLISHER_CONFIRMS
ENTRYPOINT [ "python",  "main.py" ]