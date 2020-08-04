FROM python:3.7-slim

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8080

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/keys/service_account.json

CMD gunicorn -b 0.0.0.0:8080 main:app