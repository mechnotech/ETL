FROM python:3.10.0-slim-buster
LABEL name='ETL' version=1
WORKDIR /code
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
COPY ./requirements.txt .
RUN pip install -r requirements.txt
COPY ./ .
ENTRYPOINT ["python3", "updater.py"]