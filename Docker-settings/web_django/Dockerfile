FROM python:3.10.0-slim-buster
LABEL name='Movies Admin' version=1
WORKDIR /code
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
COPY ./requirements.txt .
RUN pip install -r requirements.txt
COPY ./ .
COPY ./Docker-settings/web_django/ .
RUN ["chmod", "+x", "commands.sh"]
RUN ["chmod", "+x", "upload.sh"]
ENTRYPOINT ["./commands.sh"]
