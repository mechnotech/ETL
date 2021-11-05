#!/bin/bash
cd movies_admin
python3 manage.py migrate --noinput
python3 manage.py collectstatic --noinput
../upload.sh
gunicorn wsgi:application  --chdir config/. --log-level info --bind 0.0.0.0:8000
