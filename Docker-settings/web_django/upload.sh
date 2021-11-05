#!/bin/bash
echo "Upload fixtures to Postgres, please wait 20-30 seconds ..." >&1
./manage.py loaddata ../fixtures.json