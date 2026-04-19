#!/bin/sh
apt-get update
apt-get install -y libpq-dev
python3 main.py
