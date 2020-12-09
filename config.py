# -*- coding: utf-8 -*-
from baseconfig import *

REGION = REGION_SPB
DEBUG = True
ORGANISATION = '5a358952-350b-4be0-b0b4-cc960dcde02b'

DB_CONNECTION_INFO = {
    "host": "192.168.0.204",
    "port": 3306,
    "user": "dbuser",
    "password": "dbpassword",
    "db": "s12",
}

DB_LOGGER = {
    "host": "192.168.0.204",
    "port": 3306,
    "user": "dbuser",
    "password": "dbpassword",
    "db": LOGGER_DB_NAME,
}

AUTH_TOKEN = N3_AUTHORIZATION_TOKEN[REGION][DEBUG]
URL = SERVICE_URL[REGION][DEBUG]


VERSION_RESOURCE = 'api/_version/'
BUNDLE_RESOURCE = 'bundle/'

# на каком IP и порте слушает наш сервер
SERVER_IP = '0.0.0.0'
SERVER_PORT = '8000'
