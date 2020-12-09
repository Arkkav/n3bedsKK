# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# ЭТОТ ФАЙЛ РЕДАКТИРОВАТЬ НЕ НУЖНО, ВСЕ ИЗМЕНЕНИЯ -- в n3bedsKK/config.py
# -------------------------------------------------------------------------------

REGION_SPB = '78'
REGION_KK  = '23'

# -------------------------------------------------------------------------------

# Служебное
DEBUG = False  # Использование тестового сервера

# Общее
REGION = REGION_SPB

# Токен авторизации
N3_AUTHORIZATION_TOKEN = {
    REGION_SPB: (
        '',                                      # Реальный - ???
        ''   # Тестовый
    ),
    REGION_KK : (
        '',                                      # Реальный - ???
        ''                                       # Тестовый - ???
    )
}

# Адрес сервиса
SERVICE_URL = {
    REGION_SPB: (
        '',     # Реальный
        'http://r23-rc.zdrav.netrika.ru/bedservice/_api/'  # Тестовый
    ),
    REGION_KK: (
        '',         # Реальный
        ''  # Тестовый
    )
}

LOGGER_DB_NAME = 'logger' # Название базы логгера
LOG_FILE_NAME = 'log.log'