import sys
import os
import logging
global_logger = None


def get_logger():
    global global_logger
    if global_logger is None:
        global_logger = logging.getLogger('bedsfund')
        formatter = logging.Formatter('[%(asctime)s] %(message)s')
        handler = logging.FileHandler(config.LOG_FILE_NAME)
        handler.setFormatter(formatter)
        global_logger.addHandler(handler)
        global_logger.setLevel(logging.DEBUG)
        urllib3 = logging.getLogger("urllib3.connectionpool")
        for hdlr in urllib3.handlers[:]:
            if isinstance(hdlr, logging.FileHandler):
                urllib3.removeHandler(hdlr)
        urllib3.addHandler(handler)
        urllib3.setLevel(logging.DEBUG)
    return global_logger


try:
    import config
    import pymysql
    import pytz
    import time, requests, json, traceback
    from datetime import datetime, timedelta
    from argparse import ArgumentParser
except ImportError as e:
    get_logger().error('Ошибка импорта: {message}\n'.format(message=e.args))
    sys.exit(1)

statuses_type = {200: u'Успешный ответ',
                 201: u'Успешный ответ, ресурс создан',
                 400: u'Ресурс не может быть проанализирован или не прошел валидацию',
                 403: u'Ошибка авторизации(неверный токен)',
                 404: u'Тип/метод ресурса не поддерживается',
                 405: u'Неверно сформирован запрос к сервису',
                 409: u'Попытка создания дубля данных (конфликт)',
                 415: u'Неподдерживаемый тип данных',
                 413: u'Тело запроса слишком велико',
                 422: u'Ошибка валидации',
                 500: u'Сервис недоступен. Внутренняя ошибка сервиса',
                 502: u'Сервис недоступен. Ошибки модуля',
                 503: u'Сервис недоступен. Ошибка',
                 504: u'Сервис недоступен. Таймаут'}

# Коды профилей койк для данной OrgStructure (сколько resource вставляем в entry)
query_netrica_code_count = \
    '''        
        select hbp.netrica_Code as bed_netrica_Code, os.netrica_Code as os_netrica_code,
        os.net_id as net_id, os.id as orgStructure_id, org.netrica_Code as org_netrica_code
        from OrgStructure_HospitalBed as oshb
            join OrgStructure as os on oshb.master_id = os.id
            join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
            left join Organisation org on org.id = os.organisation_id
        where os.netrica_Code = '{os_netrica_code}'
        and oshb.isPermanent = 1
        group by os.id;
    '''

# суммирукем, потому что на один netricaBed_code несколько orgStructure_id
query_counts_on_date = \
    '''
        select netrica_id,
               createDatetime,
               netricaBed_code,
               os_netrica_code,
               sum(TotalBedCount) as TotalBedCount,
               sum(FreeBedCount) as FreeBedCount,
               sum(FreeBedCountMale) as FreeBedCountMale,
               sum(FreeBedCountFemale) as FreeBedCountFemale,
               sum(FreeBedCountChild) as FreeBedCountChild,
               sum(AccompPersonCount) as AccompPersonCount,
               sum(OccupiedBedCount) as OccupiedBedCount,
               sum(PrevDayOccupiedBedCount) as PrevDayOccupiedBedCount,
               sum(BedCountOnRepair) as BedCountOnRepair
        from {logger_table}
        where
           createDatetime =
              (select createDatetime
               from logger.NetricaBedsExchange
               where createDatetime >= '{start_date}'
               {os_netrica_code_cond}
               limit 1)
            {os_netrica_code_cond}
        group by netricaBed_code;
    '''


def get_logger_table_name():
    return '{logger}.NetricaBedsExchange'.format(logger=config.LOGGER_DB_NAME)


def fpsplit(fullpath):
    path, tail = os.path.split(fullpath)
    name, ext = os.path.splitext(tail)
    return path, name, ext


def fpmerge(*parts):
    assert (len(parts) > 1)
    tail = parts[1] + parts[2] if len(parts) > 2 else parts[1]
    return os.path.join(parts[0], tail)


def chext(new_ext):
    path, name, ext = fpsplit(sys.argv[0])
    return fpmerge(path, name, new_ext)


def exists(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def file_exists(filename):
    return os.path.exists(filename)


def already_running(filename):
    if not file_exists(filename):
        return False
    fo = open(filename, 'r+')
    old_pid = fo.read()
    return exists(int(old_pid))


def create_pid_file(filename):
    with open(filename, 'w+') as f:
        f.write(str(os.getpid()))


def remove_pid_file(filename):
    if file_exists(filename):
        os.remove(filename)


def set_offset(db):
    with db.cursor() as cur:
        query = 'SELECT TIME_TO_SEC(TIMEDIFF(NOW(), UTC_TIMESTAMP)) DIV 60 AS timezone;'
        cur.execute(query)
        offset = cur.fetchone()[0]
    td = timedelta(minutes=offset)
    return offset, td


def records_to_dict_list(records, arr):
    r = []
    for rec in records:
        a = {}
        for j, name in enumerate(arr):
            a[name] = rec[j]
        r.append(a)
    return r


class CBedsExchange(object):
    def __init__(self, db, db_logger, logger):
        self.db = db  # type: library.database.CDatabase
        self.db_logger = db_logger
        self.logger = logger
        self.cdf = self.get_data_frame()
        self.offset, self.td = set_offset(self.db)

    @staticmethod
    def get_data_frame():
        columns = ['Название', '']
        values = [[u'Общее количество коек',
                   u'Общее количество свободных коек',
                   u'Незанятых мужских коек',
                   u'Незанятых женских коек',
                   u'Количество сопровождающих при больных детях',
                   u'Количество занятых коек на начало текущих суток',
                   u'Количество занятых коек на начало истекших суток',
                   u'Количество закрытых на ремонт коек',
                   ],
                  ]
        json_names = ['TotalBedCount',  # Общее количество коек
                      'FreeBedCount',  # Общее количество свободных коек
                      'FreeBedCountMale',  # Незанятых мужских коек
                      'FreeBedCountFemale',  # Незанятых женских коек
                      'FreeBedCountChild',  # Незанятых детских коек
                      'AccompPersonCount',  # Количество сопровождающих при больных детях
                      'OccupiedBedCount',  # Количество занятых коек на начало текущих суток
                      'PrevDayOccupiedBedCount',  # Количество занятых коек на начало истекших суток
                      'BedCountOnRepair',  # Количество закрытых на ремонт коек
                      ]

        queries = [
            # Общее количество коек
            '''
                select count(*)
                from OrgStructure_HospitalBed as oshb
                    join OrgStructure as os on oshb.master_id = os.id
                    join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                where
                os.id = {OrgStructure_id}
                AND oshb.isPermanent = 1
                AND hbp.netrica_Code = {netrica_bed_Code};
            ''',
            # Общее количество свободных коек
            '''
                select count(*)
                from OrgStructure_HospitalBed as oshb
                    join OrgStructure as os on oshb.master_id = os.id
                    join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                where
                    os.id = {OrgStructure_id}
                    AND isPermanent = 1
                    AND hbp.netrica_Code = {netrica_bed_Code}
                    AND NOT isHospitalBedBusy(oshb.id, STR_TO_DATE('{date}', '%Y-%m-%dT%H:%i:%s'));
            ''',
            # Незанятых мужских коек
            '''
                select count(*)
                from OrgStructure_HospitalBed as oshb
                    join OrgStructure as os on oshb.master_id = os.id
                    join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                where
                    os.id = {OrgStructure_id}
                    AND isPermanent = 1
                    AND hbp.netrica_Code = {netrica_bed_Code}
                    AND NOT oshb.age = '0-17'
                    AND oshb.sex = 1
                    AND NOT isHospitalBedBusy(oshb.id, STR_TO_DATE('{date}', '%Y-%m-%dT%H:%i:%s'));
            ''',
            # Незанятых женских коек
            '''
                select count(*)
                from OrgStructure_HospitalBed as oshb
                    join OrgStructure as os on oshb.master_id = os.id
                    join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                where
                    os.id = {OrgStructure_id}
                    AND isPermanent = 1
                    AND hbp.netrica_Code = {netrica_bed_Code}
                    AND NOT oshb.age='0-17'
                    AND oshb.sex=2
                    AND NOT isHospitalBedBusy(oshb.id, STR_TO_DATE('{date}', '%Y-%m-%dT%H:%i:%s'));
            ''',
            # Незанятых детских коек - смотрим по OrgStructure.net_id, т.к. age не заполняется
            # '''
            #     select count(*)
            #     from OrgStructure_HospitalBed as oshb
            #         join OrgStructure as os on oshb.master_id = os.id
            #         join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
            #     where
            #         os.id = {OrgStructure_id}
            #         AND isPermanent = 1
            #         AND hbp.netrica_Code = {netrica_bed_Code}
            #         AND oshb.age='0-17'
            #         AND NOT oshb.age IS NULL
            #         AND NOT oshb.age = ''
            #         AND NOT isHospitalBedBusy(oshb.id, STR_TO_DATE('{date}', '%Y-%m-%dT%H:%i:%s'));
            # ''',
            # просто количество незанятых коек еще раз
            '''
                select count(*)
                from OrgStructure_HospitalBed as oshb
                    join OrgStructure as os on oshb.master_id = os.id
                    join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                where
                    os.id = {OrgStructure_id}
                    AND isPermanent = 1
                    AND hbp.netrica_Code = {netrica_bed_Code}
                    AND NOT isHospitalBedBusy(oshb.id, STR_TO_DATE('{date}', '%Y-%m-%dT%H:%i:%s'));
            ''',
            # Количество сопровождающих при больных детях
            '''
                select count(*)
                from OrgStructure_HospitalBed as oshb
                    LEFT JOIN OrgStructure as os on oshb.master_id = os.id
                    LEFT JOIN rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                    LEFT JOIN ActionProperty_HospitalBed as aphb on oshb.id = aphb.value
                    LEFT JOIN ActionProperty as ap on ap.id = aphb.id
                    LEFT JOIN Action AS a ON a.id = ap.action_id
                    LEFT JOIN Event AS e ON e.id = a.event_id
                    LEFT JOIN Client AS c ON e.client_id = c.id
                where
                    CURRENT_DATE <= DATE_ADD(c.birthDate, INTERVAL 4 YEAR)
                    AND os.id = {OrgStructure_id}
                    AND isPermanent = 1
                    AND hbp.netrica_Code = {netrica_bed_Code}
                    AND isHospitalBedBusy(oshb.id, STR_TO_DATE('{date}', '%Y-%m-%dT%H:%i:%s'));
            ''',
            # Количество занятых коек на начало текущих суток
            '''
                select count(*)
                from OrgStructure_HospitalBed as oshb
                    join OrgStructure as os on oshb.master_id = os.id
                    join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                where
                    os.id = {OrgStructure_id} 
                    AND oshb.isPermanent = 1
                    AND isHospitalBedBusy(oshb.id, DATE_ADD(CURDATE(), INTERVAL "00:00:00" HOUR_SECOND));
            ''',
            # Количество занятых коек на начало истекших суток
            '''
                select count(*)
                from OrgStructure_HospitalBed as oshb
                    join OrgStructure as os on oshb.master_id = os.id
                    join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                where
                    os.id = {OrgStructure_id}
                    AND oshb.isPermanent = 1
                    AND isHospitalBedBusy(oshb.id, DATE_ADD(CURDATE(), INTERVAL "-1 00:00:00" DAY_SECOND));
            ''',
            # Количество закрытых на ремонт коек
            '''
                select count(1)
                from OrgStructure_HospitalBed as oshb
                    left join OrgStructure as os on oshb.master_id = os.id
                    left join rbHospitalBedProfile as hbp on hbp.id = oshb.profile_id
                    left join HospitalBed_Involute as hi on hi.master_id = oshb.id
                where hi.involuteType = 1
                    AND oshb.isPermanent = 1
                    AND os.id = {OrgStructure_id}
                    AND hbp.netrica_Code = {netrica_bed_Code}
                    AND (hi.begDateInvolute IS NULL
                        OR hi.begDateInvolute <= STR_TO_DATE('{date}', '%Y-%m-%dT%H:%i:%s'))
                    AND (hi.endDateInvolute IS NULL
                        OR hi.endDateInvolute >= STR_TO_DATE('{date}', '%Y-%m-%dT%H:%i:%s'));
            ''',
        ]
        return Dataframe(columns, values, queries, json_names)

    def save_id_to_db(self, result_json, os_netrica_code):
        # функция обрабатывает пришедший из нетрики json с датами в UTC и записывает в базу logger
        self.logger.debug(u'Запись данных в базу данных ' + config.LOGGER_DB_NAME)
        return_flag = 1
        try:
            db = self.db_logger
            for resource in result_json['entry']:
                netrica_id = resource['resource']['id']
                netrica_bed_code = str(
                    resource['resource']['characteristic'][0]['coding'][0]['code'])
                start_date = datetime.strptime(resource['resource']['extension'][-1]['valuePeriod']['start'],
                                               '%Y-%m-%dT%H:%M:%SZ')
                start_date_local = start_date + self.td  # локальное время
                start_date_local.replace(tzinfo=pytz.FixedOffset(self.offset))
                start_date_local = start_date_local.strftime("%Y-%m-%dT%H:%M:%S")
                query = '''
                    SELECT * 
                    FROM {logger_table}
                    WHERE netricaBed_code = '{netricaBed_code}'
                           and createDatetime = '{start_date}'
                           and os_netrica_code = '{os_netrica_code}';
                        '''.format(
                    logger_table=get_logger_table_name(),
                    netricaBed_code=netrica_bed_code,
                    start_date=start_date_local,
                    os_netrica_code=os_netrica_code)
                with db.cursor() as cur:
                    cur.execute(query)
                    record = cur.fetchone()

                if record:
                    with db.cursor() as cur:
                        query = '''
                            UPDATE {logger_table} 
                            SET modifyDatetime = '{current_date}', netrica_id = '{netrica_id}'
                            WHERE netricaBed_code = '{netricaBed_code}'
                               and createDatetime = '{start_date}'
                               and os_netrica_code = '{os_netrica_code}';
                        '''.format(
                            current_date=now_in_db(db).strftime("%Y-%m-%dT%H:%M:%S"),
                            netrica_id=netrica_id,
                            logger_table=get_logger_table_name(),
                            netricaBed_code=netrica_bed_code,
                            start_date=start_date_local,
                            os_netrica_code=os_netrica_code)
                        cur.execute(query)
                    db.commit()
                    self.logger.debug(u'Запись с id = ' + netrica_id + u' обновлена')
                else:
                    return_flag = 0
                    self.logger.debug(u'В таблице ' + get_logger_table_name() + \
                                      u' нет записи с netrica_id = ' + \
                                      str(netrica_id) + u', os_netrica_code = ' + \
                                      os_netrica_code + u', createDatetime = ' + start_date_local)
        except Exception as e:
            self.logger.error(u'Ошибка записи в базу данных')
            self.logger.error(str(e.args))
            traceback.print_exc(file=sys.stdout)
        else:
            if return_flag:
                self.logger.debug(u'Запись в базу успешно сохранена')
            return return_flag

    def send_beds_info(self, json_data):
        self.logger.debug(u'Отправка данных в сервис')
        headers = {'Content-Type': 'application/fhir+json', 'Authorization': 'N3 ' + config.AUTH_TOKEN, }
        data = json.dumps(json_data)
        response = requests.post(config.URL + config.BUNDLE_RESOURCE, data=data, headers=headers)
        # self.logger.debug(json_data)
        # self.logger.debug(str(json.dumps(response.json(), indent=4)))
        try:
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.debug('Ошибка обмена:')
                self.logger.debug(response.json().get('issue')[0].get('details').get('text'))
                return
        except:
            self.logger.debug(str(json.dumps(response.json(), indent=4)))
            return


    def save_info_to_logger_db(self, os_netrica_code):
        u"""
            функция логирует данные из базы в базу logger для одного заданного guid в таблице orgStructure.
            В итоге в таблице логгера для одного guid может быть много orgStructure_id.
            Записываем по не по guid, а еще и по orgStructure_id, потому что нужен net_id.
        :return: None
        """
        self.logger.debug(u'Запрос данных из базы')
        db = self.db
        date = now_in_db(db)
        date = date.strftime('%Y-%m-%dT%H:%M:%S')
        db_logger = self.db_logger
        query = query_netrica_code_count.format(os_netrica_code=os_netrica_code)
        with db.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()
        records = records_to_dict_list(records, ['bed_netrica_Code', 'os_netrica_code', 'net_id', 'orgStructure_id',
                                                 'org_netrica_code'])
        if not records:
            self.logger.debug(u'Записи о койках отсутствуют для подразделения: ' + os_netrica_code)
            return None
        else:
            self.logger.debug(u'Записи для guid: ' + os_netrica_code)
        for rec in records:
            bed_netrica_code = int(rec.get('bed_netrica_Code'))
            org_structure_id = int(rec.get('orgStructure_id'))
            org_netrica_code = str(rec.get('org_netrica_code'))
            # количество детских коек смотрим по OrgStructure.net_id, т.к. age не заполняется
            net_id = rec.get('net_id')
            self.logger.debug(u'Код профиля коек: ' + str(bed_netrica_code) + u', подразделение: ' + str(org_structure_id))
            insert_cols = {
                'netrica_id': "''",
                'orgStructure_id': str(org_structure_id),
                'os_netrica_code': "'" + str(os_netrica_code) + "'",
                'netricaBed_code': "'" + str(bed_netrica_code) + "'",
                'createDatetime': "'" + date + "'",
                'modifyDatetime': "'" + date + "'",
                'org_netrica_code': "'" + org_netrica_code + "'",
            }
            msg_list = []
            for j in range(len(self.cdf['json_names'])):
                # если детское отделение, то только детские койки
                if net_id == 2 and self.cdf['json_names'][j] in ('FreeBedCountFemale', 'FreeBedCountMale'):
                    count = 0
                elif net_id != 2 and self.cdf['json_names'][j] == 'FreeBedCountChild':
                    count = 0
                else:
                    query = self.cdf['queries'][j].format(
                        OrgStructure_id=org_structure_id, netrica_bed_Code=bed_netrica_code, date=date)
                    with db.cursor() as cur:
                        cur.execute(query)
                        count = cur.fetchone()[0]
                insert_cols[self.cdf['json_names'][j]] = str(count)
                # msg_list.append(self.cdf['json_names'][j] + u' = ' + str(count))

            # если койки с sex=0 (бесполые), то делим пополам между М и Ж
            if net_id != 2:
                free_bed_count = int(insert_cols['FreeBedCount'])
                free_bed_count_male = int(insert_cols['FreeBedCountMale'])
                free_bed_count_female = int(insert_cols['FreeBedCountFemale'])
                free_bed_count_female = free_bed_count_female + (
                            free_bed_count - free_bed_count_male - free_bed_count_female) // 2
                insert_cols['FreeBedCountFemale'] = str(free_bed_count_female)
                insert_cols['FreeBedCountMale'] = str(free_bed_count - free_bed_count_female)
            msg_list = [self.cdf['json_names'][j] + u' = ' + insert_cols[self.cdf['json_names'][j]] for j in
                        range(len(self.cdf['json_names']))]
            self.logger.debug(u', '.join(msg_list))
            self.logger.debug(u'Запись данных в базу данных ' + config.LOGGER_DB_NAME)
            columns = ', '.join(insert_cols.keys())
            values = ', '.join(insert_cols.values())
            try:
                with db_logger.cursor() as cur:
                    query = '''
                        INSERT INTO {logger_table} ({columns}) VALUES ({values});
                    '''.format(
                        logger_table=get_logger_table_name(),
                        columns=columns,
                        values=values)
                    cur.execute(query)
                db_logger.commit()
            except Exception as e:
                self.logger.error(u'Ошибка записи в базу данных')
                self.logger.error(str(e.args))
                traceback.print_exc(file=sys.stdout)
            else:
                self.logger.debug(u'Запись успешно сохранена')

    def get_beds_info(self, date, os_netrica_code):
        u"""
            Собирает данные из таблицы логгера в json по одному os_netrica_code.
        :param date: дата в локальном времени, тип Datetime
        :return: словарь объектов нетрики
        """
        if date < now_in_db(self.db) - timedelta(days=1):
            self.logger.info(u'Дата не должна отличаться от текущей больше, чем на сутки')
            return None
        date_str = date.strftime("%Y-%m-%dT%H:%M:%S")
        self.logger.debug(u'Запрос из логгера данных для подразделения: ' + os_netrica_code)
        db = self.db_logger
        os_netrica_code_cond = "and os_netrica_code = '{os_netrica_code}'".format(os_netrica_code=os_netrica_code)
        query = query_counts_on_date.format(logger_table=get_logger_table_name(),
                                            os_netrica_code_cond=os_netrica_code_cond,
                                            start_date=date_str)
        columns = ['netrica_id', 'createDatetime', 'netricaBed_code',
                   'os_netrica_code']
        columns.extend(self.cdf['json_names'])
        with db.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()
        if records:
            records = records_to_dict_list(records, columns)
            date_utc = records[0].get('createDatetime') - self.td
            date_utc = date_utc.replace(tzinfo=pytz.FixedOffset(0))
            date_utc = date_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            self.logger.debug(
                u'Записи с датой большей или равной дате ' + date_str + u' отсутствуют (местное время)')
            return None
        bundle_json = []
        for rec in records:
            bed_netrica_code = int(rec.get('netricaBed_code'))
            self.logger.debug(u'Код профиля коек: ' + str(bed_netrica_code))
            extension = [
                {
                    "url": "ActualOn",
                    "valuePeriod": {
                        "start": date_utc
                    }
                }
            ]
            msg_list = []
            for j in range(len(self.cdf['json_names'])):
                count = int(rec.get(self.cdf['json_names'][j]))

                extension.insert(0, {
                    "url": self.cdf['json_names'][j],
                    "valueInteger": count
                })
                msg_list.extend([self.cdf['json_names'][j] + u' = ' + str(count)])
            resource = {
                "resource": {
                    "resourceType": "HealthcareService",
                    "extension": extension,
                    "providedBy": {
                        "reference": "Organization/" + os_netrica_code
                    },
                    "characteristic": [
                        {
                            "coding": [
                                {
                                    "system": "urn:oid:1.2.643.5.1.13.2.1.1.221",
                                    "version": "1",
                                    "code": str(bed_netrica_code)
                                }
                            ]
                        }
                    ]
                }
            }
            bundle_json.append(resource)
            self.logger.debug(u', '.join(msg_list))
        bundle_json = {
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": bundle_json
        }
        self.logger.debug(u'Собраны данные в JSON формате')
        return bundle_json

    def parse_netrica_codes(self, date, netrica_codes):
        u"""
            Делает из строки гуидов организаций и подструктур лист гуидов только оподструктур
        :param netrica_codes: - строка переданных гуидов
        :param date: - дата в локальном времени, тип Datetime
        :return: лист гуидов уже только подструктур (orgStructure)
        """
        db = self.db
        date_str = date.strftime("%Y-%m-%dT%H:%M:%S")
        netrica_codes_splited = netrica_codes.split(',')
        if netrica_codes:
            query = \
                '''
                    select distinct os_netrica_code
                    from {logger_table}
                    where
                        createDatetime >= '{start_date}'
                        and (org_netrica_code in ({netrica_codes})
                                        or os_netrica_code in ({netrica_codes}));
                '''
        else:
            # если гуиды не заданы, возвращяем первые после заданной даты записи
            query = \
                '''
                    select distinct os_netrica_code
                    from {logger_table}
                    where
                        createDatetime =
                        (select createDatetime
                            from {logger_table}
                            where createDatetime >= '{start_date}'
                            limit 1);
                '''
        netrica_codes_str = ', '.join(["'" + code + "'" for code in netrica_codes_splited])
        query = query.format(logger_table=get_logger_table_name(), netrica_codes=netrica_codes_str, start_date=date_str)
        with db.cursor() as cur:
                cur.execute(query)
                records = cur.fetchall()
                if len(records) == 0:
                    self.logger.debug(
                        u'Записи с датой большей или равной дате ' + date_str + u' отсутствуют (местное время)')
                    return []
                if netrica_codes and len(records) < len(netrica_codes_splited):
                    self.logger.debug(
                        u'Один или несколько giud-ов не найдено в таблице логгера на дату большую или равную дате '
                            + date_str + u' (местное время)')
                    return []
        return [rec[0] for rec in records]


class Dataframe(dict):
    def __init__(self, columns, values, queries, json_names):
        # if len(values) != len(columns):
        #     raise Exception("Bad values")
        self['columns'] = columns
        self['values'] = values
        self['queries'] = queries
        self.index = values[0]
        self['json_names'] = json_names

    def get_value(self, i, j):
        return self['values'][j][i]

    def set_value(self, i, j, value):
        self['values'][j][i] = value

    def get_query(self, i):
        return self['queries'][i]


def now_in_db(db):
    query = 'SELECT now();'
    with db.cursor() as cur:
        cur.execute(query)
        record = cur.fetchone()
    return record[0]


def strptime_default(datetime_str, format, default=None):
    try:
        d = datetime.strptime(datetime_str, format)
    except ValueError:
        return default
    else:
        return d


def wsgi_app(start_date, netrica_codes, db=None, db_logger=None, data=None):
    # start_date - дата в UTC времени типа string, формата 'yyyy-MM-ddThh:mm:ssZ'
    # netrica_codes - строка переданных гуидов
    def send_request(start_date, netrica_codes, db, db_logger, data=None):
        if not data:
            data = CBedsExchange(db, db_logger, get_logger())
        if not start_date:
            data.logger.error(u'Дата должна быть указана')
            return 400
        offset, td = set_offset(db)
        start_date = strptime_default(start_date, "%Y-%m-%dT%H:%M:%SZ")
        if not start_date:
            data.logger.error(u"Дата должна быть в формате yyyy-MM-ddThh:mm:ssZ")
            return 400
        start_date.replace(tzinfo=pytz.FixedOffset(0))
        td = timedelta(minutes=offset)
        start_date_local = start_date + td
        start_date_local.replace(tzinfo=pytz.FixedOffset(offset))
        os_netrica_codes = data.parse_netrica_codes(start_date_local, netrica_codes)
        if not os_netrica_codes:
            return 400
        for code in os_netrica_codes:
            result_json = data.get_beds_info(start_date_local, code)
            if not result_json:
                status = 400
                break
            result_json = data.send_beds_info(result_json)
            if result_json and data.save_id_to_db(result_json, code):
                status = 200
                continue
            else:
                status = 500
                break
        return status

    if not db:
        try:
            db = pymysql.connect(**config.DB_CONNECTION_INFO)
            if not db_logger:
                db_logger = pymysql.connect(**config.DB_LOGGER)
        except Exception as e:
            get_logger().exception(e)
            status = 500
        else:
            status = send_request(start_date, netrica_codes, db, db_logger, data)
        finally:
            if db is not None:
                db.close()
            if db_logger is not None:
                db_logger.close()
            return status
    else:
        return send_request(start_date, netrica_codes, db, db_logger, data)


def main():
    parser = ArgumentParser(description='Netrika beds fund exchange')
    parser.add_argument('--send',
                        '-s',
                        dest='send_result',
                        help='sending beds fund to KK',
                        action='store_true',
                        default='')
    parser.add_argument('--collect',
                        '-c',
                        dest='collect_beds_fund',
                        help='collecting beds fund info to logger DB',
                        action='store_true',
                        default=False)
    parser.add_argument('--date',
                        '-d',
                        dest='start_date',
                        help='start_date',
                        action='store',
                        default=False)
    parser.add_argument('--netricacodes',
                        '-nc',
                        dest='netrica_codes',
                        help='netrica_codes',
                        action='store',
                        default=False)
    parser.add_argument('--pidfile',
                        dest='pid_filename',
                        help='PID filename for process',
                        action='store_true',
                        default=chext('.pid'))
    parser.add_argument('--version',
                        '-v',
                        dest='version',
                        help='Service client version',
                        action='store_true',
                        default=False)
    options = parser.parse_args(sys.argv[1:])
    if options.version:
        from __init__ import __version__
        print(__version__)
        return

    if already_running(options.pid_filename):
        return

    logger = get_logger()
    try:
        db = pymysql.connect(**config.DB_CONNECTION_INFO)
        db_logger = pymysql.connect(**config.DB_LOGGER)
        data = CBedsExchange(db, db_logger, logger)
        create_pid_file(options.pid_filename)
        msgList = []
        if options.send_result:
            date = ''
            netrica_codes = ''
            if options.start_date:
                date = options.start_date
            if options.netrica_codes:
                netrica_codes = options.netrica_codes
            wsgi_app(date, netrica_codes, db, db_logger, data)
        if options.collect_beds_fund:
            for org in config.ORGANISATIONS:
                data.save_info_to_logger_db(org)
        if msgList:
            logger.info('\n'.join([''] + msgList + ['*' * 120]))
    except Exception as e:
        logger.exception(e)
    finally:
        try:
            if db is not None:
                db.close()
            if db_logger is not None:
                db_logger.close()
        except Exception as e:
            pass
        remove_pid_file(options.pid_filename)


if __name__ == '__main__':
    main()
