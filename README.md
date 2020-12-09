## Сервис обмена данными с коечным фондом КК

##### 1. Определение дистрибутива Linux
```shell script
uname -a; lsb_release -a; cat /etc/*release
```
##### 2. Подключаемся в серверу
```shell script
ssh  root@192.168.0.3
# пароль: ******
```
##### 3. Проверяем, есть ли весия python 3.5
```shell script
which python
ls -la /usr/bin/ | grep python
python -V
python3.5 -V
python3 -V
# Ubuntu
apt-get update
# Fedora
yum update
# Если нет, устанавливаем python 3.5.9 по инструкции для Ubuntu: https://tecadmin.net/install-python-3-5-on-ubuntu/
# Для Fedora https://tecadmin.net/install-python-3-5-on-centos/
```
##### 4. Установка приложения
```shell script
cd /var/www/html/
svn co --username arkkav svn://192.168.0.3/s11/appendix/n3bedsKK
# пароль: ******
cd ./n3bedsKK
```
##### 5. Настройка виртуального окружения
```shell script
virtualenv venv -p $(which python3.5)
. venv/bin/activate
pip install -r requirements.txt
```
Если нет интернета:
```shell script
# там, где есть интернет, выполнить (складывает все пакеты в текущую папку)
pip download -r requirements.txt
scp -r /home/vista/PycharmProjects/111/wheels root@192.168.0.3:/var/www/html/n3bedsKK/wheels
# пароль: ******
# на сервере:
pip install --no-index /var/www/html/n3bedsKK/wheels/*
```
##### 6. База данных
```shell script
mysql
CREATE SCHEMA IF NOT EXISTS logger;
# здесь применяем SQL из папки dbupdate
exit
```
##### 7. Настройки config.py
```shell script
nano config.py
```
DB_CONNECTION_INFO - настройки базы, из которой собираем информацию\
DB_LOGGER - базу, куда собираем информацию (адрес можно посмотреть по команде ifconfig)\
ORGANISATION - организация из справочника 1.2.643.2.69.1.1.1.64, по который собираем информацию\
DEBUG = True - режим отладки\
REGION - регион сервиса netrica
##### 8. Сервер в systemd
```shell script
touch /etc/systemd/system/vservice_bedsfund_server.service
chmod 664 /etc/systemd/system/vservice_bedsfund_server.service
nano /etc/systemd/system/vservice_bedsfund_server.service
```
Пишем:
```shell script
[Unit]
Description=bedsfund server.
Requires=network.target
After=network.target

[Service]
Restart=always
RestartSec=10
Environment=FLASK_ENV=production
ExecStart=/var/www/html/n3bedsKK/venv/bin/python3 /var/www/html/n3bedsKK/server.py
WorkingDirectory=/var/www/html/n3bedsKK

[Install]
WantedBy=multi-user.target
```
##### 9. Сборщика информации в systemd
```shell script
touch /etc/systemd/system/vservice_bedsfund_collector.service
chmod 664 /etc/systemd/system/vservice_bedsfund_collector.service
nano /etc/systemd/system/vservice_bedsfund_collector.service
```
Пишем:
```shell script
[Unit]
After=mariadb.service
Requires=mariadb.service
Description=bedsfund info collector.

[Service]
Type=simple
StandardOutput=syslog
StandardError=syslog
WorkingDirectory=/var/www/html/n3bedsKK
ExecStart=/var/www/html/n3bedsKK/venv/bin/python3 /var/www/html/n3bedsKK/exchange.py -c

[Install]
WantedBy=multi-user.target
```
##### 10. Таймер для сборщика в systemd
```shell script
touch /etc/systemd/system/vservice_bedsfund_collector.timer
chmod 664 /etc/systemd/system/vservice_bedsfund_collector.timer
nano /etc/systemd/system/vservice_bedsfund_collector.timer
```
Пишем:
```shell script
[Unit]
Description=RUN_BEDSFUND

[Timer]
OnBootSec=0min
OnCalendar=*:0/15

[Install]
WantedBy=timers.target
```
##### 11. Настройка сети
Добавяем правила для таблиц маршрутизации, чтоб открыть эти порты для трафика (в Feroda и CentOS по-умолчанию весь трафик блокируется):
```shell script
nano /etc/sysconfig/iptables
```
Прописываем свои порты ко всем остальным открытым портам в строку вида:\
*-A INPUT -p tcp -m conntrack --ctstate NEW -m multiport --dports {порты} -j trusted*

Сохраняем файл, обновляем таблицу:
```shell script
rd
```
##### 12. Запускаем сервисы
```shell script
systemctl daemon-reload
systemctl enable vservice_bedsfund_collector.timer
systemctl enable vservice_bedsfund_server.service
systemctl start vservice_bedsfund_collector.timer
systemctl start vservice_bedsfund_server.service
```
##### 13. Проверка работы сервиса
Запускаем сбор информации:
```shell script
/var/www/html/n3bedsKK/venv/bin/python3.5 exchange.py -c
```
Отправка информации в сервер, запись id из сервиса netrica:
```shell script
/var/www/html/n3bedsKK/venv/bin/python3.5 exchange.py -s -d 2020-12-03T11:04:05Z
```
Смотрим лог:
```shell script
nano /var/www/html/n3bedsKK/log.log
```
Состояние сервисов и наличие в списке:
```shell script
systemctl status vservice_bedsfund_collector.timer
systemctl status vservice_bedsfund_server.service
systemctl list-units -t service --all | grep vservice
```
Смотрим, слушается ли наш порт:
```shell script
netstat -l -p -n 
```
Просмотр журнала:
```shell script
journalctl -u vservice_bedsfund_server.service
journalctl -u vservice_bedsfund_collector.timer
journalctl -u vservice_bedsfund_collector.service
```
Просмотр системного лога (варианты для Ubuntu, Fedora, CentOS):
```shell script
sudo cat /var/log/syslog | grep -i vservice_bedsfund
sudo cat /var/log/messages | grep -i vservice_bedsfund
sudo cat var/log/secure | grep -i vservice_bedsfund
```
Смотрим лог:
```shell script
cat /var/www/html/n3bedsKK/log.log
```
В браузере:\
<http://192.168.0.3:8000/?start_date=2020-12-03T11:04:05Z>

При необходимости чистим таблицу:
```shell script
delete from logger.NetricaBedsExchange;
commit;
```






  