import requests
import sqlite3
import os, importlib
from datetime import datetime
import time
import logging
import json


# Константы
logging.basicConfig(filename="log/app.log", level=logging.DEBUG, format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
logger = logging.getLogger(__name__)
db_path = os.getcwd() + os.sep + 'src' + os.sep + 'db.sqlite'
# Храним чувствительные данные в переменной окружения
# Это значение по умолчанию на случай, если переменной окружения не будет
os.environ.setdefault('SETTINGS_MODULE', 'config')
# Импортируем модуль, указанный в переменной окружения
config = importlib.import_module(os.getenv('SETTINGS_MODULE'))


def get_json():  # забираем json
    s = requests.session()
    url = 'http://10.10.137.53:8080/monitor/health'
    r = s.get(url)
    if r.status_code == 200:
        return r.json()
    else:
        alarmtext = f'При попытке опросить redis код ответа {r.status_code}. Мониторинг acqpc встал на паузу 350с'
        do_alarm(alarmtext)


def get_cursor_id(table_name):
    conn = sqlite3.connect(db_path)  # Инициируем подключение к БД
    cursor = conn.cursor()
    line_id = cursor.execute(f"SELECT seq FROM sqlite_sequence WHERE name='{table_name}'").fetchall()[0][0]
    conn.commit()
    return line_id


def recording_data():  # разбираем данные и пишем в базу

    data = get_json()
    conn = sqlite3.connect(db_path)  # Инициируем подключение к БД
    cursor = conn.cursor()

    acqpc_status = data['status']
    acqpc_datasource_status = data['components']['db']['components']['acqpcDatasource']['status']
    autopays_datasource_status = data['components']['db']['components']['autopaysDatasource']['status']
    total_disk_space = data['components']['diskSpace']['details']['total']
    free_disk_space = data['components']['diskSpace']['details']['free']
    free_disk_space_procent = round(free_disk_space / total_disk_space * 100, 2)
    ping_status = data['components']['ping']['status']
    rabbit_status = data['components']['rabbit']['status']
    rabbit_version = data['components']['rabbit']['details']['version']
    redis_status = data['components']['redis']['status']
    redis_version = data['components']['redis']['details']['version']
    now = datetime.now()
    request_time = now.strftime('%d-%m-%Y %H:%M:%S')
    # Пора все записать в БД
    cursor.execute(f'INSERT INTO all_data VALUES (Null, "{acqpc_status}", "{acqpc_datasource_status}", "{autopays_datasource_status}", "{free_disk_space_procent}", "{ping_status}", "{rabbit_status}", "{rabbit_version}", "{redis_status}", "{redis_version}", "{request_time}")')
    conn.commit()
    check_values()


def check_values():
    conn = sqlite3.connect(db_path)  # Инициируем подключение к БД
    cursor = conn.cursor()

    last_id = get_cursor_id('all_data')
    pre_id = last_id - 1
    val = cursor.execute(f"SELECT acqpc_status, acqpc_datasource_status, autopays_datasource_status, ping_status, "
                         f"rabbit_status, rabbit_version, redis_status, redis_version FROM all_data WHERE id = "
                         f"'{last_id}' OR id = '{pre_id}'").fetchall()
    conn.commit()
    values_names = {  # Чтоб выводить аларм на человеческом языке
        0: 'Статус ACQ-PC',
        1: 'Статус acqpc_datasource',
        2: 'Статус autopays_datasource',
        3: 'Статус ping до ACQ-PC',
        4: 'Статус Rabbit',
        5: 'Версия Rabbit',
        6: 'Статус Redis',
        7: 'Версия Redis'
    }
    try:  # По итерации сравниваются значения двух массивов. 8 - это кол-во элементов в каждом массиве
        for i in range(0, 8):
            if val[0][i] != val[1][i]:
                logger.info(f'Value "{values_names[i]}" changed whith {val[0][i]} to {val[1][i]}')
                alarmtext = f'Значение "{values_names[i]}" сменилось с {val[0][i]} на {val[1][i]}'
                do_alarm(alarmtext)
                logger.info('Worked Alarm')
    except IndexError:
        print('Индексы кончились')
        logger.error('Out of indexes in def check_values(). Check What are you download from bd.')


def do_alarm(alarmtext):  # отправка сообщения в канал slack
    headers = {"Content-type": "application/json"}
    url = config.webhook_url
    payload = {"text": f"{alarmtext}"}
    requests.post(url, headers=headers, data=json.dumps(payload))


if __name__ == '__main__':
    while True:
        try:
            recording_data()
            print('data recorded')
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info('Program has been stop manually')
        except TypeError:  # Если ошибка записи данных, то пытаемся снова
            logger.error(f'TypeError Exception', exc_info=True)
            time.sleep(650)
        except Exception:
            logger.error(f'Other except error Exception', exc_info=True)
