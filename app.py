import requests
import sqlite3
import os
from datetime import datetime
import time
import logging
import json


# Константы
logging.basicConfig(filename="log/app.log", level=logging.INFO)
logger = logging.getLogger(__name__)
db_path = os.getcwd() + os.sep + 'src' + os.sep + 'db.sqlite'


def get_json():  # забираем json
    s = requests.session()
    url = 'http://10.10.137.53:8080/monitor/health'
    r = s.get(url)
    return r.json()


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
    request_time = datetime.now()
    # Пора все записать в БД
    cursor.execute(f'INSERT INTO all_data VALUES (Null, "{acqpc_status}", "{acqpc_datasource_status}", "{autopays_datasource_status}", "{free_disk_space_procent}", "{ping_status}", "{rabbit_status}", "{rabbit_version}", "{redis_status}", "{redis_version}", "{request_time}")')
    conn.commit()
    time.sleep(60)  # делаем функцию итеративной
    check_values()
    recording_data()


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
                logger.info(val[0][i], 'Не равно', val[1][i])
                alarmtext = f'Значение "{values_names[i]}" сменилось с {val[0][i]} на {val[1][i]}'
                do_alarm(alarmtext)
    except IndexError:
        print('Индексы кончились')
        logger.info('Кончились индексы в def check_values(). Проверь какие данные выгружаешь и сколько.')


def do_alarm(alarmtext):  # отправка сообщения в канал slack
    headers = {"Content-type": "application/json"}
    url = "https://hooks.slack.com/services/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"
    payload = {"text": f"{alarmtext}"}
    requests.post(url, headers=headers, data=json.dumps(payload))


if __name__ == '__main__':
    try:
        recording_data()
    except KeyboardInterrupt:
        print('Работа программы завершена')
        logger.info('Работа программы завершена вручную')
