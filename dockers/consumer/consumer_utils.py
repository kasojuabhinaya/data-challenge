import configparser
import datetime
import os
import pg8000


def get_kafka_config():
    config = configparser.ConfigParser()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_ini_path = current_dir + '/config.ini'
    config.read(config_ini_path)

    kafka_host = config['KAFKA']['host']
    kafka_topic = config['KAFKA']['topic']
    kafka_group_id = config['KAFKA']['group_id']

    return kafka_host, kafka_topic, kafka_group_id


def get_db_config():
    config = configparser.ConfigParser()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_ini_path = current_dir + '/config.ini'
    config.read(config_ini_path)

    db = config['DATA']['db']
    host = config['DATA']['host']
    user = config['DATA']['user']
    password = config['DATA']['password']
    port = int(config['DATA']['port'])

    db_config = {
        'db': db,
        'host': host,
        'user': user,
        'password': password,
        'port': port
    }

    return db_config


def is_valid_ip_format(ip):
    if not ip:
        return False

    a = ip.split('.')

    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True


def is_valid_date(date_value):
    try:
        datetime.datetime.strptime(date_value, '%d/%m/%Y')
    except ValueError:
        return False

    return True


def is_valid(row):
    valid_ip_format = is_valid_ip_format(row.get('ip_address'))
    valid_date = is_valid_date(row.get('date'))

    return valid_ip_format and valid_date


def get_updated_count(new_scores, total):
    if not total:
        total = 0
    return sum(new_scores) + total


def write_to_db(db_config, sql, data):
    pg_con = None

    try:
        pg_con = pg8000.connect(
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['db'],
        )

        cur = pg_con.cursor()
        cur.execute(sql, data)
        pg_con.commit()
        pg_con.close()
    except Exception as e:
        if pg_con:
            pg_con.close()
        print(e)