import configparser
import datetime
import json
import logging
import os

import pg8000
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def __get_kafka_config():
    config = configparser.ConfigParser()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_ini_path = current_dir + '/config.ini'
    config.read(config_ini_path)

    kafka_host = config['KAFKA']['host']
    kafka_topic = config['KAFKA']['topic']
    kafka_group_id = config['KAFKA']['group_id']

    return kafka_host, kafka_topic, kafka_group_id


def __get_db_config():
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


def __is_valid_ip_format(ip):
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


def __is_valid_date(date_value):
    try:
        datetime.datetime.strptime(date_value, '%d/%m/%Y')
    except ValueError:
        return False

    return True


def __is_valid(row):
    valid_ip_format = __is_valid_ip_format(row.get('ip_address'))
    valid_date = __is_valid_date(row.get('date'))

    return valid_ip_format and valid_date


def __get_updated_count(new_scores, total):
    if not total:
        total = 0
    return sum(new_scores) + total


def __write_to_db(db_config, sql, data):
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


def __add_stats_db(rdd, topic, db_config):
    sql = """
        INSERT INTO consumer.stats (topic, total)
        VALUES (%s, %s)
    """
    rdd.foreach(lambda record: __write_to_db(db_config, sql, data=(topic, record,)))


def __countries_stats_db(rdd, topic, db_config):
    sql = """
        INSERT INTO consumer.country_stats (topic, country, total)
        VALUES (%s, %s, %s)
    """
    rdd.foreach(lambda record: __write_to_db(db_config, sql, data=(topic, record[0], record[1])))


def __streamer():
    s_logger = logging.getLogger('py4j.java_gateway')
    s_logger.setLevel(logging.ERROR)
    sc = SparkContext(appName="OetketStreaming")
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 1)

    kafka_host, kafka_topic, kafka_group_id = __get_kafka_config()

    print(kafka_host, kafka_topic, kafka_group_id)

    db_config = __get_db_config()

    kafka_stream = KafkaUtils.createStream(ssc,
                                           kafka_host,
                                           kafka_group_id,
                                           {kafka_topic: 1}
                                           )
    # add batch count to DB
    kafka_stream.count().foreachRDD(
        lambda x: __add_stats_db(x, kafka_topic, db_config)
    )

    count_this_batch = kafka_stream.count() \
        .map(lambda x: ('Items this batch: %s' % x))

    # Count by windowed time period
    count_windowed = kafka_stream.countByWindow(10, 1) \
        .map(lambda x: ('Items (10s window): %s' % x))

    parsed = kafka_stream.map(lambda item: json.loads(item[1]))
    parsed = parsed.filter(lambda item: __is_valid(item))

    valid_items_batch = parsed.count() \
        .map(lambda x: ('Valid items this batch: %s' % x))

    valid_items_window = parsed.countByWindow(10, 1) \
        .map(lambda x: ('Valid items (10s window): %s' % x))

    # Unique emails in the batch
    emails_count_batch = parsed.map(lambda item: item['email']) \
        .countByValue() \
        .count() \
        .map(lambda x: "Batch - Unique emails:\t %s" % x)

    # Unique emails in the Window
    emails_window_batch = parsed.map(lambda item: item['email']) \
        .countByValueAndWindow(10, 1) \
        .count() \
        .map(lambda x: "Window - Unique emails:\t %s" % x)

    countries_stream = parsed.map(lambda item: item['country'].capitalize())

    countries_batch_count = countries_stream.countByValue()

    # add country stats of the batch to DB
    countries_batch_count.foreachRDD(
        lambda x: __countries_stats_db(x, kafka_topic, db_config)
    )

    # Top represented countries
    top_countries_batch = countries_batch_count \
        .transform(lambda rdd: rdd.sortBy(lambda x: -x[1])) \
        .map(lambda x: "Batch - Most represented county:\t %s\t %s" % (x[0], x[1]))

    # Least represented countries
    least_countries_batch = countries_batch_count \
        .transform(lambda rdd: rdd.sortBy(lambda x: x[1])) \
        .map(lambda x: "Batch - Least represented county:\t %s\t %s" % (x[0], x[1]))

    countries_window_count = countries_stream.countByValueAndWindow(10, 1)

    # Most represented countries - window
    top_countries_window = countries_window_count \
        .transform(lambda rdd: rdd.sortBy(lambda x: -x[1])) \
        .map(lambda x: "Window - Most represented county:\t %s\t %s" % (x[0], x[1]))

    # Least represented countries
    least_countries_window = countries_window_count \
        .transform(lambda rdd: rdd.sortBy(lambda x: x[1])) \
        .map(lambda x: "Window - Least represented county:\t %s\t %s" % (x[0], x[1]))

    count_this_batch.union(count_windowed).pprint()
    valid_items_batch.union(valid_items_window).pprint()

    top_countries_batch.union(top_countries_window).pprint(5)
    least_countries_batch.union(least_countries_window).pprint(5)

    emails_count_batch.union(emails_window_batch).pprint(2)

    count_pairs = kafka_stream.count() \
        .map(lambda x: ('count', x))

    running_counts = count_pairs.updateStateByKey(__get_updated_count)
    running_counts.pprint()

    return ssc


def run():
    ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v2', lambda: __streamer())
    ssc.start()
    ssc.awaitTermination()

    __streamer()


if __name__ == "__main__":
    run()