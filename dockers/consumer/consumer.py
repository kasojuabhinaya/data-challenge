import json
import logging

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
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


def clean_entry():
    def meth(row):
        # Uppercase first letter of the country
        # e.g. Germany, germany, GeRmany -> Germany
        country = row['country'].lower().capitalize()
        row['country'] = country
        return Row(**row)
    return meth


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


def add_stats_db(rdd, topic, db_config):
    sql = """
        INSERT INTO consumer.stats (topic, total)
        VALUES (%s, %s)
    """
    rdd.foreach(lambda record: write_to_db(db_config, sql, data=(topic, record,)))


def countries_stats_db(rdd, topic, db_config):
    sql = """
        INSERT INTO consumer.country_stats (topic, country, total)
        VALUES (%s, %s, %s)
    """
    rdd.foreach(lambda record: write_to_db(db_config, sql, data=(topic, record[0], record[1])))


def valid_items_stats(valid_items):
    valid_items_batch = valid_items.count() \
        .map(lambda x: ('Valid items this batch: %s' % x))
    valid_items_window = valid_items.countByWindow(10, 1) \
        .map(lambda x: ('Valid items (10s window): %s' % x))
    valid_items_batch.union(valid_items_window).pprint()


def stream_running_stats(kafka_stream):
    count_pairs = kafka_stream.count() \
        .map(lambda x: ('count', x))
    running_counts = count_pairs.updateStateByKey(get_updated_count)
    running_counts.pprint()


def top_country_stats(countries_batch_count, countries_window_count):
    # Top represented countries
    top_countries_batch = countries_batch_count \
        .transform(lambda rdd: rdd.sortBy(lambda x: -x[1])) \
        .map(lambda x: "Batch - Most represented county:\t %s\t %s" % (x[0], x[1]))

    # Most represented countries - window
    top_countries_window = countries_window_count \
        .transform(lambda rdd: rdd.sortBy(lambda x: -x[1])) \
        .map(lambda x: "Window - Most represented county:\t %s\t %s" % (x[0], x[1]))
    top_countries_batch.union(top_countries_window).pprint(5)


def least_country_stats(countries_batch_count, countries_window_count):
    # Least represented countries
    least_countries_batch = countries_batch_count \
        .transform(lambda rdd: rdd.sortBy(lambda x: x[1])) \
        .map(lambda x: "Batch - Least represented county:\t %s\t %s" % (x[0], x[1]))
    # Least represented countries
    least_countries_window = countries_window_count \
        .transform(lambda rdd: rdd.sortBy(lambda x: x[1])) \
        .map(lambda x: "Window - Least represented county:\t %s\t %s" % (x[0], x[1]))
    least_countries_batch.union(least_countries_window).pprint(5)


def unique_email_stats(parsed, window_interval, sliding_interval):
    # Unique emails in the batch
    emails_count_batch = parsed.map(lambda item: item['email']) \
        .countByValue() \
        .count() \
        .map(lambda x: "Batch - Unique emails:\t %s" % x)
    # Unique emails in the Window
    emails_window_batch = parsed.map(lambda item: item['email']) \
        .countByValueAndWindow(window_interval, sliding_interval) \
        .count() \
        .map(lambda x: "Window - Unique emails:\t %s" % x)
    emails_count_batch.union(emails_window_batch).pprint(2)


def process_streamer():
    sliding_interval = 1
    window_interval = 10

    ssc = get_streaming_context(sliding_interval)

    db_config = get_db_config()

    kafka_host, kafka_topic, kafka_group_id = get_kafka_config()
    kafka_stream = KafkaUtils.createStream(ssc,
                                           kafka_host,
                                           kafka_group_id,
                                           {kafka_topic: 1}
                                           )
    # add batch count to DB
    kafka_stream.count().foreachRDD(
        lambda x: add_stats_db(x, kafka_topic, db_config)
    )
    count_this_batch = kafka_stream.count() \
        .map(lambda x: ('Items this batch: %s' % x))

    # Count by windowed time period
    count_windowed = kafka_stream.countByWindow(window_interval, sliding_interval) \
        .map(lambda x: ('Items (window): %s' % x))

    count_this_batch.union(count_windowed).pprint()

    # Load the streamed batch - parsed
    # Filter out improper data - valid items
    parsed = kafka_stream.map(lambda item: json.loads(item[1]))
    valid_items = parsed.filter(lambda item: is_valid(item))
    cleaned_stream = valid_items.map(clean_entry())

    valid_items_stats(cleaned_stream)
    unique_email_stats(cleaned_stream, window_interval, sliding_interval)

    country_stream = cleaned_stream.map(lambda x: x['country'])
    country_stream.pprint()
    countries_batch_count = country_stream.countByValue()
    countries_window_count = country_stream.countByValueAndWindow(window_interval, sliding_interval)

    # Top represented country stats
    top_country_stats(countries_batch_count, countries_window_count)

    # least represented country stats
    least_country_stats(countries_batch_count, countries_window_count)

    # add country stats of the batch to DB
    countries_batch_count.foreachRDD(
        lambda x: countries_stats_db(x, kafka_topic, db_config)
    )

    # stream running stats
    stream_running_stats(kafka_stream)

    return ssc


def get_streaming_context(sliding_interval):
    s_logger = logging.getLogger('py4j.java_gateway')
    s_logger.setLevel(logging.ERROR)
    sc = SparkContext(appName="OetkerStreaming")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, sliding_interval)
    return ssc


def run():
    ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v2', lambda: process_streamer())
    ssc.start()
    ssc.awaitTermination()
    process_streamer()


if __name__ == "__main__":
    run()
