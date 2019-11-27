import json
import logging

import pg8000
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from consumer.consumer_utils import get_db_config, get_kafka_config, get_updated_count, write_to_db, is_valid




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


def streamer(ssc, sliding_interval, window_interval):

    kafka_host, kafka_topic, kafka_group_id = get_kafka_config()
    print(kafka_host, kafka_topic, kafka_group_id)

    db_config = get_db_config()

    kafka_stream = KafkaUtils.createStream(ssc,
                                           kafka_host,
                                           kafka_group_id,
                                           {kafka_topic: 1}
                                           )
    # add batch count to DB
    kafka_stream = kafka_stream.count().foreachRDD(
        lambda x: add_stats_db(x, kafka_topic, db_config)
    )
    count_this_batch = kafka_stream.count() \
        .map(lambda x: ('Items this batch: %s' % x))

    # Count by windowed time period
    count_windowed = kafka_stream.countByWindow(window_interval, sliding_interval) \
        .map(lambda x: ('Items (10s window): %s' % x))

    parsed = kafka_stream.map(lambda item: json.loads(item[1]))
    parsed = parsed.filter(lambda item: is_valid(item))

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
        lambda x: countries_stats_db(x, kafka_topic, db_config)
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

    running_counts = count_pairs.updateStateByKey(get_updated_count)
    running_counts.pprint()

    return ssc


def count_batch(kafka_stream, kafka_topic, db_config):
    # add batch count to DB
    kafka_stream = kafka_stream.count().foreachRDD(
        lambda x: add_stats_db(x, kafka_topic, db_config)
    )
    count_this_batch = kafka_stream.count() \
        .map(lambda x: ('Items this batch: %s' % x))
    return count_this_batch


def run():
    ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v2', lambda: streamer())
    ssc.start()
    ssc.awaitTermination()
    s_logger = logging.getLogger('py4j.java_gateway')
    s_logger.setLevel(logging.ERROR)
    sc = SparkContext(appName="OetkerStreaming")
    sc.setLogLevel("ERROR")

    sliding_interval = 1
    window_interval = 10
    ssc = StreamingContext(sc, sliding_interval)
    streamer(ssc, sliding_interval, window_interval)


if __name__ == "__main__":
    run()
