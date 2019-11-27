import configparser
import os
import urllib.request
import time
import logging

import psycopg2
from kafka import KafkaProducer


class Producer:
    DELAY = 2
    BATCH_SIZE = 50

    def __init__(self, mode):
        self.mode = mode
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s',
            level=logging.INFO,
            datefmt='%m/%d/%Y %I:%M:%S%p'
        )

        self.logger = logging.getLogger('producer-logging')
        self.logger.info('Running with {}'.format(self.mode))

    def __read_ini(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.kafka_host = config['KAFKA']['host']
        self.kafka_topic = config['KAFKA']['topic']
        self.data_url = config['DATA']['url']

        self.db_cnn_str = config['DATA']['db']

        self.logger.info('Reading ini file')
        self.logger.info('Kafka host {} , topic {}'.format(self.kafka_host, self.kafka_topic))

    def __set_producer(self):
        try:
            self.producer = KafkaProducer(bootstrap_servers=self.kafka_host)
        except Exception as e:
            self.logger.error(e)
            exit(-1)

    def __add_stats_pg(self, total):
        pg_con = None
        try:
            pg_con = psycopg2.connect(self.db_cnn_str)
            cur = pg_con.cursor()
            sql = """
                INSERT INTO producer.stats(topic, total) 
                VALUES (%s, %s)
            """
            cur.execute(sql, (self.kafka_topic, total,))
            pg_con.commit()
            cur.close()
            pg_con.close()
        except Exception as e:
            if pg_con:
                pg_con.close()
            self.logger.error(e)

    def __produce(self, data):
        """
        Writes data into Kafka
        :param data: data to be written into Kafka
        :type data: bytes
        """
        try:
            self.producer.send(self.kafka_topic, value=data)
            self.producer.flush()
        except Exception as e:
            self.logger.error(e)

    def __handler(self):
        """
        Reads from the url line by line and does minor cleaning to create a
        valid json string in order to write to Kafka
        """
        t1 = time.time()

        cnt = 0

        with urllib.request.urlopen(self.data_url) as myfile:
            while True:
                line = myfile.readline()
                if not line:
                    self.logger.info('No lines to read')
                    break

                dc_line = line.decode('utf-8')

                if dc_line[0:1] == '[':
                    dc_line = dc_line[1:]
                if dc_line[-1] == ']':
                    dc_line = dc_line[0:-1]
                else:
                    dc_line = dc_line[0:-2]

                self.__produce(bytes(dc_line, 'utf-8'))
                cnt += 1

                if self.mode == 'delay':
                    if cnt % Producer.BATCH_SIZE == 0:
                        self.logger.info('parsed {}, sleeping for {}'.format(cnt, Producer.DELAY))
                        time.sleep(Producer.DELAY)

        t2 = time.time()

        self.__add_stats_pg(cnt)

        self.logger.info('Total time taken:{}'.format(t2 - t1))
        self.logger.info('Total produced:{}'.format(cnt))

    def run(self):
        self.__read_ini()
        self.__set_producer()
        self.__handler()
        # print(self.producer.metrics())


if __name__ == "__main__":
    mode = os.getenv('MODE', 'live').lower()
    producer = Producer(mode=mode)
    producer.run()
