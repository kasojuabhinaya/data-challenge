import configparser
import urllib.request
import time
import logging

from kafka import KafkaProducer


class Producer:
    DELAY = 2
    BATCH_SIZE = 50

    def __init__(self):

        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s',
            level=logging.INFO,
            datefmt='%m/%d/%Y %I:%M:%S%p'
            )
        self.logger = logging.getLogger('producer-logging')
        pass
    def __read_ini(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.kafka_host = config['KAFKA']['host']
        self.kafka_topic = config['KAFKA']['topic']
        self.data_url = config['DATA']['url']
        logging.info('Reading ini file')

        logging.info(self.kafka_host, self.kafka_topic)

    def __set_producer(self):
        try:
            self.producer = KafkaProducer(bootstrap_servers=self.kafka_host)
        except Exception as e:
            print(e)
            exit(-1)

    def __produce(self, data):
        """
        Writes data into Kafka
        :param data: data to be written into Kafka
        :type data: bytes
        """
        try:
            self.producer.send(self.kafka_topic, value=data).get(timeout=10)
            logging.info('produced => {}'.format(data.decode('utf-8')))
            #print('produced => {}'.format(data.decode('utf-8')))
        except Exception as e:
            print(e)

    def __handler(self):
        """
        Reads from the url line by line and does minor cleaning to create a
        valid json string in order to write to Kafka
        """
        t1 = time.time()

        cnt = 0

        with urllib.request.urlopen(self.data_url) as myfile:
            while True:
                if cnt % Producer.BATCH_SIZE == 0:
                    logging.info('parsed {}, sleeping for {}'.format(cnt, Producer.DELAY))
                    time.sleep(Producer.DELAY)

                line = myfile.readline()
                if not line:
                    logging.warning('No lines to read')
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

        t2 = time.time()

        logging.info('Total time taken:{}'.format(t2 - t1))

    def run(self):
        logging.info('Running')
        self.__read_ini()
        self.__set_producer()
        self.__handler()
        # print(self.producer.metrics())


if __name__ == "__main__":
    producer = Producer()
    producer.run()
