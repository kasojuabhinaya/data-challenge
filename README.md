# data-challenge

This repo covers the following tasks:

1. Read data from a file hosted on web. 
2. Stream *(producer)* the data into **Kafka**.
3. Using **Spark**, read the stream *(consume)* the data.
   Provide stats like 
   - *total items in the batch*
   - *countries that are most represented*
   - *countries that are least represented*
   Same results are also calculated in a rolling window.
   
   Each poll for consumption is 1s, and the window is 10s, meaning 
   data of last 10s is reduced every second. 
   
   Note: rolling window is small as the data is small.
   
 4. Produced data and consumed data is written to a *Postgres DB (PG)* and can be monitored
   
 The solution is **Docker** based but can be partly run otherwise. 
 However, local installation of Kafka and any PG server can be used independently too.
 
 
### How to run - using docker
 
## Kafka
 
The docker image used is [docker-kafka by spotify](https://github.com/spotify/docker-kafka). 
To start the service
```buildoutcfg
docker run -p 2181:2181 -p 9092:9092 \
  --env ADVERTISED_HOST=<IP> \
  --env ADVERTISED_PORT=9092 \
  --env CONSUMER_THREADS=1 \
  --env TOPICS=oetker \
  --env ZK_CONNECT=kafka7zookeeper:2181/root/path \
  --env GROUP_ID=group-1 \
  spotify/kafka
```
 
On Mac

To start the service
```buildoutcfg
docker run -p 2181:2181 -p 9092:9092 \
  --env ADVERTISED_HOST=$(ipconfig getifaddr en0) \
  --env ADVERTISED_PORT=9092 \
  --env CONSUMER_THREADS=1 \
  --env TOPICS=oetker \
  --env ZK_CONNECT=kafka7zookeeper:2181/root/path \
  --env GROUP_ID=group-1 \
  spotify/kafka
```
   
 On Ubuntu
 ```buildoutcfg
docker run -p 2181:2181 -p 9092:9092 \
  --env ADVERTISED_HOST=$(hostname -I) \
  --env ADVERTISED_PORT=9092 \
  --env CONSUMER_THREADS=1 \
  --env TOPICS=oetker \
  --env ZK_CONNECT=kafka7zookeeper:2181/root/path \
  --env GROUP_ID=group-1 \
  spotify/kafka
```

### Postgres

```buildoutcfg
cd <dockers/postgres>
docker build --no-cache -t abhi/oetker-postgres .
docker run -it abhi/oetker-postgres
```
Note: The run recreates the necessary schemas, so data is not persisted.

### Consumer

```buildoutcfg
cd <dockers/consumer>
docker build --no-cache -t abhi/oetker-consumer .
docker run -it abhi/oetker-consumer
```
This builds an image with Spark, python, and submits the job,  namely `consumer.py` with necessary 
dependencies. 

### Producer

```buildoutcfg
cd <dockers/producer>
docker build --no-cache -t abhi/oetker-producer .
docker run -it abhi/oetker-producer
```
The run command runs the producer `producer.py`, 
which reads the file (URL). The data is a stringified JSON.
Each line is read and streamed into Kafka. 
 
Additionally, the image can be run as follows:

`docker run -it --env MODE=DELAY abhi/oetker-producer`

This injects a delay of 2s per 50 lines read.

### Monitor
```buildoutcfg
docker run -it abhi/oetker-producer monitor
```


## How to run - local mode

This solution works better running Kafka using the docker

To run locally - if there is a running PG, please update `dockers/producer/config.ini`
and `dockers/consumer/config.ini` with the details of the Postgres server. Also, run the script `dockers/postgres/init.sql` to create the necessary schema and tables.

### Consumer

Change the host in `consumer/config.in` to localhost (assuming running locally). Also, update the postgres details.
Also, update the postgres details. If postgres is running locally using Docker, please
change the host to `localhost`.

Run

[Download Spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz). Install py dependencies.
`
pip3 install -t dependencies -r <path_to>/dockers/consumer/requirements.txt --no-cache-dir
cd dependencies && zip -r ../dependencies.zip .
`
This will essentially create a folder with required packages downloaded. Navigate to the folder and zip into one.

`export PYSPARK_PYTHON=python3` to use Python 3

```buildoutcfg
./bin/spark-submit \
      --jars <path_to>/dockers/consumer/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar \
      --py-files <path_to>/dependencies.zip \
      <path_to>/dockers/consumer/consumer.py
```

### Producer

Change the host in `producer/config.in` to localhost (assuming running locally). 
Also, update the postgres details. If postgres is running locally using Docker, please
change the host to `localhost`. 

Python (3.7) packages `kafka-python` and `psycopg2-binary` need to be installed.

Run
```buildoutcfg
chmod +x start.sh
./start.sh 
./start.sh monitor 
```

