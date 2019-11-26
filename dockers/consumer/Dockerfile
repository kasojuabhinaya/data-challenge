FROM openjdk:8-jre-alpine

RUN apk add --update \
    curl \
    build-base \
    && rm -rf /var/cache/apk/*

RUN apk add bash zip

ENV JAVA_HOME="/usr/lib/jvm/default-jvm"
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN apk add --no-cache openjdk8

RUN apk add --no-cache python3 \
	&& python3 -m ensurepip \
	&& pip3 install --upgrade pip setuptools \
	&& rm -r /usr/lib/python*/ensurepip \
	&& if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi \
	&& if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi \
	&& rm -r /root/.cache

COPY . /app

RUN pip3 install -t dependencies -r app/requirements.txt --no-cache-dir

RUN cd dependencies \
	&& zip -r ../dependencies.zip .

RUN mv dependencies.zip /app

RUN mkdir -p spark-2.4.4
RUN curl -L -O https://www-eu.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz && \
	tar -zxf spark-2.4.4-bin-hadoop2.7.tgz -C / && \
	rm spark-2.4.4-bin-hadoop2.7.tgz

ENV PYSPARK_PYTHON=python3
ENV SPARK_HOME="/spark-2.4.4-bin-hadoop2.7"

WORKDIR /spark-2.4.4-bin-hadoop2.7/

RUN rm -R /tmp/checkpoint_v2* || true

# Run this when the container starts
ENTRYPOINT ["./bin/spark-submit", "--jars", "/app/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar", "--py-files",  "/app/dependencies.zip", "/app/consumer.py"]