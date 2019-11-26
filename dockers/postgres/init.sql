DROP SCHEMA IF EXISTS producer;

CREATE SCHEMA producer;

DROP TABLE IF EXISTS producer.stats;
CREATE TABLE producer.stats(
    topic text,
    ts timestamp DEFAULT now(),
    total integer
);

DROP SCHEMA IF EXISTS consumer;
CREATE SCHEMA consumer;

DROP TABLE IF EXISTS consumer.consumer;
CREATE TABLE consumer.stats(
    ts timestamp  DEFAULT now(),
    topic text,
    total integer
);

CREATE TABLE consumer.country_stats(
    country text,
    topic text,
    ts timestamp DEFAULT now(),
    total integer
);