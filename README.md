# Kafka Beginners Course

Code and resources from the [Learn Apache Kafka for Beginners v2](https://www.udemy.com/course/apache-kafka/)
Udemy course.

## Setup

Create a `.env` file with your [Twitter API](https://developer.twitter.com/en/apply-for-access) secrets:
```bash
cp example.env .env
````
Replace `API_KEY`, `API_SECRET`, `ACCESS_TOKEN` and `ACCESS_TOKEN_SECRET` with the corresponding values for your account.

Create a symlink to it into the test resources to be able to run
integration tests:
```bash
ln -s -f ../../../../.env twitter2elastic/src/test/resources/.env
```

## Launch Kafka and ElasticSearch clusters

[Kafka](https://kafka.apache.org/quickstart) and [ElasticSearch](https://www.elastic.co/) can be launched using
`docker-compose`, based on the [Confluent quickstart instructions](https://developer.confluent.io/quickstart/kafka-docker/):

```bash
docker-compose up -d
```

## Cleanup
```bash
docker-compose down --volumes
```
