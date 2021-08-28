# Kafka Beginners Course

Code and resources from the [Learn Apache Kafka for Beginners v2](https://www.udemy.com/course/apache-kafka/) Udemy course.

## Launch kafka

Kafka and Zookeper can be launched using `docker-compose`, based on the [Confluent quickstart instructions](https://developer.confluent.io/quickstart/kafka-docker/):

```
docker-compose up -d
```

## Cleanup
```
docker-compose down --volumes
```
