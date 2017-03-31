#!/bin/bash
docker rm -fv kafka 2>/dev/null || echo "No kafka instance to kill"
docker rm -fv zookeeper  2>/dev/null || echo "No zookeeper instance to kill"
docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
docker run -d --name kafka -p 9092:9092 --link zookeeper:zookeeper --env KAFKA_ADVERTISED_HOST_NAME=localhost confluent/kafka

