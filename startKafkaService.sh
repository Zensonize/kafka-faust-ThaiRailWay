#!/bin/bash

echo "starting zookeeper service"
kafka_2.13-2.6.0/bin/zookeeper-server-start.sh -daemon kafka_2.13-2.6.0/config/zookeeper.properties
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start zookeeper service: $status"
  exit $status
fi

echo "starting kafka broker service"
kafka_2.13-2.6.0/bin/kafka-server-start.sh -daemon kafka_2.13-2.6.0/config/server.properties
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start zookeeper service: $status"
  exit $status
fi

echo "create topic"
kafka_2.13-2.6.0/bin/kafka-topics.sh --create --topic new-schedule --bootstrap-server localhost:9092
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to create new new-schedule topic: $status"
fi

kafka_2.13-2.6.0/bin/kafka-topics.sh --create --topic train-status --bootstrap-server localhost:9092
if [ $status -ne 0 ]; then
  echo "Failed to create new train-status topic: $status"
fi