#!/bin/bash
kafka_2.13-2.6.0/bin/kafka-topics.sh --create --topic new-schedule --bootstrap-server localhost:9092
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to create new new-schedule topic: $status"
fi
kafka_2.13-2.6.0/bin/kafka-topics.sh --create --topic train-status --bootstrap-server localhost:9092
if [ $status -ne 0 ]; then
  echo "Failed to create new train-status topic: $status"
fi