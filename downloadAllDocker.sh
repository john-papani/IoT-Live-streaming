#!/bin/bash
docker create -it  --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.10-management
docker create --name opentsdb-latest -p 4242:4242 petergrace/opentsdb-docker
docker create --name grafana  -p 3000:3000 grafana/grafana

FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"
docker network create flink-network

docker create \
   --name=jobmanager \
   --network flink-network\
   --publish 8081:8081 \
   --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
   flink:1.16-scala_2.12-java11 jobmanager


docker create \
   --name=taskmanager \
   --network flink-network \
   --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
   flink:1.16-scala_2.12-java11 taskmanager
    
docker create \
   --name=taskmanager2 \
   --network flink-network \
   --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
   flink:1.16-scala_2.12-java11 taskmanager

