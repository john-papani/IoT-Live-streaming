#!/usr/bin/env python
import pika
import json
from time import sleep
import time
import random
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello1',durable=True)

for i in range(1,50):
    data = {
        "timeday": 1577836800+i*3600,
        "temperature": random.randint(10,35),
        "description":"{}_description_{}".format(i,i),
         "th1":random.randint(0,100),
        "th2":random.randint(0,100),
        "hvac1":random.randint(0,100),
        "hvac2":random.randint(0,100),
        "miac1":random.randint(0,100),
        "miac2":random.randint(0,100),
        "etot":random.randint(0,100),
        "mov1":random.randint(0,100),
        "w1":random.randint(0,100),
        "wtot":random.randint(0,100),
        }
    sleep(0.01)
    message = json.dumps(data)
# for x in range(1, 100000):
    channel.basic_publish(exchange='',
                            routing_key='hello1',
                            body=message)
    print(" [x] Sent ", message)
time.sleep(0.5)

connection.close()

# https://www.rabbitmq.com/tutorials/tutorial-one-python.html