import pika
import time
import json
from datetime import datetime

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

channel = connection.channel()
MINS_INTERVAL = 0.004

channel.queue_declare(queue='hello1', durable=True)

try:
    # while True :
            with open('./events_.json', 'r') as f:
                events = f.read().strip().split('\n')
            for event in events[1:-1]:
                if event[-1] == ",":
                    e = "{" + event[:-1] + "}"
                else:                    
                    e = "{" + event + "}"

                e2 = e
                date = e.strip('{"').split('":{"')[0]
                values = e2.split('":{"')[1]
                datetime_object = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                ux_date = int(time.mktime(datetime_object.timetuple()))
                ux_date += 60*60*2

                data = '{"timeday":"'+str(ux_date) + '", "' + values
                data = data.replace("}}","}")

                message = json.dumps(data)

                channel.basic_publish(exchange='',
                routing_key='hello1',
                body=data,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ))              
                print(" [x] Sent %r" % data)
                time.sleep(0.01)

finally:
    connection.close()

