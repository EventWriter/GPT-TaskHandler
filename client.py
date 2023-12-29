#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='desante.dev'))
channel = connection.channel()

channel.queue_declare(queue='writer_jobs')

channel.basic_publish(exchange='', routing_key='writer_jobs', body='Hello World!')
print(" [x] Sent 'Hello World!'")
connection.close()