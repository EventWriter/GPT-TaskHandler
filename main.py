import os, sys
import pika
import json

from dotenv import load_dotenv
from openai import OpenAI
from pymongo import MongoClient
from flask import Flask, jsonify
from threading import Thread
from bson import json_util
from bson.objectid import ObjectId

QUEUE_NAME = "writer_jobs"

def main():

    load_dotenv()

    def rabbitmq_consumer():
        mongo_client = MongoClient("mongodb+srv://joeldesante:%s@cluster0.fyke8yt.mongodb.net/?retryWrites=true&w=majority" % os.getenv("MONGO_PASSWORD"))
        openai_client = OpenAI()
        mq_connection = pika.BlockingConnection(pika.ConnectionParameters('desante.dev'))

        mq_channel = mq_connection.channel()
        mq_channel.queue_declare(queue=QUEUE_NAME)

        mongo_db = mongo_client.eventwriter
        mongo_collection = mongo_db.writingjobs

        def on_job(ch, method, properties, body):
            print("[*] Recieved Job")
            completion = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a wedding thank you note writer. You write elegant and thoughtful notes to the guests of a wedding."},
                    {"role": "user", "content": "Compose a short thank you letter. The recipient is the DeSante Family. They gave a baby stroller as a gift."}
                ]
            )

            job = {
                "status": "complete",
                "body": completion.choices[0].message.content
            }

            mongo_collection.insert_one(job)
            print("[*] Job Complete")
        
        mq_channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_job, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        
        mq_channel.start_consuming()

    rabbitmq_thread = Thread(target=rabbitmq_consumer)
    rabbitmq_thread.start()
    
    # Setup Flask Stuff
    mongo_client = MongoClient("mongodb+srv://joeldesante:%s@cluster0.fyke8yt.mongodb.net/?retryWrites=true&w=majority" % os.getenv("MONGO_PASSWORD"))
    mongo_db = mongo_client.eventwriter
    mongo_collection = mongo_db.writingjobs

    flask_app = Flask(__name__)

    # Version and Service Name
    @flask_app.route("/")
    def index():
        return {
            "service": "Writer Service",
            "version": "0.0.1"
        }

    # Returns a list of ALL jobs in the system. Paginated.
    # If provided with a user id param, then the jobs will be limited to that user ID
    # Will only show jobs your account has access too
    @flask_app.route("/jobs")
    def jobs():
        cursor = mongo_collection.find({})
        return json.loads(json_util.dumps(list(cursor)))

    # Returns a job given a job_id. Must have permission to view.
    @flask_app.route("/jobs/<job_id>")
    def job(job_id):
        job = mongo_collection.find_one({ "_id": ObjectId(job_id) })
        return json.loads(json_util.dumps(job)) or "Not Found"
    
    flask_app.run(debug=True)
    

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
