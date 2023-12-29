import os, sys
from dotenv import load_dotenv
import pika
from openai import OpenAI

QUEUE_NAME = "writer_jobs"

def main():
    load_dotenv()

    openai_client = OpenAI()
    mq_connection = pika.BlockingConnection(pika.ConnectionParameters('desante.dev'))

    mq_channel = mq_connection.channel()
    mq_channel.queue_declare(queue=QUEUE_NAME)

    def on_job():
        completion = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a wedding thank you note writer. You write elegant and thoughtful notes to the guests of a wedding."},
                {"role": "user", "content": "Compose a thank you letter which is about 300 words. The recipient is the DeSante Family."}
            ]
        )

        print(completion.choices[0].message)
    
    mq_channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_job, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    
    mq_channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
