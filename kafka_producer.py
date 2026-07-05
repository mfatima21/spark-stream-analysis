import os
import csv
import json
import time
from kafka import KafkaProducer
from dotenv import load_dotenv
load_dotenv()

kafka_servers = os.getenv("KAFKA_SERVERS", "localhost:9092")
review_topic = os.getenv("REVIEW_TOPIC", "reviews")
review_csv_path = os.getenv("REVIEW_CSV", "/home/ubuntu/data/mycsv/review.csv")

numeric_fields = ["stars", "useful", "funny", "cool"]


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer


def send_reviews():
    producer = create_producer()
    print("sending to", review_servers_msg())

    with open(review_csv_path, 'r') as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            for field in numeric_fields:
                if row.get(field):
                    try:
                        row[field] = float(row[field])
                    except ValueError:
                        row[field] = None
            producer.send(review_topic, row)
            count += 1
            if count % 1000 == 0:
                print("sent", count, "reviews")
                time.sleep(0.1)

    producer.flush()
    print("done, sent", count, "reviews")


def review_servers_msg():
    return kafka_servers + " topic " + review_topic


if __name__ == "__main__":
    send_reviews()
