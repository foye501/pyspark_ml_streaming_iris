#
# python 2.7 program to push random lines from the iris dataset into Kafka
# Written by Tal Franji
# Code is free for all usage - feel free to copy and use.
from kafka import KafkaConsumer
from kafka import KafkaProducer
import random
import sys
import time

# need to install:
#$ sudo pip install kafka-python

iris_csv = "./iris.csv"
kafka_topic = 'wwwlog'
kafka_server = '34.242.73.97:9092'

def LoadIris():
    with open(iris_csv) as f:
        lines = f.readlines()[1:]  # remove header
        return lines


def Consumer():
    # Consumer - used for testing the generator
    consumer = KafkaConsumer('wwwlog', bootstrap_servers=kafka_server)
    for msg in consumer:
        print (msg)


def RandomLineGenerator():
    lines = LoadIris()
    while True:
        random.shuffle(lines)
        for line in lines:
            yield line

def Producer():
    producer = KafkaProducer(bootstrap_servers=kafka_server)
    print "PRODUCE..."
    throttle = 0
    throttle_n = 10 # count of messages in 100 mesec
    line_iter = RandomLineGenerator()
    while True:
        throttle += 1
        line = line_iter.next()
        print line
        producer.send(kafka_topic, line)
        if throttle >= throttle_n:
            throttle = 0
            time.sleep(0.1)


if len(sys.argv) > 1 and sys.argv[1].lower() == "c":
    Consumer()
else:
    Producer()
