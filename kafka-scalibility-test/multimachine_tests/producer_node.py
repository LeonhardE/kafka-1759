from data_loader import data_loader
from config import total_records, total_producers, total_consumers, topic
import time
from kafka import KafkaProducer
import pickle

def produce():
    producer = KafkaProducer(bootstrap_servers='10.1.0.1:9092')
    records = data_loader(total_records // total_producers).get_data()
    time_costs = 0
    total_bytes = 0
    for record in records:
        data = pickle.dumps(record)
        total_bytes += len(data)
        start = time.time()
        producer.send(topic, data)
        time_costs += time.time() - start
    print(time_costs, total_bytes)
    
produce()