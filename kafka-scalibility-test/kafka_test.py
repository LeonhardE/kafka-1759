import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import json
import os
import pickle
import matplotlib.pyplot as plt


class data_loader:
    def __init__(self, target):
        self.res = []
        self.target = target
            
    def get_data(self):
        def get_log(path):
            with open(path, "r") as file:
                contents = file.read()
                lis = json.loads(contents)
                load_len = min(self.target, len(lis))
                self.res += lis[:load_len]
                self.target -= load_len
            
        def recur(path):
            if self.target == 0:
                return
            if os.path.isfile(path):
                get_log(path)
            else:
                for child in os.listdir(path):
                    recur(os.path.join(path, child))
                    
        recur(os.path.join(os.path.dirname(__file__), 'json-logs'))
        return self.res

class Tester:
    def __init__(self, topic, num): 
        self.topic = topic
        self.num = num    
        self.consume_time = 0
        self.produce_time = 0
        self.consume_start = None
    
    def run(self):
        t1 = threading.Thread(target=self.produce)
        t2 = threading.Thread(target=self.consume)
        t2.start()
        time.sleep(1)
        t1.start()
        t1.join()
        t2.join()
        return f'Total records : {self.num}, time for producer {self.produce_time}, time for consumer {self.consume_time}.'
            
    def produce(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        records = data_loader(self.num).get_data()
        for record in records:
            start = time.time()
            producer.send(self.topic, pickle.dumps(record))
            self.produce_time += time.time() - start

    def consume(self):
        consumer = KafkaConsumer(self.topic, 
                                auto_offset_reset="latest",
                                consumer_timeout_ms=5000,
                                )
        
        for record in consumer:
            if self.consume_start != None:
                self.consume_time += time.time() - self.consume_start
            self.consume_start = time.time()
        
    
def test1():
    x = []
    y = []
    num = 10
    while num < 1000:
        tester = Tester('test1', num)
        x.append(num)
        y.append(tester.run())
        num = int(num * 1.5)
    plt.plot(x, y)
    plt.savefig('test1.png')



if __name__ == "__main__":
    test1()
    

    
