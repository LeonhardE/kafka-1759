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
                    if self.target == 0:
                        return
                    recur(os.path.join(path, child))
                    
        recur(os.path.join(os.path.dirname(__file__), 'json-logs'))
        return self.res

class Tester:
    def __init__(self, topic, records, producers, consumers): 
        self.topic = topic
        self.r_num = records
        self.p_num = producers
        self.c_num = consumers
        self.producers_time = [0 for _ in range(self.p_num)]
        self.consumers_time = [0 for _ in range(self.c_num)]
    
    def run(self):
        producer_threads = list()
        consumer_threads = list()
        for i in range(self.p_num):
            producer_threads.append(threading.Thread(target=self.produce, args=[i]))
        for i in range(self.c_num):
            consumer_threads.append(threading.Thread(target=self.consume, args=[i]))
        
        for t in consumer_threads:
            t.start()
        time.sleep(1)
        for t in producer_threads:
            t.start()
            
        for t in producer_threads:
            t.join()
        for t in consumer_threads:
            t.join()

        return self.producers_time, self.consumers_time
            
    def produce(self, seq):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        records = data_loader(self.r_num // self.p_num).get_data()
        for record in records:
            start = time.time()
            producer.send(self.topic, pickle.dumps(record))
            self.producers_time[seq] += time.time() - start

    def consume(self, seq):
        consumer = KafkaConsumer(self.topic, 
                                auto_offset_reset="latest",
                                consumer_timeout_ms=2000,
                                )
        total_time = 0
        consume_start = None
        for record in consumer:
            if consume_start != None:
                total_time += time.time() - consume_start
            consume_start = time.time()
        self.consumers_time[seq] = total_time
        
def test1():
    test_name = 'test1'
    x = []
    yProduce = []
    yConsume = []
    num = 10
    while num < 10000:
        tester = Tester(test_name, num, 1, 1)
        x.append(num)
        res = tester.run()
        yProduce.append(sum(res[0]))
        yConsume.append(sum(res[1]))
        num = int(num * 1.5)
    
    plt.clf()   
    plt.title('1 producer and 1 consumer')
    plt.xlabel('number of records')
    plt.ylabel('time')
    plt.plot(x, yProduce, label='time for producer')
    plt.plot(x, yConsume, label='time for consumer')
    plt.legend()
    plt.savefig(f'{test_name}.png')

def test2():
    test_name = 'test2'
    x = []
    yProduce = []
    yConsume = []
    producers = 1
    records= 10000
    while producers < 9:
        tester = Tester(test_name, records, producers, 1)
        x.append(producers)
        res = tester.run()
        yProduce.append(sum(res[0]))
        yConsume.append(sum(res[1]))
        producers += 1
    
    plt.clf()
    plt.title('10000 records and 1 consumer')
    plt.xlabel('number of producers')
    plt.ylabel('time')
    plt.plot(x, yProduce, label='total time for producers')
    plt.plot(x, yConsume, label='time for consumer')
    plt.legend()
    plt.savefig(f'{test_name}.png')

def test3():
    test_name = 'test3'
    x = []
    yProduce = []
    yConsume = []
    consumers = 1
    records= 10000
    while consumers < 9:
        tester = Tester(test_name, records, 1, consumers)
        x.append(consumers)
        res = tester.run()
        yProduce.append(sum(res[0]))
        yConsume.append(sum(res[1]))
        consumers += 1
    
    plt.clf()
    plt.title('10000 records and 1 producer')
    plt.xlabel('number of consumers')
    plt.ylabel('time')
    plt.plot(x, yProduce, label='time for producer')
    plt.plot(x, yConsume, label='total time for consumers')
    plt.legend()
    plt.savefig(f'{test_name}.png')


if __name__ == "__main__":
    test1()
    test2()
    test3()

    
