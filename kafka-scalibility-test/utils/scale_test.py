import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import json
import os
import pickle
import sys
import matplotlib.pyplot as plt

class data_loader:
    def __init__(self, target):
        self.res = []
        self.target = target
            
    def get_data(self):
        # def get_log(path):
        #     with open(path, "r") as file:
        #         contents = file.read()
        #         lis = json.loads(contents)
        #         load_len = min(self.target, len(lis))
        #         self.res += lis[:load_len]
        #         self.target -= load_len
            
        # def recur(path):
        #     if self.target == 0:
        #         return
        #     if os.path.isfile(path):
        #         get_log(path)
        #     else:
        #         for child in os.listdir(path):
        #             if self.target == 0:
        #                 return
        #             recur(os.path.join(path, child))
                    
        # recur(os.path.join(os.path.dirname(__file__), 'json-logs'))
        index = 0
        prefix = "./json-logs/"
        while self.target:
            fname = prefix + str(index) + ".json"
            with open(fname, "r") as file:
                contents = file.read()
                list = json.loads(contents)
                load_len = min(self.target, len(list))
                self.res += list[:load_len]
                self.target -= load_len
            index += 1
        return self.res

class Tester:
    def __init__(self, topic, records, producers, consumers): 
        self.topic = topic
        self.r_num = records
        self.p_num = producers
        self.c_num = consumers
        self.data_size=0
        self.consume_start = None
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

        return self.data_size, self.producers_time, self.consumers_time
            
    def produce(self, seq):
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda m: json.dumps(m).encode('ascii'))
        records = data_loader(self.r_num // self.p_num).get_data()
        for record in records:
            start = time.time()
            producer.send(self.topic, record)
            self.data_size += sys.getsizeof(record["timestamp"]) + sys.getsizeof(record["type"])+ sys.getsizeof(record["app"]) + sys.getsizeof(record["message"])
            self.producers_time[seq] += time.time() - start

    def consume(self, seq):
        consumer = KafkaConsumer(self.topic, 
                                auto_offset_reset="latest",
                                value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                consumer_timeout_ms=2000)
        total_time = 0
        consume_start = None
        for record in consumer:
            if consume_start:
                total_time += time.time() - consume_start
            consume_start = time.time()
        print("consume", consume_start, total_time)
        self.consumers_time[seq] = total_time
        
def test1():
    test_name = 'test1'
    x = []
    yProduce = []
    yConsume = []
    num = 1000
    for i in range(20):
        print(num)
        tester = Tester(test_name, num, 1, 1)
        res = tester.run()
        print(res)
        x.append(res[0])
        yProduce.append(sum(res[1]))
        yConsume.append(sum(res[2]))
        num += 1000
    
    plt.clf()   
    plt.title('1 producer and 1 consumer')
    plt.xlabel('size of records(bytes)')
    plt.ylabel('time')
    plt.plot(x, yProduce, label='time for producer')
    plt.plot(x, yConsume, label='time for consumer')
    plt.legend()
    plt.savefig(f'{test_name}_byte.png')

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
    # test2()
    # test3()
    # result = data_loader(1000).get_data()
    # record = result[0]
    # print(result[1], len(record), record.__sizeof__(), sys.getsizeof("record"), sys.getsizeof(1))
    # for i in range(10):
    #     print(sys.getsizeof(record[i]["timestamp"]) + sys.getsizeof(record[i]["type"])+ sys.getsizeof(record[i]["app"]) + sys.getsizeof(record[i]["message"]))

    
