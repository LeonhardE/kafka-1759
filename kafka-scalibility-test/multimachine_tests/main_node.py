from fabric import Connection
import pickle
import threading
import time
import matplotlib.pyplot as plt

local_mode = False
consumer_timeout_ms = 2000
# only these machines have pip and we don't have sudo apt-install permission
hostnames = [f'10.1.0.{i}' for i in range(2, 5)] + ['10.1.0.7', '10.1.0.9', '10.1.0.11', '10.1.0.13']
password = 'ece1759'

def connect_host(seq):
    if local_mode:
        print(f'Connecting to luozhonghao@10.1.0.1')
        return Connection(f'luozhonghao@10.1.0.1', connect_kwargs={'password': password})
    else:
        print(f'Connecting to luozhonghao@{hostnames[seq]}')
        return Connection(f'luozhonghao@{hostnames[seq]}', connect_kwargs={'password': password})

def init_machine(machine_number):
    client = connect_host(machine_number)
    client.run(f'cd kafka-1759/;\
                git pull;\
                pip install -r requirements.txt;\
            ')
    
def init_machines(seq):
    for i in range(seq):
        init_machine(i)  

class Tester:
    def __init__(self, topic, total_records, producers, consumers): 
        self.topic = topic
        self.total_records = total_records
        if producers + consumers > len(hostnames):
            raise Exception(f'Not enough machines to run {producers} producers and {consumers} consumers')
        self.total_producers = producers
        self.total_consumers = consumers
        self.producers_time = [0 for _ in range(self.total_producers)]
        self.consumers_time = [0 for _ in range(self.total_consumers)]
        self.total_bytes = None
        
    def run(self):
        producer_threads = list()
        consumer_threads = list()
        machine_number = 0
        while machine_number < self.total_producers:
            producer_threads.append(threading.Thread(target=self.produce, args=[machine_number]))
            machine_number += 1
            
        while machine_number < self.total_consumers + self.total_producers:
            consumer_threads.append(threading.Thread(target=self.consume, args=[machine_number]))
            machine_number += 1
        
        for t in consumer_threads:
            t.start()
        time.sleep(1)
        for t in producer_threads:
            t.start()
            
        for t in producer_threads:
            t.join()
        for t in consumer_threads:
            t.join()

        return self.producers_time, self.consumers_time, self.total_bytes
    
    def run_script(self, machine_number, script_name):
        client = connect_host(machine_number)
        return client.run(f'python3 kafka-1759/kafka-scalibility-test/multimachine_tests/{script_name}.py --topic {self.topic} --total_records {self.total_records} --total_producers {self.total_producers} --total_consumers {self.total_consumers} --consumer_timeout_ms {consumer_timeout_ms}')
        
    def produce(self, seq):
        res = self.run_script(seq, 'producer_node').stdout
        res = res.strip('\n').split(' ')
        print('this is producer results', res)
        self.producers_time[seq] = float(res[0])
        self.total_bytes = float(res[1])
        
    def consume(self, seq):
        res = self.run_script(seq, 'consumer_node').stdout
        print('this is consumer results', res)
        self.consumers_time[seq - self.total_producers] = float(res)

   
def test6():
    test_name = 'test6'
    total_records = 100
    x = []
    yProduce = []
    yConsume = []
    init_machines(2)
    while total_records < 10000:
        res = Tester(test_name, total_records, 1, 1).run()
        yProduce.append(sum(res[0]))
        yConsume.append(sum(res[1]))
        x.append(res[2])
        total_records = int(total_records * 1.5)
    import numpy as np
    print(x, yProduce, yConsume)
    a, b = np.polyfit(x, yProduce, 1)
    print(f'a = {a}, b = {b}, speed = {(1/a) / 1024 / 1024} MB/s')
    plt.clf()   
    plt.title('1 producer and 1 consumer')
    plt.xlabel('number of bytes')
    plt.ylabel('time')
    plt.plot(x, yProduce, label='time for producer')
    plt.plot(x, yConsume, label='time for consumer')
    plt.legend()
    plt.savefig(f'{test_name}.png')
    
if __name__ == "__main__":
    test6()

