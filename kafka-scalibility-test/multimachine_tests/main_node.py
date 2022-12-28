from fabric import Connection
import config

hostnames = [f'10.1.0.{i}' for i in range(2, 5)] + [f'10.1.0.{i}' for i in range(7, 23)]
password = 'ece1759'

def connect_host(seq):
    return Connection(f'luozhonghao@{hostnames[seq]}', connect_kwargs={'password': password})

def init_machine(machine_number):
    client = connect_host(machine_number)
    return client.run(f'git clone https://github.com/LeonhardE/kafka-1759.git;\
            cd kafka-1759/;\
            pip install -r requirements.txt;\
        ')
    
def run_script(machine_number, script_name):
    client = connect_host(machine_number)
    return client.run(f'cd kafka-1759/;\
            git pull;\
            pip install -r requirements.txt;\
            python3 kafka-scalibility-test/multimachine_tests/{script_name}.py\
        ')


def test6():
    test_name = 'test6'
    config.topic = test_name
    config.total_producers = 1
    config.total_records = 1000
    results = run_script(2, 'producer_node')
    print(results.stdout)
    
    
if __name__ == "__main__":
    test6()

