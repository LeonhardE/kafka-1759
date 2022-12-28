from fabric import Connection

hostnames = [f'10.1.0.{i}' for i in range(2, 5)] + [f'10.1.0.{i}' for i in range(7, 23)]
password = 'ece1759'

def connect_host(seq):
    return Connection(f'luozhonghao@{hostnames[seq]}', connect_kwargs={'password': password})


def run_script(machine_number, script_name):
    client = connect_host(machine_number)
    client.run('git clone https://github.com/LeonhardE/kafka-1759.git')
    client.run('cd kafka-1759/kafka-scalibility-test')
    client.run('pip install -r requirement.txt')
    client.run(f'python3 multimachine_tests/{script_name}')



