import json
import os

logs_folder_prefix = '/../../homes/rodri228/logs/'
logs_folders = [
    "hadoop-24hr-logs-only",
    "openstack-24hrs-logs-only",
    "Loghub-datasets",
    "var-logs"
]
json_root_folder = os.path.join(
    os.getcwd(), 'kafka-scalibility-test/json-logs')
logs_folders = [os.path.join(logs_folder_prefix, p) for p in logs_folders]

seq = 0
max_seq = None


def log2json(fp):
    global seq
    tf = os.path.join(json_root_folder, f'{seq}.json')
    seq += 1
    with open(tf, 'w', encoding='utf-8') as json_file:
        dic_lis = []
        with open(fp, 'r', encoding='utf-8') as log_file:
            for line in log_file:
                lis = line.split(sep=' ', maxsplit=3)
                if len(lis) < 4:
                    continue
                dic = dict()
                dic['timestamp'] = f'{lis[0]} {lis[1]}'
                dic['type'] = lis[2]
                lis = lis[3].split(sep=':', maxsplit=1)
                if len(lis) < 2:
                    continue
                dic['app'] = lis[0]
                dic['message'] = lis[1]
                dic_lis.append(dic)
        json.dump(obj=dic_lis,
                  fp=json_file,
                  indent=4,
                  separators=(',', ': '))


def recur(path):
    if seq > max_seq:
        return
    if os.path.isfile(path):
        log2json(path)
    else:
        for child in os.listdir(path):
            if seq > max_seq:
                return
            recur(os.path.join(path, child))


max_seq = int(input('how many json log files you need?\n')) - 1
for folder in logs_folders:
    recur(folder)
