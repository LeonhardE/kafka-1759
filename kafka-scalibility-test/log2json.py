import json
import os

DEBUG = True

logs_folder_prefix = '/../../homes/rodri228/logs/'
logs_folders = [
    "hadoop-24hr-logs-only",
    "openstack-24hrs-logs-only",
    "Loghub-datasets",
    "var-logs"
]
json_root_folder = os.path.join(os.getcwd(), 'json-files')
logs_folders = [os.path.join(logs_folder_prefix, p) for p in logs_folders]

check_one = False


def log2json(fp):
    global check_one
    check_one = True
    with open(fp, 'r', encoding='utf-8') as log_file:
        for line in log_file:
            lis = line.split(sep=' ', maxsplit=3)
            print(lis)
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
            json_object = json.dumps(dic, indent=4)
            print(json_object)


def recur(path):
    if check_one:
        return
    if os.path.isfile(path):
        log2json(path)
    else:
        for child in os.listdir(path):
            recur(os.path.join(path, child))


for folder in logs_folders:
    recur(folder)
