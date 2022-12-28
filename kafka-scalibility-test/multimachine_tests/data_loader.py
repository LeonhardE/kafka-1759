import json
import os

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
                    
        recur(os.path.join(os.path.dirname(__file__), '../json-logs'))
        return self.res