import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--topic', type=str, required=True, help='topic for test')
parser.add_argument('--total_records', type=str, required=True, help='total number of records')
parser.add_argument('--total_producers', type=str, required=True, help='total number of producers')
parser.add_argument('--total_consumers', type=str, required=True, help='total number of consumers')
parser.add_argument('--consumer_timeout_ms', type=str, required=True, help='consumer timeout in ms')