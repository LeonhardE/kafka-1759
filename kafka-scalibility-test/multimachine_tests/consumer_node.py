import time
from kafka import KafkaConsumer
from myparser import parser

def consume():
    args = parser.parse_args()
    print(args)
    consumer = KafkaConsumer(args.topic,
                            bootstrap_servers = '10.1.0.1:9092', 
                            auto_offset_reset="latest",
                            consumer_timeout_ms=2000)
    total_time = 0
    consume_start = None
    for record in consumer:
        if consume_start != None:
            total_time += time.time() - consume_start
        consume_start = time.time()
    print(total_time)


consume()