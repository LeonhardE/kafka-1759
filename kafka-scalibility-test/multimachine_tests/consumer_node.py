import config
import time
from kafka import KafkaConsumer


def consume():
    consumer = KafkaConsumer(config.topic, 
                            auto_offset_reset="earliest",
                            consumer_timeout_ms=2000)
    total_time = 0
    consume_start = None
    for record in consumer:
        if consume_start != None:
            total_time += time.time() - consume_start
        consume_start = time.time()
    print(total_time)


consume()