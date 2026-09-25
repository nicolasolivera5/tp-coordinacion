import os
import logging
import bisect
import signal
import sys

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_for_client = {}
        self.eof_client_count = {}
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.stop()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)
    
    def stop(self):
        try:
            self.input_exchange.stop_consuming()
        except Exception:
            pass

    def close(self):
        try:
            self.input_exchange.close()
        except Exception:
            pass
        try:
            self.output_queue.close()
        except Exception:
            pass

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        self.fruit_top_for_client.setdefault(client_id, [])
        fruit_top = self.fruit_top_for_client[client_id]
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_item = fruit_top.pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(self.fruit_top_for_client[client_id], updated_item)
                return
        bisect.insort(self.fruit_top_for_client[client_id], fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        self.eof_client_count[client_id] = self.eof_client_count.get(client_id, 0) + 1
        if self.eof_client_count[client_id] < SUM_AMOUNT:
            return
        
        fruit_top_list = sorted(self.fruit_top_for_client.pop(client_id, []))
        fruit_chunk = list(fruit_top_list[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))


    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(fields[0])
        ack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            self.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    try:
        aggregation_filter.start()
    except SystemExit:
        pass
    return 0


if __name__ == "__main__":
    main()
