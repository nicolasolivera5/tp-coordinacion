import os
import logging
import signal
import sys

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eof_client_count = {}
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.stop()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def stop(self):
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass

    def close(self):
        try:
            self.input_queue.close()
        except Exception:
            pass
        try:
            self.output_queue.close()
        except Exception:
            pass

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        result = message_protocol.internal.deserialize(message)
        client_id, fruit_top_parcial = result
        self.final_top(client_id, fruit_top_parcial)
        self.eof_client_count[client_id] = self.eof_client_count.get(client_id, 0) + 1
        if self.eof_client_count[client_id] == AGGREGATION_AMOUNT:
            self.output_queue.send(message_protocol.internal.serialize([client_id, self.fruit_top_by_client[client_id][:TOP_SIZE]]))
        ack()

    def final_top(self, client_id, fruit_top_parcial):
        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []
            
        self.fruit_top_by_client[client_id].extend(fruit_top_parcial)
        self.fruit_top_by_client[client_id].sort(key=lambda item: item[1], reverse=True)
        self.fruit_top_by_client[client_id] = self.fruit_top_by_client[client_id]

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    try:
        join_filter.start()
    except SystemExit:
        pass

    return 0


if __name__ == "__main__":
    main()
