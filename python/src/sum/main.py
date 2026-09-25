import os
import logging
import threading
import hashlib
import signal
import sys

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit_and_client = {}
        self.processed_eof_clients = set()
        self.lock = threading.RLock()

        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        self.control_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )

        threading.Thread(
            target=self._start_control_consumer,
            daemon=True,
        ).start()

        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def _start_control_consumer(self):
        try:
            self.control_consumer.start_consuming(self._process_control_message)
        finally:
            try:
                self.control_consumer.close()
            except Exception:
                pass

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
        try:
            self.control_consumer.connection.add_callback_threadsafe(
                self.control_consumer.stop_consuming
            )
        except Exception:
            pass

    def close(self):
        try:
            self.input_queue.close()
        except Exception:
            pass
        try:
            self.control_exchange.close()
        except Exception:
            pass
        for exchange in self.data_output_exchanges:
            try:
                exchange.close()
            except Exception:
                pass

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        with self.lock:
            self.amount_by_fruit_and_client.setdefault(client_id, {})
            self.amount_by_fruit_and_client[client_id][fruit] = self.amount_by_fruit_and_client[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        with self.lock:
            if client_id in self.processed_eof_clients:
                return
            self.processed_eof_clients.add(client_id)
            client_data = self.amount_by_fruit_and_client.pop(client_id, {})

        logging.info(f"Broadcasting data messages for client {client_id}")
        for final_fruit_item in client_data.values():
            self.data_output_exchanges[self._get_agregation_key(final_fruit_item.fruit)].send(message_protocol.internal.serialize(
                [client_id, final_fruit_item.fruit, final_fruit_item.amount]
            ))

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def _process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        with self.lock:
            self._process_eof(client_id)
        ack()

    def _get_agregation_key(self, fruit):
        hex_to_int = int(hashlib.sha256(fruit.encode()).hexdigest(), 16)
        return hex_to_int % AGGREGATION_AMOUNT

    def _process_data_messsage(self, message, ack, nack):
        with self.lock:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                self._process_eof(fields[0])
                self.control_exchange.send(message_protocol.internal.serialize([fields[0]])) ## le paso a los otros sum que el cliente termino
        ack()

    def start(self):
        try:
            self.input_queue.start_consuming(self._process_data_messsage)
        finally:
            self.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    try:
        sum_filter.start()
    except SystemExit:
        pass
    return 0


if __name__ == "__main__":
    main()
