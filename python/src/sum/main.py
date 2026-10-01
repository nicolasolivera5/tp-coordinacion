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
        self.msg_count_by_client = {}
        self.coordinators = {}

        # estado del nodo cuando actúa como coordinador de un client_id:
        # {client_id: {"total_expected": N, "counts": {sum_id: count}}}
        self.active_coordinations = {}

        self.lock = threading.RLock()

        # canales de envio de control
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )

        # consumidor de control (hilo secundario)
        # escucha el exchange de control con dos routing keys:
        # SUM_CONTROL_EXCHANGE: mensajes broadcast (PREPARE, COMMIT) para todos los Sum.
        # READY_{ID}: mensajes dirigidos exclusivamente a este nodo cuando es coordinador.
        self.control_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE, f"READY_{ID}"]
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
        logging.info(f"Process data for client {client_id}")
        if client_id in self.processed_eof_clients:
            return
        self.amount_by_fruit_and_client.setdefault(client_id, {})
        self.amount_by_fruit_and_client[client_id][fruit] = self.amount_by_fruit_and_client[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
        self.msg_count_by_client[client_id] = self.msg_count_by_client.get(client_id, 0) + 1

        # Si ya habíamos recibido un PREPARE para este cliente, este dato llegó rezagado.
        # Reenviamos el nuevo conteo directamente al coordinador para actualizar la suma.
        if client_id in self.coordinators:
            coordinator_id = self.coordinators[client_id]
            count = self.msg_count_by_client[client_id]
            self.control_exchange.send_to(
                message_protocol.internal.serialize(["READY", client_id, ID, count]),
                f"READY_{coordinator_id}"
            )

    def _process_eof(self, client_id):
        with self.lock:
            if client_id in self.processed_eof_clients:
                return
            self.processed_eof_clients.add(client_id)
            client_data = self.amount_by_fruit_and_client.pop(client_id, {})
            self.msg_count_by_client.pop(client_id, None)
            self.coordinators.pop(client_id, None)
            self.active_coordinations.pop(client_id, None)

            logging.info(f"Broadcasting data messages for client {client_id}")
            for final_fruit_item in client_data.values():
                self.data_output_exchanges[self._get_agregation_key(final_fruit_item.fruit)].send(message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                ))

            logging.info(f"Broadcasting EOF message for client {client_id}")
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    # invocado por el nodo que extrae el EOF original de input_queue (asume rol coordinador)
    def _start_coordination(self, client_id, total_expected):
        if client_id in self.processed_eof_clients:
            return
        my_count = self.msg_count_by_client.get(client_id, 0)
        self.active_coordinations[client_id] = {
            "total_expected": total_expected,
            "counts": {ID: my_count}
        }
        # si justo recibio todos los mensajes el envia el commit
        if my_count >= total_expected:
            self._commit_coordination(client_id)
        else:
            # envia un mensaje a todas las replicas de sum para pedirles su conteo
            self.control_exchange.send(
                message_protocol.internal.serialize(["PREPARE", client_id, ID])
            )

    # envia el mensaje commit a todas las replicas de sum y procesa el eof local
    def _commit_coordination(self, client_id):
        self.active_coordinations.pop(client_id, None)
        commit_msg = message_protocol.internal.serialize(["COMMIT", client_id])
        self.control_exchange.send(commit_msg)
        self._process_eof(client_id)

    # procesa los mensajes de control
    def _process_control_message(self, message, ack, nack):
        with self.lock:
            fields = message_protocol.internal.deserialize(message)
            msg_type = fields[0]

            if msg_type == "PREPARE":
                client_id = fields[1]
                coordinator_id = fields[2]

                # si ya procese el eof o soy el coordinador ignoro el mensaje
                if client_id in self.processed_eof_clients or coordinator_id == ID:
                    ack()
                    return

                # guardo al coordinador para reenviar updates si llegan mensajes rezagados
                self.coordinators[client_id] = coordinator_id
                count = self.msg_count_by_client.get(client_id, 0)

                # respondo READY con el conteo unicamente al coordinador
                self.control_exchange.send_to(
                    message_protocol.internal.serialize(["READY", client_id, ID, count]),
                    f"READY_{coordinator_id}"
                )

            elif msg_type == "READY":
                client_id = fields[1]
                sender_id = fields[2]
                count = fields[3]

                if client_id in self.processed_eof_clients:
                    ack()
                    return

                if client_id in self.active_coordinations:
                    coordination = self.active_coordinations[client_id]
                    coordination["counts"][sender_id] = count
                    total_received = sum(coordination["counts"].values())

                    # si la suma de todos los conteos alcanza el total esperado
                    if total_received >= coordination["total_expected"]:
                        self._commit_coordination(client_id)

            elif msg_type == "COMMIT":
                client_id = fields[1]
                self.coordinators.pop(client_id, None)
                self._process_eof(client_id)

        ack()

    def _get_agregation_key(self, fruit):
        hex_to_int = int(hashlib.sha256(fruit.encode()).hexdigest(), 16)
        return hex_to_int % AGGREGATION_AMOUNT

    # procesa los mensajes de datos y el eof de input_queue
    def _process_data_messsage(self, message, ack, nack):
        with self.lock:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                self._start_coordination(fields[0], fields[1])
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
