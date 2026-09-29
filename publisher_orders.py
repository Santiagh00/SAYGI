import json
import time
import uuid
import pika
from config import get_rabbitmq_connection, EXCHANGE_ORDERS

class OrderServicePublisher:
    def __init__(self):
        self.service_name = "ORDERS_SERVICE"
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()

        # Declaramos un Exchange de tipo 'fanout' (Pub/Sub puro)
        # durable=True asegura que el exchange sobreviva si el broker se reinicia
        self.channel.exchange_declare(
            exchange=EXCHANGE_ORDERS,
            exchange_type="fanout",
            durable=True
        )
        print(f"[{self.service_name}] Conectado a RabbitMQ. Exchange '{EXCHANGE_ORDERS}' listo.")

    def create_order(self, order_id: str, user_id: str, amount: float):
        # Reconectar si la conexión o el canal están cerrados por inactividad
        if self.connection is None or self.connection.is_closed or self.channel.is_closed:
            self.connection = get_rabbitmq_connection()
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=EXCHANGE_ORDERS,
                exchange_type="fanout",
                durable=True
            )

        event_payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order.created",
            "timestamp": time.time(),
            "data": {
                "order_id": order_id,
                "user_id": user_id,
                "amount": amount,
                "status": "PENDING"
            }
        }

        message_body = json.dumps(event_payload)

        # Publicamos en el exchange fanout (el routing_key se ignora en fanout)
        self.channel.basic_publish(
            exchange=EXCHANGE_ORDERS,
            routing_key="",
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent, # Mensaje persistente en disco
                content_type="application/json"
            )
        )

        print(f"\n[{self.service_name}] Evento 'order.created' publicado:")
        print(f"   -> Orden: {order_id} | Usuario: {user_id} | Total: ${amount:.2f}")

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

if __name__ == "__main__":
    print("=" * 60)
    print(" INICIANDO MICROSERVICIO: ORDERS (PUBLISHER)")
    print("=" * 60)

    publisher = OrderServicePublisher()
    try:
        # Generar un par de órdenes de prueba
        test_orders = [
            ("ORD-101", "USER-99", 150.50),
            ("ORD-102", "USER-42", 89.99),
            ("ORD-103", "USER-99", 320.00),
        ]

        for order_id, user_id, amount in test_orders:
            publisher.create_order(order_id, user_id, amount)
            time.sleep(1)

        print("\n[ORDERS_SERVICE] Todas las órdenes fueron publicadas con éxito.")
    finally:
        publisher.close()