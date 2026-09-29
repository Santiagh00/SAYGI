import json
import time
from config import get_rabbitmq_connection, EXCHANGE_ORDERS
from discovery_client import DiscoveryClient

class UserServiceSubscriber:
    def __init__(self):
        self.service_name = "users-service"
        self.queue_name = "users_service_queue"
        
        # Conexión y auto-registro en Service Discovery
        self.discovery = DiscoveryClient(
            service_name=self.service_name,
            port=8001,
            description="Microservicio de Gestión de Clientes, Perfiles y Puntos"
        )
        self.discovery.start()

        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()


        # Asegurar que el exchange existe
        self.channel.exchange_declare(
            exchange=EXCHANGE_ORDERS,
            exchange_type="fanout",
            durable=True
        )

        # Declarar la cola propia de este microservicio
        self.channel.queue_declare(queue=self.queue_name, durable=True)

        # Enlazar (Bind) la cola al exchange
        self.channel.queue_bind(exchange=EXCHANGE_ORDERS, queue=self.queue_name)

        # Prefetch: procesar 1 mensaje a la vez
        self.channel.basic_qos(prefetch_count=1)

    def handle_message(self, ch, method, properties, body):
        try:
            event = json.loads(body.decode("utf-8"))
            data = event.get("data", {})
            order_id = data.get("order_id")
            user_id = data.get("user_id")
            amount = data.get("amount")

            print(f"\n[{self.service_name}] Evento recibido: '{event.get('event_type')}'")
            print(f"   -> Actualizando historial del cliente {user_id}...")

            # Simulación de cálculo de puntos de lealtad y actualización de base de datos
            time.sleep(0.5)
            points_earned = int(amount // 10)
            print(f"   -> [HISTORIAL ACTUALIZADO] Orden {order_id} agregada al perfil. Puntos sumados: +{points_earned} pts.")

            # Confirmación de recibido a RabbitMQ
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f"[{self.service_name}] Error procesando mensaje: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_listening(self):
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.handle_message,
            auto_ack=False
        )
        print("=" * 60)
        print(f" [{self.service_name}] Listo y escuchando cola: '{self.queue_name}'")
        print(" Presiona CTRL+C para detener el servicio.")
        print("=" * 60)
        self.channel.start_consuming()

if __name__ == "__main__":
    subscriber = UserServiceSubscriber()
    try:
        subscriber.start_listening()
    except KeyboardInterrupt:
        print(f"\n[{subscriber.service_name}] Deteniendo servicio de forma segura...")
        subscriber.channel.stop_consuming()
        subscriber.connection.close()
        subscriber.discovery.stop()
        print(f"[{subscriber.service_name}] Servicio detenido.")