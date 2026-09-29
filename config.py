import os
import sys
import pika
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "").strip()

# Nombre del exchange donde se publicarán los eventos de órdenes
EXCHANGE_ORDERS = "orders_events_exchange"

def get_rabbitmq_connection():
    """
    Crea y retorna una conexión a RabbitMQ usando la URL configurada.
    """
    if not RABBITMQ_URL or RABBITMQ_URL == "tu_url_aqui":
        print("\n[ERROR DE CONFIGURACIÓN]")
        print("Debes configurar la variable RABBITMQ_URL en el archivo .env con tu URL de CloudAMQP.")
        print("Ejemplo: RABBITMQ_URL=amqps://usuario:pass@hostname/vhost\n")
        sys.exit(1)

    try:
        parameters = pika.URLParameters(RABBITMQ_URL)
        # Timeout de conexión razonable
        parameters.socket_timeout = 10
        connection = pika.BlockingConnection(parameters)
        return connection
    except Exception as e:
        print(f"\n[ERROR DE CONEXIÓN A RABBITMQ]: {e}")
        print("Verifica que la URL en .env sea correcta y que tengas conexión a internet.\n")
        sys.exit(1)
