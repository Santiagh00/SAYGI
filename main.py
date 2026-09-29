import sys
from config import get_rabbitmq_connection, RABBITMQ_URL, EXCHANGE_ORDERS
from publisher_orders import OrderServicePublisher

def main():
    print("=" * 60)
    print(" SISTEMA DE MICROSERVICIOS PUB/SUB (RABBITMQ / CLOUDAMQP)")
    print("=" * 60)

    if not RABBITMQ_URL or RABBITMQ_URL == "tu_url_aqui":
        print("\n[CONFIGURACIÓN PENDIENTE]")
        print("Antes de ejecutar, pega tu URL de CloudAMQP en el archivo .env:")
        print("  RABBITMQ_URL=amqps://tu_usuario:tu_password@tu_host/tu_vhost\n")
        print("Guía rápida para obtener tu URL gratis:")
        print("1. Entra en https://customer.cloudamqp.com/ y regístrate gratis (con Google o GitHub).")
        print("2. Haz clic en 'Create New Instance', elige el plan gratis 'Little Lemur'.")
        print("3. Abre la instancia creada y copia la 'AMQP URL'.")
        print("4. Pégala en el archivo .env y vuelve a ejecutar este comando.")
        sys.exit(1)

    print("\nProbando conexión con RabbitMQ...")
    conn = get_rabbitmq_connection()
    print(" ¡Conexión exitosa a RabbitMQ!")
    conn.close()

    print("\n¿Qué deseas hacer?")
    print("1. Enviar una orden de prueba (ORD-TEST)")
    print("2. Enviar múltiples órdenes simuladas")
    print("3. Salir")

    opcion = input("\nSelecciona una opción (1/2/3): ").strip()

    if opcion == "1":
        publisher = OrderServicePublisher()
        try:
            publisher.create_order("ORD-999", "USER-DEMO", 199.99)
            print("\nOrden enviada al Exchange de RabbitMQ.")
            print("Verifica las terminales de 'subscriber_payments.py' y 'subscriber_users.py' para ver cómo la procesaron.")
        finally:
            publisher.close()

    elif opcion == "2":
        publisher = OrderServicePublisher()
        try:
            publisher.create_order("ORD-201", "USER-ALICE", 45.00)
            publisher.create_order("ORD-202", "USER-BOB", 120.50)
            publisher.create_order("ORD-203", "USER-CARLOS", 310.20)
            print("\n3 órdenes enviadas al Exchange de RabbitMQ.")
        finally:
            publisher.close()
    else:
        print("Operación cancelada.")

if __name__ == "__main__":
    main()