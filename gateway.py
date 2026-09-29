from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from publisher_orders import OrderServicePublisher
from discovery_client import DiscoveryClient, lookup_service

discovery = DiscoveryClient(
    service_name="api-gateway",
    port=8000,
    description="Punto único de entrada HTTP y enrutador de microservicios"
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Al iniciar el API Gateway: Auto-registro en Service Discovery
    discovery.start()
    yield
    # Al apagar: Des-registro
    discovery.stop()

# Inicialización de FastAPI (nuestro API Gateway)
app = FastAPI(
    title="API Gateway",
    description="Punto único de entrada que recibe peticiones HTTP, consulta Service Discovery y despacha a RabbitMQ",
    lifespan=lifespan
)


# Instancia global del publicador de órdenes
publisher = None

def get_publisher():
    global publisher
    if publisher is None:
        publisher = OrderServicePublisher()
    return publisher

# Estructura del cuerpo de la petición (JSON)
class OrderRequest(BaseModel):
    order_id: str
    user_id: str
    amount: float

@app.get("/")
def home():
    """Ruta para verificar que el API Gateway está funcionando."""
    return {
        "status": "online",
        "service": "API Gateway",
        "docs": "/docs",
        "discovery_url": "http://localhost:8500"
    }

@app.get("/services")
def get_discovered_services():
    """Demuestra cómo el Gateway descubre los microservicios consultando al Discovery."""
    import requests
    from discovery_client import DISCOVERY_URL
    try:
        res = requests.get(f"{DISCOVERY_URL}/services", timeout=2)
        return {
            "source": "Service Discovery",
            "active_services": res.json()
        }
    except Exception as e:
        return {
            "source": "Service Discovery",
            "error": f"No se pudo consultar Service Discovery: {str(e)}"
        }


@app.post("/orders", status_code=201)
def create_order(order: OrderRequest):
    """
    Recibe una orden vía HTTP POST y la publica como evento en RabbitMQ.
    Los microservicios (Users y Payments) reaccionarán automáticamente.
    """
    try:
        pub = get_publisher()
        pub.create_order(
            order_id=order.order_id,
            user_id=order.user_id,
            amount=order.amount
        )
        return {
            "status": "success",
            "message": f"Orden {order.order_id} recibida y despachada a RabbitMQ exitosamente.",
            "data": order
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al enviar la orden a RabbitMQ: {str(e)}"
        )
