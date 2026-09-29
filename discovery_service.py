import os
import time
from datetime import datetime
from typing import Dict, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import uvicorn

# Inicialización del Servicio de Discovery
app = FastAPI(
    title="Service Discovery (Registro de Servicios)",
    description="Servidor central donde los microservicios se registran y anuncian su estado de salud.",
    version="1.0.0"
)

# Estructura de datos para registrar un microservicio
class ServiceRegistration(BaseModel):
    service_name: str
    host: str
    port: int
    description: Optional[str] = "Microservicio del sistema"
    protocol: Optional[str] = "http"

class HeartbeatRequest(BaseModel):
    service_name: str

# Directorio en memoria de microservicios registrados
# Formato: { "service_name": { ...datos..., "last_heartbeat": timestamp, "status": "UP" } }
registry: Dict[str, dict] = {}

# Umbrales de tiempo (segundos)
HEARTBEAT_TIMEOUT = 12  # Si no envía heartbeat en 12s, pasa a DOWN

def get_service_status(data: dict) -> str:
    """Calcula si un microservicio sigue vivo según su último heartbeat."""
    elapsed = time.time() - data.get("last_heartbeat", 0)
    return "UP" if elapsed <= HEARTBEAT_TIMEOUT else "DOWN"

@app.post("/register", status_code=200)
def register_service(service: ServiceRegistration):
    """
    1. REGISTRO: Cada microservicio llama a este endpoint al iniciar
    para indicar su nombre, dirección IP/host y puerto.
    """
    now = time.time()
    url = f"{service.protocol}://{service.host}:{service.port}"
    registry[service.service_name] = {
        "service_name": service.service_name,
        "host": service.host,
        "port": service.port,
        "url": url,
        "description": service.description,
        "registered_at": datetime.now().strftime("%H:%M:%S"),
        "last_heartbeat": now,
        "last_heartbeat_human": "recién conectado",
        "status": "UP"
    }
    print(f"[DISCOVERY] Microservicio registrado: '{service.service_name}' en {url}")
    return {
        "status": "success",
        "message": f"Microservicio '{service.service_name}' registrado exitosamente.",
        "service": registry[service.service_name]
    }

@app.post("/heartbeat")
def heartbeat(req: HeartbeatRequest):
    """
    2. LATIDO (HEARTBEAT): El microservicio avisa periódicamente que sigue vivo.
    """
    if req.service_name in registry:
        registry[req.service_name]["last_heartbeat"] = time.time()
        registry[req.service_name]["status"] = "UP"
        return {"status": "success", "message": f"Heartbeat recibido de {req.service_name}"}
    raise HTTPException(status_code=404, detail="Microservicio no encontrado en el registro")

@app.post("/deregister")
def deregister_service(req: HeartbeatRequest):
    """
    3. DES-REGISTRO: Cuando un microservicio se apaga de forma ordenada,
    se elimina o marca como apagado.
    """
    if req.service_name in registry:
        removed = registry.pop(req.service_name)
        print(f"[DISCOVERY] Microservicio des-registrado: '{req.service_name}'")
        return {"status": "success", "message": f"Microservicio '{req.service_name}' des-registrado."}
    return {"status": "warning", "message": "El microservicio no estaba registrado."}

@app.get("/services")
def list_services():
    """
    4. CONSULTA / DESCUBRIMIENTO: Devuelve la lista de servicios activos y su estado.
    """
    current_time = time.time()
    result = {}
    for name, data in registry.items():
        elapsed = int(current_time - data["last_heartbeat"])
        status = "UP" if elapsed <= HEARTBEAT_TIMEOUT else "DOWN"
        item = dict(data)
        item["status"] = status
        item["seconds_since_heartbeat"] = elapsed
        result[name] = item
    return result

@app.get("/services/{service_name}")
def get_service(service_name: str):
    """Permite al API Gateway consultar dónde está un microservicio en específico."""
    services = list_services()
    if service_name in services:
        return services[service_name]
    raise HTTPException(status_code=404, detail=f"Microservicio '{service_name}' no encontrado en el registro.")

@app.get("/")
def root():
    """Ruta para verificar que el Service Discovery está activo."""
    return {
        "status": "online",
        "service": "Service Discovery",
        "total_registered": len(registry),
        "endpoints": {
            "list_services": "/services",
            "register": "POST /register",
            "heartbeat": "POST /heartbeat",
            "deregister": "POST /deregister"
        }
    }

if __name__ == "__main__":
    port = int(os.getenv("DISCOVERY_PORT", 8500))
    print("=" * 60)
    print(" SERVICIO DE REGISTRO Y DESCUBRIMIENTO (SERVICE DISCOVERY)")
    print(f" Servidor iniciado en: http://localhost:{port}")
    print(f" Consultar servicios registrados: http://localhost:{port}/services")
    print("=" * 60)
    uvicorn.run("discovery_service:app", host="0.0.0.0", port=port, reload=False)

