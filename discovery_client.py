import os
import atexit
import threading
import time
import requests

DISCOVERY_URL = os.getenv("DISCOVERY_URL", "http://localhost:8500").rstrip("/")

class DiscoveryClient:
    """
    Cliente liviano que conecta cualquier microservicio con el Service Discovery:
    1. Se auto-registra al iniciar.
    2. Envía un latido (heartbeat) continuo en segundo plano cada 5 segundos.
    3. Se des-registra automáticamente al cerrar el programa.
    """
    def __init__(self, service_name: str, port: int, host: str = "localhost", description: str = ""):
        self.service_name = service_name
        self.port = port
        self.host = host
        self.description = description
        self.is_running = False
        self.heartbeat_thread = None

    def start(self):
        """Registra el microservicio e inicia los latidos (heartbeats)."""
        self._register()
        self.is_running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        atexit.register(self.stop)

    def _register(self):
        payload = {
            "service_name": self.service_name,
            "host": self.host,
            "port": self.port,
            "description": self.description,
            "protocol": "http"
        }
        try:
            res = requests.post(f"{DISCOVERY_URL}/register", json=payload, timeout=3)
            if res.status_code == 200:
                print(f"[DISCOVERY]  Registrado exitosamente en {DISCOVERY_URL} como '{self.service_name}'.")
        except requests.exceptions.RequestException:
            print(f"[DISCOVERY] ⚠️  No se pudo contactar con Service Discovery en {DISCOVERY_URL}. (¿Está encendido discovery_service.py?)")

    def _heartbeat_loop(self):
        """Hilo en segundo plano enviando señal de vida cada 5 segundos."""
        while self.is_running:
            time.sleep(5)
            if not self.is_running:
                break
            try:
                requests.post(
                    f"{DISCOVERY_URL}/heartbeat",
                    json={"service_name": self.service_name},
                    timeout=2
                )
            except requests.exceptions.RequestException:
                pass  # Fallo silencioso temporal si discovery se reinicia

    def stop(self):
        """Des-registra el microservicio del servidor Discovery al apagarse."""
        if not self.is_running:
            return
        self.is_running = False
        try:
            requests.post(
                f"{DISCOVERY_URL}/deregister",
                json={"service_name": self.service_name},
                timeout=2
            )
            print(f"[DISCOVERY]  Des-registrado de Service Discovery.")
        except requests.exceptions.RequestException:
            pass

def lookup_service(service_name: str) -> dict:
    """
    Función de DESCUBRIMIENTO: Permite a un microservicio o al API Gateway
    consultar dinámicamente la URL y estado de otro servicio.
    """
    try:
        res = requests.get(f"{DISCOVERY_URL}/services/{service_name}", timeout=3)
        if res.status_code == 200:
            return res.json()
    except requests.exceptions.RequestException:
        pass
    return None
