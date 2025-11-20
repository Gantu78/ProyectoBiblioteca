from locust import HttpUser, task, between
import random

class BibliotecaUser(HttpUser):
    # Cada usuario esperará entre 0.1 y 1 seg entre solicitudes (configurable)
    wait_time = between(0.1, 1.0)

    # Las solicitudes equivalentes al archivo de texto:
    solicitudes = [
        "DEVOLUCION;prestamoId=101",
        "RENOVACION;prestamoId=102;nuevaFecha=2025-10-22",
        "PRESTAMO;usuarioId=U1;libroCodigo=L1;inicio=2025-11-18;fin=2025-11-25",
        "PRESTAMO;usuarioId=U1;libroCodigo=L2;inicio=2025-11-18;fin=2025-11-25",
        "PRESTAMO;usuarioId=U2;libroCodigo=L1;inicio=2025-11-18;fin=2025-11-25",
    ]

    @task
    def enviar_linea(self):
        linea = random.choice(self.solicitudes)

        # Locust usa HTTP, así que asumimos que tu clase PS expone /send
        self.client.post(
            "/send",
            data=linea,
            name="OperacionBiblioteca"
        )
