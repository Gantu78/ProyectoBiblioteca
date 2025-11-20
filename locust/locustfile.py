from locust import HttpUser, task, between
import random

class BibliotecaUser(HttpUser):
    wait_time = between(0.1, 1.0)

    puertos = [8081, 8082, 8083]

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
        puerto = random.choice(self.puertos)

        # Sobrescribimos la URL completa
        self.client.post(
            f"http://localhost:{puerto}/send",
            data=linea,
            name=f"OperacionBiblioteca_P{puerto}"
        )
