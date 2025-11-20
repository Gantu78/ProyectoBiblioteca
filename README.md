# ProyectoBiblioteca — Cómo ejecutar (IntelliJ) e info importante

Resumen rápido
- Proyecto Maven Java: ver [pom.xml](pom.xml).
- Código principal: actores y gestores en `src/main/java/co/javeriana/`.
  - [`co.javeriana.GestorCarga`](src/main/java/co/javeriana/GestorCarga.java)
  - [`co.javeriana.ActorPrestamo`](src/main/java/co/javeriana/ActorPrestamo.java)
  - [`co.javeriana.ActorDevolucion`](src/main/java/co/javeriana/ActorDevolucion.java)
  - [`co.javeriana.ActorRenovacion`](src/main/java/co/javeriana/ActorRenovacion.java)
  - [`co.javeriana.PS`](src/main/java/co/javeriana/PS.java)
  - Replicación y almacenamiento: [`co.javeriana.ReplicaManager`](src/main/java/co/javeriana/ReplicaManager.java), [`co.javeriana.GestorAlmacenamiento`](src/main/java/co/javeriana/GestorAlmacenamiento.java), [`co.javeriana.GestorAlmacenamientoConReplica`](src/main/java/co/javeriana/GestorAlmacenamientoConReplica.java)
  - Persistencia en fichero: [FileBasedLibroRepository.java](src/main/java/co/javeriana/FileBasedLibroRepository.java), [FileBasedPrestamoRepository.java](src/main/java/co/javeriana/FileBasedPrestamoRepository.java)
  - Cola durable: [DurableQueue.java](src/main/java/co/javeriana/DurableQueue.java)

Requisitos
- JDK compatible con la versión indicada en [pom.xml](pom.xml) (propiedad maven.compiler.*).
- Plugins Maven descargados por IntelliJ.

Ejecutar desde IntelliJ (pasos)
1. Importar el proyecto: File → New → Project from Existing Sources → seleccionar `pom.xml`.
2. Dejar que IntelliJ resuelva dependencias (jeromq, gson).
3. Crear una "Run/Debug Configuration" tipo Application para cada servicio:
   - GestorCarga
     - Main class: `co.javeriana.GestorCarga` ([GestorCarga.java](src/main/java/co/javeriana/GestorCarga.java))
     - Program arguments ejemplo: `tcp://0.0.0.0:5555 tcp://0.0.0.0:5560 tcp://localhost:5570 tcp://0.0.0.0:5556`
   - ActorPrestamo
     - Main class: `co.javeriana.ActorPrestamo` ([ActorPrestamo.java](src/main/java/co/javeriana/ActorPrestamo.java))
     - Program arguments ejemplo: `tcp://*:5570`
     - VM options útiles: `-DfailAfterN=10 -DremoteGcEndpoints=tcp://gc1:5555 -DnotifyGcEnqueue=tcp://gc1:5556 -DsiteId=sede1`
   - ActorDevolucion / ActorRenovacion
     - Main classes: `co.javeriana.ActorDevolucion` ([ActorDevolucion.java](src/main/java/co/javeriana/ActorDevolucion.java)) y `co.javeriana.ActorRenovacion` ([ActorRenovacion.java](src/main/java/co/javeriana/ActorRenovacion.java))
     - Program arguments ejemplo: `tcp://localhost:5560 tcp://localhost:5556`
   - PS (puede usarse para inyectar solicitudes vía HTTP)
     - Main class: `co.javeriana.PS` ([PS.java](src/main/java/co/javeriana/PS.java))
     - Program arguments ejemplo: `tcp://localhost:5555 8081`

Archivos de ejemplo y pruebas
- Solicitudes de ejemplo: [src/main/resources/solicitudes_sede1.txt](src/main/resources/solicitudes_sede1.txt) y la copia en target: [target/classes/solicitudes_sede1.txt](target/classes/solicitudes_sede1.txt).
- Script de carga (locust): [locust/locustfile.py](locust/locustfile.py) — envía POST a PS.

Puntos importantes / notas operativas
- Los datos persistentes se escriben en `data/primaria`, `data/replica`, y `data/gc` (colas). Ver implementaciones en [FileBasedLibroRepository.java](src/main/java/co/javeriana/FileBasedLibroRepository.java), [FileBasedPrestamoRepository.java](src/main/java/co/javeriana/FileBasedPrestamoRepository.java) y [DurableQueue.java](src/main/java/co/javeriana/DurableQueue.java).
- Simulación de fallo de primaria: pasar `-DfailAfterN=<n>` a `ActorPrestamo` para que `GestorAlmacenamiento` marque la primaria no disponible tras n escrituras.
- Cuando ocurre failover, [`co.javeriana.ReplicaManager`](src/main/java/co/javeriana/ReplicaManager.java) intenta sincronizar ficheros `libros.db` y `prestamos.db` entre `data/primaria` y `data/replica`.
- Para notificar y reenviar en caso de failover se usan propiedades: `remoteGcEndpoints`, `notifyGcEnqueue`, `siteId` (ver [ActorPrestamo.java](src/main/java/co/javeriana/ActorPrestamo.java)).
- Locks y archivos temporales: la persistencia usa `.lock` y `.tmp`; no borrar manualmente salvo para depuración.

Diagnóstico
- Salida y errores se imprimen por consola (System.out / System.err).
- Logs y comportamientos clave ubicados en clases: [`co.javeriana.GestorAlmacenamiento`](src/main/java/co/javeriana/GestorAlmacenamiento.java) y [`co.javeriana.GestorAlmacenamientoConReplica`](src/main/java/co/javeriana/GestorAlmacenamientoConReplica.java).

Si se requiere, crear configuraciones Run en IntelliJ para cada Main class listada arriba y arrancarlas en el orden apropiado: GC → ActorPrestamo → ActorDevolucion/ActorRenovacion → PS →