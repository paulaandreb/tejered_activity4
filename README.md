# TejeRed

Prototipo de un sistema distribuido de procesamiento de lotes, desarrollado para el mini proyecto de la Unidad 2 del curso Arquitectura de Software.

Un coordinador recibe lotes de tareas y los reparte entre nodos trabajadores conectados por TCP. Cada nodo ejecuta las tareas en un grupo de hilos. Si un nodo cae o se cuelga, sus tareas vuelven a la cola y ningún resultado se cuenta dos veces.

No requiere dependencias externas. Funciona con Python 3.11 o superior.

## Demostración rápida

```bash
python experiments/demo.py
```

La demostración levanta el coordinador y cuatro nodos, envía 2400 tareas, termina a la fuerza un nodo y muestra el resumen final.

## Ejecución en varias terminales

En cada terminal, desde la raíz del repositorio:

```bash
# Terminal 1
python -m tejered.coordinator --port 9400

# Terminal 2
python -m tejered.worker --id nodo-a --port 9400 --threads 4

# Terminal 3
python -m tejered.worker --id nodo-b --port 9400 --threads 4
```

Para usarlo en equipos distintos, inicia el coordinador con `--host 0.0.0.0` y los nodos con `--host <IP del coordinador>`. El protocolo no cifra ni autentica los mensajes, así que solo debe usarse en una red de confianza.

Enviar un lote desde Python:

```python
from tejered.proxy import CoordinatorProxy

cliente = CoordinatorProxy("127.0.0.1", 9400)
lote = cliente.submit({"kind": "word_count", "payload": {"text": "hola red"}} for _ in range(100))
print(cliente.wait(lote)["done"])
```

## Pruebas y mediciones

```bash
python -m unittest tests.test_tejered
python experiments/benchmark.py --reps 3 --out experiments/resultados.json
python experiments/benchmark.py --reps 1 --only e4
```

| Prueba | Pregunta |
| --- | --- |
| `e1` | ¿Cuánto mejora el rendimiento al agregar nodos? |
| `e2` | ¿Qué efecto tienen los hilos en tareas de E/S y de CPU? |
| `e3` | ¿Cuál es el costo máximo del coordinador? |
| `e4` | ¿Cómo se comporta el reparto con un nodo lento? |
| `e5` | ¿Se pierden tareas al fallar un nodo? |
| `e6`, `e7` | ¿Qué tamaño de ventana conviene? |
| `e8` | ¿Cuánto aporta cada ajuste por separado? |

## Patrones de diseño

| Patrón | Archivo |
| --- | --- |
| Proxy | `tejered/proxy.py` |
| Mediator | `tejered/coordinator.py` |
| Observer distribuido | `tejered/events.py` |
| Strategy | `tejered/strategies.py` |
| Thread Pool | `tejered/worker.py` |
| Heartbeat | `tejered/worker.py`, `tejered/coordinator.py` |

## Limitaciones

El coordinador es un punto único de falla y mantiene el estado en memoria. El prototipo no tiene autenticación ni cifrado. Está pensado para fines académicos.
