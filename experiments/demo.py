"""Demostración guiada para el video.

Levanta el coordinador y cuatro nodos como procesos, envía un lote, tumba un
nodo a mitad del trabajo y muestra que ninguna tarea se pierde.

Uso:
    python experiments/demo.py
    python experiments/demo.py --tareas 2000 --sin-falla
"""
from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tejered.cluster import LocalCluster, WorkerSpec  # noqa: E402
from tejered.coordinator import Coordinator, CoordinatorConfig  # noqa: E402
from tejered.proxy import CoordinatorProxy, EventListener  # noqa: E402

RESET, BOLD, GREEN, RED, CYAN = "\033[0m", "\033[1m", "\033[32m", "\033[31m", "\033[36m"
MENSAJES = {
    "node.up": (GREEN, "NODO ACTIVO", "{worker_id} con {capacity} hilos"),
    "node.down": (RED, "NODO CAÍDO", "{worker_id} ({reason}); {recovered} tareas vuelven a la cola"),
    "job.submitted": (CYAN, "LOTE RECIBIDO", "{job_id} con {total} tareas"),
    "job.finished": (CYAN, "LOTE TERMINADO", "{job_id}: {done} correctas, {failed} fallidas"),
}


def mostrar_evento(evento: dict) -> None:
    color, titulo, plantilla = MENSAJES[evento["topic"]]
    print(f"\n{color}{BOLD}[evento] {titulo}{RESET}: {plantilla.format(**evento['data'])}", flush=True)


def barra(hechas: int, total: int, ancho: int = 36) -> str:
    llenas = int(ancho * hechas / total)
    return "█" * llenas + "░" * (ancho - llenas)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tareas", type=int, default=2400)
    parser.add_argument("--ms", type=int, default=20)
    parser.add_argument("--sin-falla", action="store_true")
    args = parser.parse_args()

    coordinador = Coordinator(CoordinatorConfig())
    puerto = coordinador.start()
    print(f"{BOLD}Coordinador escuchando en 127.0.0.1:{puerto}{RESET}")
    observador = EventListener("127.0.0.1", puerto, mostrar_evento,
                               ["node.*", "job.*"])
    time.sleep(0.1)

    cluster = LocalCluster(puerto)
    especificaciones = [
        WorkerSpec("nodo-a", 4), WorkerSpec("nodo-b", 4),
        WorkerSpec("nodo-c", 4), WorkerSpec("nodo-lento", 4, slowdown=5.0),
    ]
    for especificacion in especificaciones:
        cluster.add_worker(especificacion)

    cliente = CoordinatorProxy("127.0.0.1", puerto)
    cliente.wait_for_workers(len(especificaciones))
    time.sleep(0.2)

    lote = cliente.submit(
        {"kind": "io_sim", "payload": {"ms": args.ms}} for _ in range(args.tareas)
    )

    if not args.sin_falla:
        def tumbar() -> None:
            time.sleep(1.2)
            print(f"\n{RED}{BOLD}>>> Se termina a la fuerza el proceso de nodo-b{RESET}", flush=True)
            cluster.kill("nodo-b")

        threading.Thread(target=tumbar, daemon=True).start()

    while True:
        estado = cliente.status(lote)
        hechas = estado["done"] + estado["failed"]
        print(f"\r{barra(hechas, args.tareas)} {hechas:>5}/{args.tareas}  "
              f"{estado['elapsed']:5.2f} s", end="", flush=True)
        if estado["finished"]:
            break
        time.sleep(0.1)
    time.sleep(0.2)

    final = cliente.status(lote, detail=True)
    estadisticas = cliente.stats()
    print(f"\n\n{BOLD}Resumen{RESET}")
    print(f"  Tareas correctas:  {final['done']} de {args.tareas}")
    print(f"  Tareas fallidas:   {final['failed']}")
    print(f"  Tiempo total:      {final['elapsed']:.2f} s")
    print(f"  Reasignadas:       {estadisticas['metrics']['reassigned']}")
    print(f"  Nodos perdidos:    {estadisticas['metrics']['workers_lost']}")
    print(f"\n{BOLD}Reparto entre los nodos que siguen activos{RESET}")
    for nombre, nodo in sorted(estadisticas["workers"].items()):
        print(f"  {nombre:<11} {nodo['completed']:>5} tareas")

    observador.close()
    cluster.shutdown()
    cliente.close()
    coordinador.stop()


if __name__ == "__main__":
    main()
