"""Pruebas de escalabilidad y robustez de TejeRed.

Uso:
    python experiments/benchmark.py --out experiments/resultados.json
    python experiments/benchmark.py --only e4 --reps 1
"""
from __future__ import annotations

import argparse
import json
import platform
import os
import statistics
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tejered.cluster import LocalCluster, WorkerSpec  # noqa: E402
from tejered.coordinator import Coordinator, CoordinatorConfig  # noqa: E402
from tejered.proxy import CoordinatorProxy, EventListener  # noqa: E402

V1 = CoordinatorConfig(strategy="round_robin", window=0, nodelay=False, split_writes=True)
V2 = CoordinatorConfig(strategy="least_loaded", window=1.0, nodelay=True, split_writes=False)
HEARTBEAT = 0.3


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


class Run:
    """Levanta un clúster nuevo para que cada medición sea independiente."""

    def __init__(self, config: CoordinatorConfig, specs: list[WorkerSpec]):
        self.config = config
        self.specs = specs

    def __enter__(self) -> "Run":
        self.coordinator = Coordinator(self.config)
        self.port = self.coordinator.start()
        self.cluster = LocalCluster(
            self.port, HEARTBEAT, self.config.nodelay, self.config.split_writes
        )
        for spec in self.specs:
            self.cluster.add_worker(spec)
        self.proxy = CoordinatorProxy("127.0.0.1", self.port)
        self.proxy.wait_for_workers(len(self.specs))
        return self

    def __exit__(self, *_) -> None:
        self.cluster.shutdown()
        self.proxy.close()
        self.coordinator.stop()


def workers(count: int, threads: int = 4, slow: dict[str, float] | None = None) -> list[WorkerSpec]:
    slow = slow or {}
    return [WorkerSpec(f"w{i}", threads, slow.get(f"w{i}", 1.0)) for i in range(count)]


def execute(config, specs, kind, payload, total) -> dict:
    with Run(config, specs) as run:
        job = run.proxy.submit({"kind": kind, "payload": payload} for _ in range(total))
        status = run.proxy.wait(job, timeout=300)
        stats = run.proxy.stats()
    latencies = status["latencies"]
    return {
        "elapsed": status["elapsed"],
        "throughput": status["done"] / status["elapsed"],
        "done": status["done"],
        "failed": status["failed"],
        "p50_ms": percentile(latencies, 0.50) * 1000,
        "p95_ms": percentile(latencies, 0.95) * 1000,
        "per_worker": {k: v["completed"] for k, v in stats["workers"].items()},
    }


def repeated(reps: int, *args) -> dict:
    samples = [execute(*args) for _ in range(reps)]
    keys = ("elapsed", "throughput", "p50_ms", "p95_ms")
    summary = {key: statistics.median(s[key] for s in samples) for key in keys}
    summary["throughput_min"] = min(s["throughput"] for s in samples)
    summary["throughput_max"] = max(s["throughput"] for s in samples)
    summary["done"] = samples[0]["done"]
    summary["all_complete"] = all(s["failed"] == 0 and s["done"] == samples[0]["done"] for s in samples)
    summary["reps"] = reps
    middle = sorted(samples, key=lambda s: s["elapsed"])[len(samples) // 2]
    summary["per_worker"] = middle["per_worker"]
    return summary


def e1_scale_out(reps: int) -> dict:
    """Más nodos, misma carga de E/S."""
    results: dict = {"params": {"tasks": 1600, "ms": 20, "threads": 4}}
    for label, config in (("v1", V1), ("v2", V2)):
        results[label] = {}
        for count in (1, 2, 4, 8):
            results[label][count] = repeated(reps, config, workers(count), "io_sim", {"ms": 20}, 1600)
            print("E1", label, count, round(results[label][count]["throughput"], 1), flush=True)
    return results


def e2_threads(reps: int) -> dict:
    """Más hilos en un solo nodo: E/S frente a CPU."""
    results: dict = {"params": {"io_tasks": 400, "io_ms": 20, "cpu_tasks": 40, "cpu_rounds": 2000000},
                     "io": {}, "cpu": {}}
    for threads in (1, 2, 4, 8, 16):
        results["io"][threads] = repeated(
            reps, V2, workers(1, threads), "io_sim", {"ms": 20}, 400
        )
        results["cpu"][threads] = repeated(
            reps, V2, workers(1, threads), "cpu_hash", {"rounds": 2000000}, 40
        )
        print("E2", threads, round(results["io"][threads]["throughput"], 1),
              round(results["cpu"][threads]["throughput"], 1), flush=True)
    results["cpu_processes"] = {}
    for count in (1, 2, 4):
        results["cpu_processes"][count] = repeated(
            reps, V2, workers(count, 1), "cpu_hash", {"rounds": 2000000}, 40
        )
        print("E2 procesos", count, round(results["cpu_processes"][count]["throughput"], 1), flush=True)
    return results


def e3_overhead(reps: int) -> dict:
    """Tareas vacías: el límite lo pone el middleware, no el trabajo."""
    results: dict = {"params": {"tasks": 30000, "threads": 4}}
    for label, config in (("v1", V1), ("v2", V2)):
        results[label] = {}
        for count in (1, 2, 4, 8):
            results[label][count] = repeated(reps, config, workers(count), "noop", {}, 30000)
            print("E3", label, count, round(results[label][count]["throughput"], 1), flush=True)
    return results


def e4_heterogeneous(reps: int) -> dict:
    """Un nodo cinco veces más lento: efecto de la política de reparto."""
    slow = {"w0": 5.0}
    variants = {
        "rr_sin_ventana": replace(V2, strategy="round_robin", window=0),
        "ll_sin_ventana": replace(V2, strategy="least_loaded", window=0),
        "rr_con_ventana": replace(V2, strategy="round_robin", window=1.0),
        "ll_con_ventana": replace(V2, strategy="least_loaded", window=1.0),
    }
    results: dict = {"params": {"tasks": 800, "ms": 20, "threads": 4, "workers": 4, "slowdown_w0": 5}}
    for label, config in variants.items():
        results[label] = repeated(reps, config, workers(4, 4, slow), "io_sim", {"ms": 20}, 800)
        print("E4", label, round(results[label]["elapsed"], 2), results[label]["per_worker"], flush=True)
    return results


def e6_window(reps: int) -> dict:
    """Calibración del tamaño de la ventana de envío."""
    slow = {"w0": 5.0}
    results: dict = {"params": {"tasks": 800, "ms": 20, "threads": 4, "workers": 4, "slowdown_w0": 5}}
    for window in (1.0, 1.5, 2.0, 4.0, 8.0):
        config = replace(V2, window=window)
        results[window] = repeated(reps, config, workers(4, 4, slow), "io_sim", {"ms": 20}, 800)
        print("E6", window, round(results[window]["elapsed"], 2), flush=True)
    return results


def e7_window_overhead(reps: int) -> dict:
    """Costo de la ventana cuando las tareas son vacías o el grupo es homogéneo."""
    results: dict = {"params": {"noop_tasks": 30000, "io_tasks": 1600, "ms": 20, "threads": 4, "workers": 4},
                     "noop": {}, "io": {}}
    for window in (1.0, 1.5, 2.0, 4.0, 8.0, 16.0):
        config = replace(V2, window=window)
        results["noop"][window] = repeated(reps, config, workers(4), "noop", {}, 30000)
        results["io"][window] = repeated(reps, config, workers(4), "io_sim", {"ms": 20}, 1600)
        print("E7", window, round(results["noop"][window]["throughput"]),
              round(results["io"][window]["throughput"]), flush=True)
    return results


def e8_isolate_adjustments(reps: int) -> dict:
    """Aísla el efecto de cada ajuste sobre el costo máximo del coordinador."""
    variants = {
        "v1": V1,
        "solo_red": replace(V1, nodelay=True, split_writes=False),
        "solo_ventana": replace(V1, window=1.0),
        "solo_estrategia": replace(V1, strategy="least_loaded"),
        "v2": V2,
    }
    # La variante "solo_ventana" se detiene por la interacción con Nagle; usa un lote menor.
    sizes = {label: 1000 if label == "solo_ventana" else 5000 for label in variants}
    results: dict = {"params": {"tasks": sizes, "threads": 4}}
    for label, config in variants.items():
        results[label] = {}
        for count in (1, 8):
            results[label][count] = repeated(reps, config, workers(count), "noop", {}, sizes[label])
        print("E8", label, {k: round(v["throughput"]) for k, v in results[label].items()}, flush=True)
    return results


def fault_run(config: CoordinatorConfig, mode: str, tasks: int = 2400) -> dict:
    events: list[dict] = []
    timeline: list[tuple[float, int]] = []
    with Run(config, workers(4)) as run:
        listener = EventListener("127.0.0.1", run.port, events.append, ["node.down"])
        time.sleep(0.05)
        job = run.proxy.submit({"kind": "io_sim", "payload": {"ms": 20}} for _ in range(tasks))
        started = time.time()
        fault_at: list[float] = []

        def inject() -> None:
            time.sleep(0.75)
            fault_at.append(time.time())
            (run.cluster.kill if mode == "kill" else run.cluster.freeze)("w0")

        threading.Thread(target=inject, daemon=True).start()
        while True:
            status = run.proxy.status(job)
            timeline.append((time.time() - started, status["done"] + status["failed"]))
            if status["finished"]:
                break
            time.sleep(0.05)
        final = run.proxy.status(job, detail=True)
        if mode == "freeze":
            run.cluster.resume("w0")
            time.sleep(0.5)
        stats = run.proxy.stats()
        after_resume = run.proxy.status(job)
        time.sleep(0.1)
        listener.close()
    detected = events[0]["ts"] - fault_at[0] if events else None
    unique_ok = sum(1 for state in final["states"].values() if state == "DONE")
    return {
        "elapsed": final["elapsed"],
        "done": final["done"],
        "failed": final["failed"],
        "unique_done": unique_ok,
        "done_after_resume": after_resume["done"],
        "detection_s": detected,
        "reassigned": stats["metrics"]["reassigned"],
        "duplicates_ignored": stats["metrics"]["duplicates_ignored"],
        "reason": events[0]["data"]["reason"] if events else None,
        "timeline": timeline,
        "fault_at": fault_at[0] - started,
    }


def e5_faults(reps: int) -> dict:
    results: dict = {"params": {"tasks": 2400, "ms": 20, "workers": 4, "threads": 4, "fault_at_s": 0.75,
                                "heartbeat_timeout_s": V2.heartbeat_timeout,
                                "heartbeat_interval_s": HEARTBEAT}}
    for label, config in (("v1", V1), ("v2", V2)):
        results[label] = {}
        for mode in ("kill", "freeze"):
            samples = [fault_run(config, mode) for _ in range(reps)]
            middle = sorted(samples, key=lambda s: s["elapsed"])[len(samples) // 2]
            results[label][mode] = {
                "elapsed": statistics.median(s["elapsed"] for s in samples),
                "detection_s": statistics.median(s["detection_s"] for s in samples),
                "reassigned": statistics.median(s["reassigned"] for s in samples),
                "duplicates_ignored": statistics.median(s["duplicates_ignored"] for s in samples),
                "all_complete": all(
                    s["done"] == s["unique_done"] == s["done_after_resume"] == 2400 and s["failed"] == 0
                    for s in samples
                ),
                "reason": middle["reason"],
                "timeline": middle["timeline"],
                "fault_at": middle["fault_at"],
                "reps": reps,
            }
            print("E5", label, mode, round(results[label][mode]["elapsed"], 2),
                  results[label][mode]["reassigned"], results[label][mode]["all_complete"], flush=True)
    return results


EXPERIMENTS = {"e1": e1_scale_out, "e2": e2_threads, "e3": e3_overhead,
               "e4": e4_heterogeneous, "e5": e5_faults, "e6": e6_window,
               "e7": e7_window_overhead, "e8": e8_isolate_adjustments}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--only", nargs="*", choices=sorted(EXPERIMENTS))
    parser.add_argument("--out", default="experiments/resultados.json")
    args = parser.parse_args()
    out = Path(args.out)
    data = json.loads(out.read_text()) if out.exists() else {}
    data["entorno"] = {
        "python": platform.python_version(),
        "sistema": platform.platform(),
        "cpu_logicas": os.cpu_count(),
        "repeticiones": args.reps,
    }
    for name in args.only or sorted(EXPERIMENTS):
        started = time.time()
        data[name] = EXPERIMENTS[name](args.reps)
        print(f"== {name} terminó en {time.time() - started:.0f} s", flush=True)
        out.write_text(json.dumps(data, indent=1))


if __name__ == "__main__":
    main()
