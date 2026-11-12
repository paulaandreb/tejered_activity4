"""Genera las gráficas del informe a partir de experiments/resultados.json."""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).parent
OUT = ROOT / "img"
OUT.mkdir(exist_ok=True)
DATA = json.loads((ROOT.parent / "experiments" / "resultados.json").read_text())

BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, SECOND, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": AXIS, "axes.labelcolor": SECOND,
    "xtick.color": SECOND, "ytick.color": SECOND, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": GRID, "grid.linewidth": 0.7,
    "axes.axisbelow": True, "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "legend.frameon": False, "xtick.major.size": 0, "ytick.major.size": 0,
})


def comma(value, _=None):
    text = f"{value:g}"
    return text.replace(".", ",")


def thousands(value, _=None):
    return f"{value:,.0f}".replace(",", ".")


def plot(ax, x, y, color, label, marker="o", **kwargs):
    return ax.plot(x, y, color=color, lw=2, marker=marker, ms=7, mec=SURFACE, mew=1.3,
                   label=label, clip_on=False, zorder=3, **kwargs)


def save(fig, name):
    fig.tight_layout()
    fig.savefig(OUT / name, dpi=220)
    plt.close(fig)


def series(block, field="throughput"):
    keys = sorted(block, key=float)
    return [float(k) for k in keys], [block[k][field] for k in keys]


def e1():
    x, y = series(DATA["e1"]["v2"])
    fig, ax = plt.subplots(figsize=(6.3, 3.3))
    ideal = [y[0] * n for n in x]
    ax.plot(x, ideal, color=MUTED, lw=1.4, ls=(0, (4, 3)), label="Escalado lineal ideal", zorder=2)
    plot(ax, x, y, BLUE, "Medido")
    for a, b in zip(x, y):
        ax.annotate(f"{b:,.0f}".replace(",", "."), (a, b), xytext=(0, 9), textcoords="offset points",
                    ha="center", color=INK, fontsize=8.5)
    ax.set_xticks(x)
    ax.set_xlabel("Nodos trabajadores (4 hilos cada uno)")
    ax.set_ylabel("Tareas por segundo")
    ax.set_ylim(0, max(ideal) * 1.15)
    ax.yaxis.set_major_formatter(thousands)
    ax.legend(loc="upper left")
    save(fig, "e1_escala.png")


def e2():
    fig, (left, right) = plt.subplots(1, 2, figsize=(6.3, 3.2))
    x, y = series(DATA["e2"]["io"])
    plot(left, x, y, BLUE, "E/S")
    left.set_xticks(x, [str(int(v)) for v in x])
    left.set_title("Tareas de E/S, un nodo", fontsize=9.5, loc="left", color=INK)
    left.set_xlabel("Hilos del nodo")
    left.set_ylabel("Tareas por segundo")
    left.set_ylim(0, max(y) * 1.1)

    x, y = series(DATA["e2"]["cpu"])
    px, py = series(DATA["e2"]["cpu_processes"])
    plot(right, x, y, ORANGE, "Más hilos, 1 proceso", marker="s")
    plot(right, px, py, BLUE, "Más procesos, 1 hilo")
    right.set_xticks(x, [str(int(v)) for v in x])
    right.set_title("Tareas de CPU", fontsize=9.5, loc="left", color=INK)
    right.set_xlabel("Hilos o procesos")
    right.set_ylim(0, max(py) * 1.45)
    right.legend(loc="upper left", fontsize=8)
    save(fig, "e2_hilos.png")


def e3():
    fig, ax = plt.subplots(figsize=(6.3, 3.2))
    x, v1 = series(DATA["e3"]["v1"])
    _, v2 = series(DATA["e3"]["v2"])
    plot(ax, x, v1, ORANGE, "Versión 1", marker="s")
    plot(ax, x, v2, BLUE, "Versión 2")
    ax.set_xticks(x)
    ax.set_xlabel("Nodos trabajadores")
    ax.set_ylabel("Tareas vacías por segundo")
    ax.set_ylim(0, max(v1 + v2) * 1.15)
    ax.yaxis.set_major_formatter(thousands)
    ax.legend(loc="upper right")
    save(fig, "e3_costo.png")


def e4():
    labels = {
        "rr_sin_ventana": "Por turnos, sin ventana",
        "ll_sin_ventana": "Menor carga, sin ventana",
        "rr_con_ventana": "Por turnos, con ventana",
        "ll_con_ventana": "Menor carga, con ventana",
    }
    keys = list(labels)[::-1]
    values = [DATA["e4"][key]["elapsed"] for key in keys]
    fig, ax = plt.subplots(figsize=(6.3, 2.6))
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", visible=True)
    bars = ax.barh([labels[k] for k in keys], values, height=0.56, color=BLUE, zorder=3)
    for bar, value in zip(bars, values):
        ax.text(value + 0.06, bar.get_y() + bar.get_height() / 2, f"{value:.2f} s".replace(".", ","),
                va="center", color=INK, fontsize=9)
    bound = 800 / (3 * 4 / 0.02 + 4 / 0.1)
    ax.axvline(bound, color=SECOND, lw=1.2, ls=(0, (4, 3)), zorder=4)
    ax.text(bound + 0.06, 3.46, f"Mínimo teórico: {bound:.2f} s".replace(".", ","), fontsize=8.5, color=SECOND)
    ax.set_xlim(0, max(values) * 1.15)
    ax.set_ylim(-0.5, 3.75)
    ax.set_xlabel("Tiempo total del lote (segundos)")
    ax.spines["left"].set_visible(False)
    save(fig, "e4_heterogeneo.png")


def e5():
    fig, ax = plt.subplots(figsize=(6.3, 3.2))
    kill = DATA["e5"]["v2"]["kill"]
    freeze = DATA["e5"]["v2"]["freeze"]
    ax.plot(*zip(*kill["timeline"]), color=BLUE, lw=2, label="Proceso terminado")
    ax.plot(*zip(*freeze["timeline"]), color=ORANGE, lw=2, ls=(0, (5, 2)), label="Proceso congelado")
    at = kill["fault_at"]
    ax.axvline(at, color=SECOND, lw=1.2, ls=(0, (2, 2)))
    ax.text(at + 0.06, 2300, "Falla inyectada", color=SECOND, fontsize=8.5, va="top")
    ax.set_xlabel("Tiempo desde el envío del lote (segundos)")
    ax.set_ylabel("Tareas terminadas")
    ax.xaxis.set_major_formatter(comma)
    ax.set_xlim(0, max(kill["elapsed"], freeze["elapsed"]) * 1.03)
    ax.set_ylim(0, 2500)
    ax.yaxis.set_major_formatter(thousands)
    ax.legend(loc="lower right")
    save(fig, "e5_fallas.png")


def e6():
    fig, ax = plt.subplots(figsize=(6.3, 2.9))
    keys = [k for k in DATA["e6"] if k != "params"]
    x = sorted(float(k) for k in keys)
    lookup = {float(k): DATA["e6"][k]["elapsed"] for k in keys}
    y = [lookup[v] for v in x]
    plot(ax, x, y, BLUE, "Tiempo")
    for a, b in zip(x, y):
        ax.annotate(f"{b:.2f}".replace(".", ","), (a, b), xytext=(0, 9), textcoords="offset points",
                    ha="center", color=INK, fontsize=8.5)
    ax.set_xlabel("Ventana de envío (tareas en vuelo por hilo)")
    ax.set_ylabel("Tiempo total (s)")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{v:g}".replace(".", ",") for v in x])
    ax.set_ylim(0, max(y) * 1.25)
    ax.yaxis.set_major_formatter(comma)
    save(fig, "e6_ventana.png")


if __name__ == "__main__":
    e1(); e2(); e3(); e4(); e5(); e6()
