"""Genera los diagramas UML de componentes y de despliegue en docs/img."""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Arc, Circle, Polygon, Rectangle  # noqa: E402

OUT = Path(__file__).parent / "img"
OUT.mkdir(exist_ok=True)
INK = "#1f2933"
MUTED = "#52606d"
FILL = "#eef3f8"
PACKAGE = "#fbfcfd"
NODE = "#f4f6f8"
ARTIFACT = "#ffffff"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 8.5})


def canvas(width, height):
    fig, ax = plt.subplots(figsize=(width, height))
    ax.set_xlim(0, width)
    ax.set_ylim(0, height)
    ax.set_aspect("equal")
    ax.axis("off")
    fig.subplots_adjust(0, 0, 1, 1)
    return fig, ax


def package(ax, x, y, w, h, title, title_x=None):
    ax.add_patch(Rectangle((x, y), w, h, fc=PACKAGE, ec=INK, lw=1.1, zorder=1))
    ax.text(title_x if title_x is not None else x + 0.12, y + h - 0.13, title, ha="left", va="top", weight="bold", fontsize=9.5, zorder=5)


def component(ax, x, y, w, h, name, note=None):
    ax.add_patch(Rectangle((x, y), w, h, fc=FILL, ec=INK, lw=1, zorder=3))
    ix, iy = x + w - 0.26, y + h - 0.24
    ax.add_patch(Rectangle((ix, iy), 0.17, 0.15, fc="white", ec=INK, lw=0.7, zorder=4))
    for dy in (0.03, 0.09):
        ax.add_patch(Rectangle((ix - 0.05, iy + dy), 0.1, 0.035, fc="white", ec=INK, lw=0.6, zorder=5))
    center = y + h / 2
    ax.text(x + w / 2, center + 0.2 if note else center + 0.13, "«component»", ha="center", va="center",
            fontsize=7.2, color=MUTED, zorder=5)
    ax.text(x + w / 2, center + 0.02 if note else center - 0.08, name, ha="center", va="center",
            weight="bold", fontsize=8.6, zorder=5)
    if note:
        ax.text(x + w / 2, center - 0.2, note, ha="center", va="center", fontsize=7.2, color=MUTED, zorder=5)


def line(ax, points, dashed=False, arrow=False, color=INK):
    xs, ys = zip(*points)
    ax.plot(xs, ys, color=color, lw=1, ls=(0, (4, 3)) if dashed else "-", zorder=2,
            solid_capstyle="butt")
    if arrow:
        (x1, y1), (x2, y2) = points[-2], points[-1]
        size = max(((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5, 1e-6)
        tail = (x2 - (x2 - x1) / size * 0.1, y2 - (y2 - y1) / size * 0.1)
        ax.annotate("", xy=points[-1], xytext=tail, zorder=2,
                    arrowprops=dict(arrowstyle="->", color=color, lw=1, shrinkA=0, shrinkB=0))


def assembly(ax, x, y, label, side="right", opening="up"):
    """Conector de ensamble: círculo de interfaz provista y semicírculo requerido."""
    ax.add_patch(Circle((x, y), 0.08, fc="white", ec=INK, lw=1, zorder=4))
    theta = (20, 160) if opening == "down" else (200, 340)
    ax.add_patch(Arc((x, y), 0.3, 0.3, theta1=theta[0], theta2=theta[1], ec=INK, lw=1, zorder=4))
    if side == "right":
        ax.text(x + 0.24, y, label, ha="left", va="center", fontsize=7.8, style="italic", zorder=5)
    else:
        ax.text(x - 0.24, y, label, ha="right", va="center", fontsize=7.8, style="italic", zorder=5)


def diagram_components():
    fig, ax = canvas(8, 8.7)
    package(ax, 0.15, 7.15, 7.7, 1.42, "«subsystem» Cliente")
    component(ax, 0.4, 7.3, 2.0, 0.85, "CoordinatorProxy", "Patrón Proxy")
    component(ax, 5.5, 7.3, 2.1, 0.85, "EventListener", "Observador remoto")

    package(ax, 0.15, 3.0, 7.7, 3.5, "«subsystem» Coordinador", title_x=1.65)
    component(ax, 0.4, 3.2, 2.0, 2.75, "ConnectionHandler", "un hilo por conexión")
    component(ax, 2.95, 5.05, 2.05, 0.9, "TaskRegistry", "estado de tareas")
    component(ax, 5.5, 5.05, 2.1, 0.9, "EventBus", "Observer distribuido")
    component(ax, 5.5, 4.13, 2.1, 0.82, "HealthMonitor", "latidos")
    component(ax, 5.5, 3.2, 2.1, 0.82, "BalancingStrategy", "Patrón Strategy")
    component(ax, 2.95, 3.2, 2.05, 0.9, "Dispatcher", "ventana de envío")

    package(ax, 0.15, 0.15, 7.7, 2.2, "«subsystem» Nodo trabajador (N instancias)", title_x=4.2)
    component(ax, 0.4, 0.4, 2.0, 1.2, "HeartbeatSender", "hilo periódico")
    component(ax, 2.95, 0.4, 2.05, 1.2, "WorkerAgent", "lector de red")
    component(ax, 5.25, 0.4, 1.3, 1.2, "ThreadPool", "exec-1..N")
    component(ax, 6.72, 0.4, 1.05, 1.2, "Task\nCatalog")

    # Interfaces: el círculo pertenece al proveedor y el semicírculo a quien la requiere
    line(ax, [(1.4, 7.3), (1.4, 6.9)])
    line(ax, [(1.4, 6.67), (1.4, 5.95)])
    assembly(ax, 1.4, 6.75, "IGestionTrabajos  (SUBMIT, STATUS, STATS)", opening="down")
    line(ax, [(6.55, 7.3), (6.55, 6.9)])
    line(ax, [(6.55, 6.67), (6.55, 5.95)])
    assembly(ax, 6.55, 6.75, "IEventos", side="left", opening="down")

    line(ax, [(1.4, 3.2), (1.4, 2.74)])
    line(ax, [(1.4, 2.51), (1.4, 1.6)])
    assembly(ax, 1.4, 2.66, "INodo  (REGISTER,\nHEARTBEAT, RESULT)", opening="up")
    line(ax, [(3.97, 3.2), (3.97, 2.81)])
    line(ax, [(3.97, 2.58), (3.97, 1.6)])
    assembly(ax, 3.97, 2.66, "IEjecucion  (ASSIGN)", opening="down")
    line(ax, [(3.3, 1.6), (3.3, 2.0), (1.4, 2.0)])

    # Dependencias internas
    line(ax, [(2.4, 5.5), (2.95, 5.5)], dashed=True, arrow=True)
    line(ax, [(5.0, 5.5), (5.5, 5.5)], dashed=True, arrow=True)
    line(ax, [(3.97, 4.1), (3.97, 5.05)], dashed=True, arrow=True)
    line(ax, [(5.0, 3.61), (5.5, 3.61)], dashed=True, arrow=True)
    line(ax, [(5.5, 4.54), (4.5, 4.54), (4.5, 5.05)], dashed=True, arrow=True)
    line(ax, [(5.0, 1.0), (5.25, 1.0)], dashed=True, arrow=True)
    line(ax, [(6.55, 1.0), (6.72, 1.0)], dashed=True, arrow=True)
    fig.savefig(OUT / "uml_componentes.png", dpi=220)
    plt.close(fig)


def device(ax, x, y, w, h, title, depth=0.16):
    ax.add_patch(Polygon([(x, y + h), (x + depth, y + h + depth), (x + w + depth, y + h + depth), (x + w, y + h)],
                         fc=NODE, ec=INK, lw=1, zorder=2))
    ax.add_patch(Polygon([(x + w, y), (x + w + depth, y + depth), (x + w + depth, y + h + depth), (x + w, y + h)],
                         fc="#dfe5eb", ec=INK, lw=1, zorder=2))
    ax.add_patch(Rectangle((x, y), w, h, fc=NODE, ec=INK, lw=1, zorder=3))
    ax.text(x + w / 2, y + h - 0.2, "«device»", ha="center", va="center", fontsize=7.2, color=MUTED, zorder=5)
    ax.text(x + w / 2, y + h - 0.39, title, ha="center", va="center", weight="bold", fontsize=8.8, zorder=5)


def environment(ax, x, y, w, h, title):
    ax.add_patch(Rectangle((x, y), w, h, fc="white", ec=INK, lw=0.9, zorder=4))
    ax.text(x + w / 2, y + h - 0.08, f"«executionEnvironment»\n{title}", ha="center", va="top",
            fontsize=7, color=MUTED, zorder=5, linespacing=1.25)


def artifact(ax, x, y, w, h, name):
    ax.add_patch(Rectangle((x, y), w, h, fc=FILL, ec=INK, lw=0.8, zorder=5))
    ax.text(x + w / 2, y + h / 2 + 0.1, "«artifact»", ha="center", va="center", fontsize=6.8, color=MUTED, zorder=6)
    ax.text(x + w / 2, y + h / 2 - 0.1, name, ha="center", va="center", fontsize=7.8, weight="bold", zorder=6)


def link(ax, a, b, label, offset=(0.08, 0)):
    line(ax, [a, b])
    ax.text((a[0] + b[0]) / 2 + offset[0], (a[1] + b[1]) / 2 + offset[1], label,
            fontsize=7.6, ha="left", va="center", zorder=7,
            bbox=dict(fc="white", ec="none", pad=1.2))


def diagram_deployment():
    fig, ax = canvas(8, 8.15)
    device(ax, 2.55, 6.42, 2.9, 1.5, "Equipo del usuario")
    environment(ax, 2.72, 6.52, 2.56, 0.93, "Python 3.11+")
    artifact(ax, 3.2, 6.58, 1.6, 0.47, "proxy.py")

    ax.add_patch(Rectangle((0.15, 0.15), 7.7, 5.72, fc="#fafbfc", ec=MUTED, lw=1, ls=(0, (6, 4)), zorder=1))
    ax.text(0.3, 5.72, "Red privada de nube (10.0.0.0/24)", ha="left", va="center", weight="bold",
            fontsize=8.8, color=MUTED, zorder=5)

    device(ax, 1.9, 3.4, 4.2, 1.95, "Máquina virtual: coordinador")
    environment(ax, 2.08, 3.5, 3.84, 1.35, "Python 3.11+")
    artifact(ax, 2.22, 3.58, 1.1, 0.72, "coordinator.py")
    artifact(ax, 3.45, 3.58, 1.1, 0.72, "events.py")
    artifact(ax, 4.68, 3.58, 1.1, 0.72, "strategies.py")

    for x, title in ((0.35, "Nodo 1"), (3.0, "Nodo 2"), (5.65, "Nodo N")):
        device(ax, x, 0.32, 1.95, 1.82, f"VM: {title}")
        environment(ax, x + 0.12, 0.42, 1.71, 1.1, "Python 3.11+")
        artifact(ax, x + 0.3, 0.5, 1.35, 0.47, "worker.py")
    ax.text(5.25, 1.1, "...", ha="center", va="center", fontsize=15, weight="bold", zorder=6)

    link(ax, (4.0, 6.42), (4.0, 5.51), "«TCP» 9400: SUBMIT, STATUS, EVENT", (0.1, 0.13))
    link(ax, (2.3, 3.4), (1.35, 2.3), "«TCP» 9400", (-1.05, 0))
    link(ax, (4.0, 3.4), (4.0, 2.3), "«TCP» 9400\nASSIGN, RESULT,\nHEARTBEAT", (0.08, 0))
    link(ax, (5.8, 3.4), (6.63, 2.3), "«TCP» 9400", (0.12, 0))
    fig.savefig(OUT / "uml_despliegue.png", dpi=220)
    plt.close(fig)


if __name__ == "__main__":
    diagram_components()
    diagram_deployment()
