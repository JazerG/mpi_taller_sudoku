"""Tarea 1: reparte los IDs (tableros completos) entre procesos MPI.

Reparto dinámico (autoplanificación): el rango 0 guarda la cola de IDs y se
los entrega uno a uno a quien quede libre, así los tableros largos no dejan a
un proceso con más carga que los demás. El rango 0 también resuelve tableros:
su hilo principal toma IDs de la misma cola mientras un hilo coordinador
atiende las peticiones MPI de los demás procesos.

Cada tablero se resuelve con resolver_con_tiempo() de ../sudoku.py, sin
modificar el resolvedor. El tiempo total es tiempo real (de pared) medido en
el rango 0 desde que empieza el reparto hasta que llega el último resultado.
"""

import argparse
from pathlib import Path
import sys
import threading
import time

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from mpi4py import MPI  # noqa: E402

from baseline_secuencial import leer_seeds  # noqa: E402
from sudoku import resolver_con_tiempo  # noqa: E402

TAG_PEDIR = 1      # trabajador -> 0: (resultado_anterior | None)
TAG_TAREA = 2      # 0 -> trabajador: (id, tablero) | None para terminar
ESPERA = 0.0005    # pausa entre sondeos; evita la espera activa de MPI


def recibir(comm, fuente, tag):
    """recv que sondea con pausas para no gastar un núcleo esperando."""
    estado = MPI.Status()
    while not comm.iprobe(source=fuente, tag=tag, status=estado):
        time.sleep(ESPERA)
    return comm.recv(source=estado.Get_source(), tag=tag), estado.Get_source()


def resolver_id(identificador, original):
    tablero = [fila[:] for fila in original]
    resuelto, segundos = resolver_con_tiempo(tablero)
    return identificador, resuelto, segundos


def imprimir(identificador, resuelto, segundos, rango):
    print(f"\nID: {identificador}")
    print("Resuelto" if resuelto else "Sin solución")
    print(f"Proceso: {rango}")
    print(f"Tiempo parcial: {segundos:.6f} segundos", flush=True)


def trabajador(comm):
    resultado = None
    while True:
        comm.send(resultado, dest=0, tag=TAG_PEDIR)
        tarea, _ = recibir(comm, 0, TAG_TAREA)
        if tarea is None:
            return
        resultado = resolver_id(*tarea)


class Coordinador:
    """Estado compartido del rango 0: cola de IDs y resultados recibidos."""

    def __init__(self, seleccionados):
        self.cola = list(seleccionados)
        self.siguiente = 0
        self.candado = threading.Lock()
        self.resultados = {}   # id -> (resuelto, segundos, rango)

    def tomar(self):
        with self.candado:
            if self.siguiente >= len(self.cola):
                return None
            tarea = self.cola[self.siguiente]
            self.siguiente += 1
            return tarea

    def registrar(self, identificador, resuelto, segundos, rango):
        with self.candado:
            self.resultados[identificador] = (resuelto, segundos, rango)
            imprimir(identificador, resuelto, segundos, rango)


def atender_trabajadores(comm, coord):
    """Hilo del rango 0: entrega IDs y recoge resultados hasta despedir a todos."""
    activos = comm.size - 1
    while activos:
        resultado, origen = recibir(comm, MPI.ANY_SOURCE, TAG_PEDIR)
        if resultado is not None:
            coord.registrar(*resultado, origen)
        tarea = coord.tomar()
        comm.send(tarea, dest=origen, tag=TAG_TAREA)
        if tarea is None:
            activos -= 1


def coordinador(comm, seleccionados):
    coord = Coordinador(seleccionados)
    hilo = None
    if comm.size > 1:
        hilo = threading.Thread(target=atender_trabajadores, args=(comm, coord))
        hilo.start()
    while True:                       # el rango 0 también resuelve tableros
        tarea = coord.tomar()
        if tarea is None:
            break
        coord.registrar(*resolver_id(*tarea), 0)
    if hilo:
        hilo.join()
    return coord.resultados


def main():
    parser = argparse.ArgumentParser(description="Sudoku MPI: reparto de tableros entre procesos.")
    parser.add_argument("--archivo", type=Path, default=RAIZ / "seeds.txt",
                        help="archivo de seeds (por defecto: ../seeds.txt)")
    seleccion = parser.add_mutually_exclusive_group()
    seleccion.add_argument("--id", "--seed", dest="id", help="resolver solo la seed con este ID")
    seleccion.add_argument("--all", action="store_true", help="resolver todos los seeds")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    if comm.rank != 0:
        trabajador(comm)
        return

    if comm.size > 1 and MPI.Query_thread() < MPI.THREAD_SERIALIZED:
        print(f"MPI no ofrece THREAD_SERIALIZED (nivel {MPI.Query_thread()}).", file=sys.stderr)
        comm.Abort(1)

    try:
        seeds = leer_seeds(args.archivo)
    except OSError as error:
        print(f"no se pudo leer {args.archivo}: {error}", file=sys.stderr)
        comm.Abort(2)
    if args.id:
        if args.id not in seeds:
            print(f"ID '{args.id}' no existe. Disponibles: {', '.join(seeds)}", file=sys.stderr)
            comm.Abort(2)
        seleccionados = [(args.id, seeds[args.id])]
    else:
        seleccionados = list(seeds.items())

    print(f"Procesos MPI: {comm.size}; tableros: {len(seleccionados)}", flush=True)
    inicio = time.perf_counter()
    resultados = coordinador(comm, seleccionados)
    total = time.perf_counter() - inicio

    resueltos = sum(1 for r, _, _ in resultados.values() if r)
    suma = sum(s for _, s, _ in resultados.values())
    print(f"\nIDs procesados: {len(resultados)}/{len(seleccionados)} (resueltos: {resueltos})")
    print(f"Suma de tiempos de búsqueda (referencia, no es el total): {suma:.6f} segundos")
    print(f"Tiempo total: {total:.6f} segundos")


if __name__ == "__main__":
    main()
