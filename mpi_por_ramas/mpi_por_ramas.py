"""Tarea 2: tableros en orden, ramas de cada tablero en paralelo.

El rango 0 recorre los IDs en el orden de seeds.txt. Para cada uno genera
ramas con las opciones válidas de las primeras casillas vacías, las reparte
entre los N procesos (él incluido) y espera a que el tablero termine antes de
pasar al siguiente. Al aparecer la solución, los demás abandonan sus ramas.

Tiempo parcial = tiempo real de ese tablero (reparto + búsqueda + cierre).
Tiempo total = tiempo real desde el primer reparto hasta el último resultado.
"""

import argparse
from pathlib import Path
import sys
import time

AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(AQUI))

import ramas  # noqa: E402  (agrega la raíz del proyecto a sys.path)
from mpi4py import MPI  # noqa: E402

from baseline_secuencial import leer_seeds  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Sudoku MPI: reparto de ramas de cada tablero.")
    parser.add_argument("--archivo", type=Path, default=ramas.RAIZ / "seeds.txt",
                        help="archivo de seeds (por defecto: ../seeds.txt)")
    seleccion = parser.add_mutually_exclusive_group()
    seleccion.add_argument("--id", "--seed", dest="id", help="resolver solo la seed con este ID")
    seleccion.add_argument("--all", action="store_true", help="resolver todos los seeds")
    parser.add_argument("--factor", type=int, default=ramas.FACTOR,
                        help="ramas mínimas por proceso (por defecto: %(default)s)")
    parser.add_argument("--cada", type=int, default=ramas.CADA,
                        help="nodos entre revisiones del buzón (por defecto: %(default)s)")
    parser.add_argument("--verificar", action="store_true",
                        help="comprobar cada solución (fuera del tiempo medido)")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    if comm.rank != 0:
        ramas.trabajador(comm, args.cada)
        return

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
    procesados = resueltos = 0
    inicio = time.perf_counter()
    for nombre, original in seleccionados:
        resuelto, segundos, cantidad, solucion = ramas.resolver_tablero(
            comm, ramas.copiar(original), args.factor, args.cada)
        procesados += 1
        resueltos += resuelto
        print(f"\nID: {nombre}")
        print("Resuelto" if resuelto else "Sin solución")
        print(f"Ramas: {cantidad}")
        if args.verificar and resuelto:
            print("Verificación:", "correcta" if ramas.es_solucion(original, solucion) else "INCORRECTA")
        etiqueta = "Tiempo" if args.id else "Tiempo parcial"
        print(f"{etiqueta}: {segundos:.6f} segundos", flush=True)
    total = time.perf_counter() - inicio
    ramas.despedir(comm)

    if not args.id:
        print(f"\nIDs procesados: {procesados}/{len(seleccionados)} (resueltos: {resueltos})")
        print(f"Tiempo total: {total:.6f} segundos")


if __name__ == "__main__":
    main()
