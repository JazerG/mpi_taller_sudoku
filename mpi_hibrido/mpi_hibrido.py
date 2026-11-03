"""Tarea 3: reparto de IDs entre grupos + reparto de ramas dentro de cada grupo.

Los N procesos se dividen con Comm.Split en G = N // S grupos de S procesos
(si N no es múltiplo de S, el último grupo recibe los procesos sobrantes).

* Nivel 1 (COMM_WORLD): el rango 0 guarda la cola de IDs. El líder de cada
  grupo (rango 0 del grupo) le pide un ID, lo resuelve con su grupo y al
  pedir el siguiente entrega el resultado anterior. Reparto dinámico: el
  grupo que se desocupa toma el siguiente ID.
* Nivel 2 (comunicador del grupo): el líder resuelve su tablero con el mismo
  motor de ramas de ../mpi_por_ramas/ramas.py (ramas de las primeras casillas
  vacías, cancelación al encontrar la solución).

El rango 0 también es líder del grupo 0: atiende los pedidos de IDs de los
otros líderes desde el mismo punto en que revisa su buzón de ramas.

Tiempo total = tiempo real en el rango 0 desde el primer reparto hasta
recibir el último resultado.
"""

import argparse
from pathlib import Path
import sys
import time

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "mpi_por_ramas"))

import ramas  # noqa: E402  (agrega la raíz del proyecto a sys.path)
from mpi4py import MPI  # noqa: E402

from baseline_secuencial import leer_seeds  # noqa: E402

TAG_ID = 21   # líder -> 0: resultado anterior | None ; 0 -> líder: (id, tablero) | None


def resolver_id(grupo, nombre, original, args, numero_grupo):
    resuelto, segundos, cantidad, solucion = ramas.resolver_tablero(
        grupo, ramas.copiar(original), args.factor, args.cada,
        gancho=args.gancho)
    verificado = ramas.es_solucion(original, solucion) if (args.verificar and resuelto) else None
    return nombre, resuelto, segundos, cantidad, numero_grupo, verificado


class Repartidor:
    """Estado del rango 0: cola de IDs, resultados y líderes por despedir."""

    def __init__(self, world, seleccionados, lideres):
        self.world = world
        self.cola = list(seleccionados)
        self.siguiente = 0
        self.resultados = {}
        self.lideres_activos = lideres
        self.estado = MPI.Status()

    def tomar(self):
        if self.siguiente >= len(self.cola):
            return None
        tarea = self.cola[self.siguiente]
        self.siguiente += 1
        return tarea

    def registrar(self, resultado):
        nombre, resuelto, segundos, cantidad, numero_grupo, verificado = resultado
        self.resultados[nombre] = resultado
        print(f"\nID: {nombre}")
        print("Resuelto" if resuelto else "Sin solución")
        print(f"Grupo: {numero_grupo}; ramas: {cantidad}")
        if verificado is not None:
            print("Verificación:", "correcta" if verificado else "INCORRECTA")
        print(f"Tiempo parcial: {segundos:.6f} segundos", flush=True)

    def atender(self):
        """Responde todos los pedidos de ID pendientes (no bloquea)."""
        while self.world.iprobe(source=MPI.ANY_SOURCE, tag=TAG_ID, status=self.estado):
            origen = self.estado.Get_source()
            resultado = self.world.recv(source=origen, tag=TAG_ID)
            if resultado is not None:
                self.registrar(resultado)
            tarea = self.tomar()
            self.world.send(tarea, dest=origen, tag=TAG_ID)
            if tarea is None:
                self.lideres_activos -= 1


def main():
    parser = argparse.ArgumentParser(description="Sudoku MPI híbrido: IDs entre grupos, ramas dentro del grupo.")
    parser.add_argument("--archivo", type=Path, default=RAIZ / "seeds.txt",
                        help="archivo de seeds (por defecto: ../seeds.txt)")
    seleccion = parser.add_mutually_exclusive_group()
    seleccion.add_argument("--id", "--seed", dest="id", help="resolver solo la seed con este ID")
    seleccion.add_argument("--all", action="store_true", help="resolver todos los seeds")
    parser.add_argument("--tam-grupo", type=int, default=2,
                        help="procesos por grupo S (por defecto: %(default)s)")
    parser.add_argument("--factor", type=int, default=ramas.FACTOR,
                        help="ramas mínimas por proceso del grupo (por defecto: %(default)s)")
    parser.add_argument("--cada", type=int, default=ramas.CADA,
                        help="nodos entre revisiones del buzón (por defecto: %(default)s)")
    parser.add_argument("--verificar", action="store_true",
                        help="comprobar cada solución encontrada")
    args = parser.parse_args()

    world = MPI.COMM_WORLD
    if args.tam_grupo < 1:
        if world.rank == 0:
            print("--tam-grupo debe ser al menos 1", file=sys.stderr)
        sys.exit(2)
    tam = min(args.tam_grupo, world.size)
    cantidad_grupos = world.size // tam
    numero_grupo = min(world.rank // tam, cantidad_grupos - 1)
    grupo = world.Split(color=numero_grupo, key=world.rank)
    args.gancho = None

    if grupo.rank != 0:                          # miembro: solo explora ramas
        ramas.trabajador(grupo, args.cada)
        return

    if world.rank != 0:                          # líder de un grupo != 0
        resultado = None
        while True:
            world.send(resultado, dest=0, tag=TAG_ID)
            estado = MPI.Status()
            while not world.iprobe(source=0, tag=TAG_ID, status=estado):
                time.sleep(ramas.ESPERA)
            tarea = world.recv(source=0, tag=TAG_ID)
            if tarea is None:
                ramas.despedir(grupo)
                return
            resultado = resolver_id(grupo, *tarea, args, numero_grupo)

    # ---- rango 0: repartidor de IDs y líder del grupo 0 ----
    try:
        seeds = leer_seeds(args.archivo)
    except OSError as error:
        print(f"no se pudo leer {args.archivo}: {error}", file=sys.stderr)
        world.Abort(2)
    if args.id:
        if args.id not in seeds:
            print(f"ID '{args.id}' no existe. Disponibles: {', '.join(seeds)}", file=sys.stderr)
            world.Abort(2)
        seleccionados = [(args.id, seeds[args.id])]
    else:
        seleccionados = list(seeds.items())

    tamanos = [tam] * cantidad_grupos
    tamanos[-1] += world.size - tam * cantidad_grupos
    print(f"Procesos MPI: {world.size}; grupos: {cantidad_grupos} "
          f"(tamaños {tamanos}); tableros: {len(seleccionados)}", flush=True)

    repartidor = Repartidor(world, seleccionados, cantidad_grupos - 1)
    args.gancho = repartidor.atender
    inicio = time.perf_counter()
    repartidor.atender()                         # entrega los primeros IDs
    while True:
        tarea = repartidor.tomar()
        if tarea is None:
            break
        repartidor.registrar(resolver_id(grupo, *tarea, args, 0))
        repartidor.atender()
    ramas.despedir(grupo)
    while repartidor.lideres_activos:            # espera los últimos resultados
        repartidor.atender()
        time.sleep(ramas.ESPERA)
    total = time.perf_counter() - inicio

    resultados = repartidor.resultados.values()
    resueltos = sum(1 for r in resultados if r[1])
    print(f"\nIDs procesados: {len(repartidor.resultados)}/{len(seleccionados)} (resueltos: {resueltos})")
    print(f"Tiempo total: {total:.6f} segundos")


if __name__ == "__main__":
    main()
