"""Motor de reparto de ramas de UN tablero entre los procesos de un comunicador.

Lo usan mpi_por_ramas.py (comunicador = COMM_WORLD) y ../mpi_hibrido
(comunicador = el de cada grupo).

Protocolo dentro del comunicador (todos los mensajes con la etiqueta TAG):
  coordinador (rango 0) -> trabajador: ("rama", tablero) | ("parar", None)
                                       | ("fin", None) | ("salir", None)
  trabajador -> coordinador:           ("pedir", None) | ("solucion", tablero)

* Al empezar un tablero el coordinador genera las ramas expandiendo las
  primeras casillas vacías (en el mismo orden que el backtracking serial) y
  envía una rama a cada trabajador; las demás se entregan bajo pedido.
* El coordinador también explora ramas. Su búsqueda se interrumpe cada
  `cada` nodos para atender mensajes, así responde pedidos sin un hilo extra.
* Cuando alguien encuentra la solución, el coordinador envía "parar" a los
  trabajadores ocupados; cada trabajador revisa su buzón cada `cada` nodos
  y abandona la rama. Después, a cada "pedir" se responde "fin".
* MPI no adelanta mensajes con el mismo origen, destino y etiqueta, así que
  un "parar" siempre llega antes del "fin" del mismo tablero y nunca se
  mezcla con el tablero siguiente.
"""

from collections import deque
from pathlib import Path
import sys
import time

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from mpi4py import MPI  # noqa: E402

from sudoku import puede_colocar  # noqa: E402

TAG = 11
ESPERA = 0.0002     # pausa entre sondeos cuando un proceso está ocioso
CADA = 300          # nodos explorados entre revisiones del buzón
FACTOR = 4          # ramas mínimas = FACTOR * procesos del comunicador


class Cancelado(Exception):
    pass


def copiar(tablero):
    return [fila[:] for fila in tablero]


def primera_vacia(tablero):
    for fila in range(9):
        for columna in range(9):
            if tablero[fila][columna] == 0:
                return fila, columna
    return None


def generar_ramas(tablero, minimo):
    """Expande niveles completos de las primeras casillas vacías.

    Devuelve (ramas, solucion). Las ramas quedan en el mismo orden en que el
    backtracking serial las visitaría.
    """
    frontera = [copiar(tablero)]
    while len(frontera) < minimo:
        nueva = []
        for actual in frontera:
            posicion = primera_vacia(actual)
            if posicion is None:          # el tablero ya está completo
                return [], actual
            fila, columna = posicion
            for valor in range(1, 10):
                if puede_colocar(actual, fila, columna, valor):
                    hijo = copiar(actual)
                    hijo[fila][columna] = valor
                    nueva.append(hijo)
        frontera = nueva
        if not frontera:                  # ninguna opción válida: sin solución
            break
    return frontera, None


def explorar(tablero, revisar, cada=CADA):
    """Mismo backtracking que sudoku.resolver(), con revisiones periódicas.

    Devuelve True (resuelto in situ), False (rama sin solución) o
    None (cancelada porque revisar() devolvió True).
    """
    contador = 0

    def buscar():
        nonlocal contador
        contador += 1
        if contador >= cada:
            contador = 0
            if revisar():
                raise Cancelado
        for fila in range(9):
            for columna in range(9):
                if tablero[fila][columna] != 0:
                    continue
                for valor in range(1, 10):
                    if puede_colocar(tablero, fila, columna, valor):
                        tablero[fila][columna] = valor
                        if buscar():
                            return True
                        tablero[fila][columna] = 0
                return False
        return True

    try:
        return buscar()
    except Cancelado:
        return None


def resolver_tablero(comm, tablero, factor=FACTOR, cada=CADA, gancho=None):
    """Lo llama el rango 0 de `comm`. Devuelve (resuelto, segundos, ramas, solucion).

    `gancho` es una función opcional que se ejecuta cada vez que el
    coordinador atiende su buzón (el híbrido la usa para repartir IDs).
    """
    inicio = time.perf_counter()
    ramas, solucion = generar_ramas(tablero, max(1, factor * comm.size))
    total_ramas = len(ramas)
    pendientes = deque(ramas)
    ocupados = set()
    fines = 0
    trabajadores = comm.size - 1

    def avisar_solucion(sol, origen):
        nonlocal solucion
        if solucion is None:
            solucion = sol
            pendientes.clear()
            for destino in ocupados:
                if destino != origen:
                    comm.send(("parar", None), dest=destino, tag=TAG)

    # reparto inicial: una rama por trabajador (o "fin" si no alcanzan)
    for destino in range(1, comm.size):
        if solucion is None and pendientes:
            comm.send(("rama", pendientes.popleft()), dest=destino, tag=TAG)
            ocupados.add(destino)
        else:
            comm.send(("fin", None), dest=destino, tag=TAG)
            fines += 1

    estado = MPI.Status()

    def atender():
        nonlocal fines
        if gancho:
            gancho()
        while comm.iprobe(source=MPI.ANY_SOURCE, tag=TAG, status=estado):
            origen = estado.Get_source()
            tipo, dato = comm.recv(source=origen, tag=TAG)
            if tipo == "solucion":
                avisar_solucion(dato, origen)
            else:                                   # "pedir"
                ocupados.discard(origen)
                if solucion is None and pendientes:
                    comm.send(("rama", pendientes.popleft()), dest=origen, tag=TAG)
                    ocupados.add(origen)
                else:
                    comm.send(("fin", None), dest=origen, tag=TAG)
                    fines += 1
        return solucion is not None

    while True:
        if solucion is None and pendientes:
            rama = pendientes.popleft()
            if explorar(rama, atender, cada):
                avisar_solucion(rama, 0)
        elif fines < trabajadores:
            atender()
            if fines < trabajadores:
                time.sleep(ESPERA)
        else:
            break

    return solucion is not None, time.perf_counter() - inicio, total_ramas, solucion


def es_solucion(original, tablero):
    """Comprueba que `tablero` esté completo, sea válido y respete las pistas."""
    if tablero is None:
        return False
    completo = set(range(1, 10))
    for i in range(9):
        if set(tablero[i]) != completo or {tablero[f][i] for f in range(9)} != completo:
            return False
        bf, bc = (i // 3) * 3, (i % 3) * 3
        if {tablero[f][c] for f in range(bf, bf + 3) for c in range(bc, bc + 3)} != completo:
            return False
    return all(original[f][c] in (0, tablero[f][c]) for f in range(9) for c in range(9))


def recibir_de_cero(comm):
    estado = MPI.Status()
    while not comm.iprobe(source=0, tag=TAG, status=estado):
        time.sleep(ESPERA)
    return comm.recv(source=0, tag=TAG)


def trabajador(comm, cada=CADA):
    """Bucle de los rangos != 0 de `comm`; termina al recibir "salir"."""

    def revisar():
        if comm.iprobe(source=0, tag=TAG):
            comm.recv(source=0, tag=TAG)        # solo puede ser "parar"
            return True
        return False

    while True:
        tipo, dato = recibir_de_cero(comm)
        if tipo == "salir":
            return
        if tipo != "rama":                      # "fin" o un "parar" tardío
            continue
        if explorar(dato, revisar, cada):
            comm.send(("solucion", dato), dest=0, tag=TAG)
        comm.send(("pedir", None), dest=0, tag=TAG)


def despedir(comm):
    for destino in range(1, comm.size):
        comm.send(("salir", None), dest=destino, tag=TAG)
