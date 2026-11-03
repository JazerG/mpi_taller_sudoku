"""Mide la versión secuencial y las tres versiones MPI con el mismo N.

Ejecuta cada caso --all varias veces (3 por defecto), toma la mediana del
"Tiempo total" que imprime cada programa y calcula la aceleración frente a la
versión secuencial. Para el híbrido prueba varios tamaños de grupo y usa el
más rápido en la tabla final. Guarda todo en resultados.md y resultados.json.

Uso dentro del contenedor (desde la carpeta del proyecto):
  docker run --rm -v "${PWD}:/app" augustosalazar/slim-mpi:2 python /app/medir.py --n 8
"""

import argparse
import json
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time

RAIZ = Path(__file__).resolve().parent
MPI = ["mpiexec", "--allow-run-as-root", "--use-hwthread-cpus", "--oversubscribe"]
PATRON_TOTAL = re.compile(r"Tiempo total: ([0-9.]+) segundos")
PATRON_PROC = re.compile(r"IDs procesados: (\d+)/(\d+)")


def ejecutar(comando, etiqueta):
    inicio = time.perf_counter()
    salida = subprocess.run(comando, capture_output=True, text=True, cwd=RAIZ)
    pared = time.perf_counter() - inicio
    if salida.returncode != 0:
        print(salida.stdout[-2000:], salida.stderr[-2000:], sep="\n")
        sys.exit(f"Falló: {etiqueta}")
    total = PATRON_TOTAL.findall(salida.stdout)
    if not total:
        sys.exit(f"No se encontró 'Tiempo total' en la salida de {etiqueta}")
    resueltos = salida.stdout.count("\nResuelto")
    procesados = PATRON_PROC.search(salida.stdout)
    if procesados and procesados.group(1) != procesados.group(2):
        sys.exit(f"{etiqueta}: solo se procesaron {procesados.group(0)}")
    return float(total[-1]), resueltos, pared


def medir(nombre, comando, repeticiones):
    tiempos = []
    for numero in range(1, repeticiones + 1):
        total, resueltos, pared = ejecutar(comando, nombre)
        tiempos.append(total)
        print(f"{nombre}, medición {numero}: {total:.3f} s "
              f"(resueltos: {resueltos}; proceso completo: {pared:.1f} s)", flush=True)
    mediana = statistics.median(tiempos)
    print(f"{nombre}: mediana {mediana:.3f} s\n", flush=True)
    return {"tiempos": tiempos, "mediana": mediana, "resueltos": resueltos}


def coma(valor, decimales=2):
    return f"{valor:.{decimales}f}".replace(".", ",")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--n", type=int, required=True, help="N elegido con detectar_procesos_mpi.py")
    parser.add_argument("--repeticiones", type=int, default=3)
    parser.add_argument("--grupos", default=None,
                        help="tamaños de grupo del híbrido separados por coma "
                             "(por defecto: divisores de N entre 2 y N/2, más 1 y N)")
    parser.add_argument("--archivo", default=str(RAIZ / "seeds.txt"))
    parser.add_argument("--sin-secuencial", type=float, default=None, metavar="SEGUNDOS",
                        help="no medir la versión secuencial y usar esta mediana")
    args = parser.parse_args()

    n = args.n
    if args.grupos:
        tamanos = sorted({int(x) for x in args.grupos.split(",")})
    else:
        tamanos = [s for s in range(1, n + 1) if n % s == 0]
    py = sys.executable
    mpi = MPI + ["-n", str(n), py]
    resultados = {"N": n, "repeticiones": args.repeticiones}

    if args.sin_secuencial is None:
        resultados["secuencial"] = medir(
            "Secuencial", [py, "baseline_secuencial.py", "--all", "--archivo", args.archivo],
            args.repeticiones)
    else:
        resultados["secuencial"] = {"tiempos": [], "mediana": args.sin_secuencial}

    resultados["tableros"] = medir(
        f"MPI por tableros (N={n})",
        mpi + ["mpi_por_tableros/mpi_por_tableros.py", "--all", "--archivo", args.archivo],
        args.repeticiones)
    resultados["ramas"] = medir(
        f"MPI por ramas (N={n})",
        mpi + ["mpi_por_ramas/mpi_por_ramas.py", "--all", "--archivo", args.archivo],
        args.repeticiones)
    resultados["hibrido_por_grupo"] = {}
    for s in tamanos:
        resultados["hibrido_por_grupo"][s] = medir(
            f"MPI híbrido (N={n}, grupos de {s})",
            mpi + ["mpi_hibrido/mpi_hibrido.py", "--all", "--tam-grupo", str(s),
                   "--archivo", args.archivo],
            args.repeticiones)
    mejor_s = min(tamanos, key=lambda s: resultados["hibrido_por_grupo"][s]["mediana"])
    resultados["hibrido"] = dict(resultados["hibrido_por_grupo"][mejor_s], tam_grupo=mejor_s)

    base = resultados["secuencial"]["mediana"]
    filas = [
        ("Secuencial", "Todos los IDs", "1", base),
        ("MPI por tableros", "Todos los IDs", str(n), resultados["tableros"]["mediana"]),
        ("MPI por ramas", "Todos los IDs, uno después de otro", str(n), resultados["ramas"]["mediana"]),
        (f"MPI híbrido ({n // mejor_s} grupos de {mejor_s})", "Todos los IDs", str(n),
         resultados["hibrido"]["mediana"]),
    ]
    lineas = ["| Implementación | Caso | Procesos | Mediana del tiempo total (s) | Aceleración frente a secuencial |",
              "| --- | --- | ---: | ---: | ---: |"]
    for nombre, caso, procesos, mediana in filas:
        lineas.append(f"| {nombre} | {caso} | {procesos} | {coma(mediana, 3)} | {coma(base / mediana)} |")
    lineas += ["", "Mediciones individuales (s):", ""]
    detalle = [("Secuencial", resultados["secuencial"]), ("MPI por tableros", resultados["tableros"]),
               ("MPI por ramas", resultados["ramas"])]
    detalle += [(f"MPI híbrido, grupos de {s}", resultados["hibrido_por_grupo"][s]) for s in tamanos]
    for nombre, datos in detalle:
        valores = ", ".join(coma(t, 3) for t in datos["tiempos"]) or "(no medido)"
        lineas.append(f"- {nombre}: {valores} → mediana {coma(datos['mediana'], 3)}")
    tabla = "\n".join(lineas)

    (RAIZ / "resultados.md").write_text(tabla + "\n", encoding="utf-8")
    (RAIZ / "resultados.json").write_text(json.dumps(resultados, indent=2), encoding="utf-8")
    print(tabla)
    print("\nGuardado en resultados.md y resultados.json")


if __name__ == "__main__":
    main()
