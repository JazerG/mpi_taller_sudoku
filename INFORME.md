# Informe de resultados

Mediciones hechas con el mismo archivo `seeds.txt` (30 IDs), en el mismo equipo y con la misma instalación de Open MPI para las cuatro versiones.

## Cómo se obtuvieron los datos

1. Calibración de `N` (una vez, en el equipo del informe):

   ```
   docker run --rm -v "${PWD}:/app" augustosalazar/slim-mpi:2 python /app/detectar_procesos_mpi.py
   ```

2. Todas las mediciones (secuencial, las tres versiones MPI y cada tamaño de grupo del híbrido; 3 repeticiones y mediana de cada una), con el `N` del paso 1:

   ```
   docker run --rm -v "${PWD}:/app" augustosalazar/slim-mpi:2 python /app/medir.py --n 2
   ```

   El script ejecuta cada programa con `mpiexec --allow-run-as-root --use-hwthread-cpus --oversubscribe -n N`, lee el `Tiempo total` que imprime, comprueba que se procesaron los 30 IDs y escribe la tabla en `resultados.md` y los datos en `resultados.json`.

Criterio de tiempo:

- **Secuencial:** suma de los tiempos de búsqueda (`time.perf_counter()`), como en el baseline.
- **MPI:** tiempo real en el rango 0, desde que empieza el reparto hasta que llega el último resultado. No se suman los tiempos de los procesos.

## Entorno

- Equipo: Intel Xeon @ 2,10 GHz, 2 hilos de hardware (`nproc = 2`), 7,8 GiB de RAM, Ubuntu 24.04 (Linux 6.18)
- MPI: Open MPI 5.0.11 + mpi4py 4.1.2, Python 3.13 (mismo entorno que `augustosalazar/slim-mpi:2`; los comandos de arriba son los del contenedor)
- Cantidad de repeticiones por medición: 3
- Valor común de `N` elegido: **2**

Salida de la calibración (`detectar_procesos_mpi.py`, conteo de primos hasta 2 000 000):

| N | Mediana (s) |
| ---: | ---: |
| 1 | 4,647 |
| 2 | 2,928 |

`N RECOMENDADO: 2`. Ese mismo `N = 2` se usó en las tres implementaciones MPI.

## Comparación final con el mismo `N`

| Implementación | Caso | Procesos | Mediana del tiempo total (s) | Aceleración frente a secuencial |
| --- | --- | ---: | ---: | ---: |
| Secuencial | Todos los IDs | 1 | 49,722 | 1,00 |
| MPI por tableros | Todos los IDs | 2 | 26,490 | 1,88 |
| MPI por ramas | Todos los IDs, uno después de otro | 2 | 24,930 | 1,99 |
| MPI híbrido (1 grupo de 2) | Todos los IDs | 2 | 24,456 | 2,03 |

Aceleración = `tiempo secuencial / tiempo MPI`. Las cuatro versiones procesaron y resolvieron los 30 IDs en todas las ejecuciones.

Mediciones individuales (s):

- Secuencial: 49,687 · 49,871 · 49,722 → mediana 49,722
- MPI por tableros: 26,440 · 26,575 · 26,490 → mediana 26,490
- MPI por ramas: 26,200 · 24,930 · 24,800 → mediana 24,930
- MPI híbrido, grupos de 1: 25,565 · 25,637 · 26,001 → mediana 25,637
- MPI híbrido, grupos de 2: 24,456 · 24,399 · 24,508 → mediana 24,456

### Tamaño de grupo del híbrido

Con `N = 2` los divisores posibles son 1 y 2, es decir, los dos casos extremos.

| Tamaño de grupo S | Grupos | Mediana (s) | Aceleración |
| ---: | ---: | ---: | ---: |
| 1 (= reparto de tableros) | 2 | 25,637 | 1,94 |
| 2 (= reparto de ramas) | 1 | 24,456 | 2,03 |

Se eligió el S con menor mediana: **S = 2**.

## Conclusiones

**Estrategia más rápida.** El híbrido con S = 2 tuvo el menor tiempo: 24,46 s frente a 49,72 s del secuencial, una aceleración de 2,03 con 2 procesos (25,27 s menos, −51 %). Le siguieron por ramas (24,93 s, ×1,99) y por tableros (26,49 s, ×1,88). La diferencia entre las tres es pequeña (2,0 s entre la mejor y la peor). Con N = 2 el techo ideal es ×2, y las tres versiones quedan cerca de él. Como con N = 2 el único S que no es reparto de tableros es S = N, el híbrido elegido funciona igual que el reparto de ramas. Su ventaja de 0,5 s sobre `mpi_por_ramas` se explica sobre todo por la variación entre ejecuciones (la primera medición de ramas dio 26,20 s y las otras dos, 24,9 s), así que no muestra una diferencia de diseño.

**Comunicación.** Pesa poco en los dos repartos. En el reparto de tableros hay solo 2 mensajes por ID. El rango 0 también resuelve y, al sondear con `iprobe` y pausas cortas, casi no le quita CPU al cálculo. En el de ramas hay unas 10–22 ramas por tablero y revisiones del buzón cada 300 nodos. Aun así, ningún tablero tardó más que en serie (aceleración por tablero mínima ×1,50 en el ID 11), así que el costo de generar y enviar ramas y de cancelar fue menor que lo ganado.

**Diferencias de duración entre tableros.** En serie los tableros tardan entre 0,88 y 3,25 s en este equipo (media 1,68 s). El reparto de tableros usa planificación dinámica, de modo que el proceso que se libera toma el siguiente ID. Aun así, al final puede quedar un proceso con el último tablero mientras el otro está ocioso. Ese desbalance de cola, junto con que la versión por tableros no tiene "atajos" (cada ID cuesta lo mismo que en serie: suma de búsquedas 50,1 s ≈ secuencial), explica su ×1,88, por debajo del ideal. Con 30 tareas y solo 2 procesos el desbalance es pequeño. Con un N mayor, el último tablero largo pesaría proporcionalmente más.

**Tableros repartidos frente a ramas de cada tablero.** El reparto de ramas, aunque procesa los tableros uno tras otro, fue ~1,6 s más rápido que repartir tableros. La causa es la búsqueda especulativa: el serial siempre explora la primera rama completa antes de pasar a la siguiente. Al repartirlas, otro proceso puede empezar directamente por la rama que contiene la solución. En 10 de los 30 tableros la aceleración superó ×2 (superlineal). El caso extremo es el ID 6: 1,696 s en serie y 0,026 s con ramas (×64), porque la solución estaba en una rama que se entregó al proceso 1 desde el inicio. En cambio, en los tableros cuya solución está en las primeras ramas, el segundo proceso trabaja en ramas que no llevan a nada y la aceleración baja a ×1,5. En promedio esos dos efectos se compensan y dejan al reparto de ramas apenas por encima del de tableros. Repartir tableros es más simple y más predecible: su tiempo depende solo de la carga, no de dónde esté la solución. El reparto de ramas tiene más variación y depende del orden en que se generan las ramas.

**Limitación.** Estas cifras corresponden a un equipo de 2 hilos, por lo que N = 2 y el híbrido solo pudo probar sus casos extremos. En un equipo con más núcleos (N = 4, 8, …) `medir.py` prueba también los tamaños de grupo intermedios, donde se espera que el híbrido sí se diferencie: muchos grupos aprovechan el reparto de IDs, y los grupos de 2 o más evitan que haya procesos ociosos al final de la ejecución.
