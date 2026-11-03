| Implementación | Caso | Procesos | Mediana del tiempo total (s) | Aceleración frente a secuencial |
| --- | --- | ---: | ---: | ---: |
| Secuencial | Todos los IDs | 1 | 49,722 | 1,00 |
| MPI por tableros | Todos los IDs | 2 | 26,490 | 1,88 |
| MPI por ramas | Todos los IDs, uno después de otro | 2 | 24,930 | 1,99 |
| MPI híbrido (1 grupos de 2) | Todos los IDs | 2 | 24,456 | 2,03 |

Mediciones individuales (s):

- Secuencial: 49,687, 49,871, 49,722 → mediana 49,722
- MPI por tableros: 26,440, 26,575, 26,490 → mediana 26,490
- MPI por ramas: 26,200, 24,930, 24,800 → mediana 24,930
- MPI híbrido, grupos de 1: 25,565, 25,637, 26,001 → mediana 25,637
- MPI híbrido, grupos de 2: 24,456, 24,399, 24,508 → mediana 24,456
