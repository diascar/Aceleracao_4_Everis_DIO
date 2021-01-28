#!/usr/bin/bash

#$1 - numero de registros para retornar
#$2 - arquivo de saída
#$3 - tipo de sort (r para reverso)


tail -n +7 |
head -n -2 |
cut -f 2,5 -d' ' |
cut -f 1-2 -d'=' --output-delimiter=' '|
sort -$3nk3 |
head -n $1 |
cut -f 1 -d ' ' > $2