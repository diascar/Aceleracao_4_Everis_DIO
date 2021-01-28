#!/usr/bin/bash

#$1 arquivo com as row-keys
#$2 comando hbase

while read -r line;
do 
    echo "$2 'cidades', '$line' $3"; 
done < $1 | hbase shell