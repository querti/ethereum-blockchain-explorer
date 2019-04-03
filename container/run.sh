#!/bin/bash
SIZE=5000
sleep 50000000
while true
do
    python3 main.py --interface /media/querti/Windows8_OS/Downloads/ethereum/geth.ipc --dbpath /media/querti/Maxtor/database/ --gather_tokens True --bulk_size $SIZE
    if [ "$?" == 1 ]
    then
        SIZE=$(( SIZE / 2 ))
        echo "Decreasing batch size to $SIZE"
    fi
    
    if [ "$SIZE" == 0 ]
    then
        echo "Fatal error"
        break
    fi
done