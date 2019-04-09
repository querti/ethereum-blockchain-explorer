#!/bin/bash
SIZE=10000
cd /home/ethereum-blockchain-explorer
while true
do
    python3 main.py --interface /home/ethereum/geth.ipc --dbpath /home/db --gather_tokens True --bulk_size $SIZE
    if [ "$?" == 1 ]
    then
        SIZE=$(( SIZE / 2 ))
        echo "Decreasing batch size to $SIZE"
    fi
    
    if [ "$SIZE" == 0 ]
    then
        echo "Fatal error" >> /home/data/errlog
        break
    fi
done