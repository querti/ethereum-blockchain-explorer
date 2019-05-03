#!/bin/bash
SIZE=10000
cd /home/ethereum-blockchain-explorer
while true
do
    python3 main.py --interface /home/ethereum/geth.ipc --dbpath /home/db --gather_tokens True --bulk_size $SIZE --datapath /home/data 2>/home/data/errlog
    if [ "$?" == 1 ]
    then
        ERROR=`cat /home/data/errlog`
        echo "$ERROR"
        if [[ $ERROR == *"memory"* ]]; then
            SIZE=$(( SIZE / 2 ))
            echo "Decreasing batch size to $SIZE"
        fi
    fi
    
    if [ "$SIZE" == 0 ]
    then
        echo "Fatal error" >> /home/data/errlog
        break
    fi
done