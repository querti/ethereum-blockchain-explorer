#!/bin/bash
SIZE=10000
touch errlog
while true
do
    python3 main.py --interface /home/ethereum/geth.ipc --dbpath /home/database --gather_tokens --bulk_size $SIZE --datapath /home/data3 2> errlog
    if [ "$?" == 1 ]
    then
        ERROR=`cat errlog`
        echo "$ERROR"
        if [[ $ERROR == *"alloc"* ]]; then
            SIZE=$(( SIZE / 2 ))
            echo "Decreasing batch size to $SIZE"
        fi
    fi
    
    if [ "$SIZE" == 0 ]
    then
        echo "Fatal error. Batch size reduced to 0." >> errlog
        break
    fi
done