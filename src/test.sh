#!/bin/bash
a=0
while [ $a -lt 11 ]
do
    python3 client.py $a
    let a++
done