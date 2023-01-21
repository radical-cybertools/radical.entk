#!/bin/sh

export RADICAL_REPORT=OFF
export RADICAL_LOG_LVL=OFF

for n in 1 2 4 8 16 32 64 128
do
    t=$(./scale.py $n $n $n)
    printf '%4d %s\n' $n $t
done

