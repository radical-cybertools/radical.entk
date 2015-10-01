#!/bin/sh

i=0
ORIG="`pwd`"
LEN=64

rm $ORIG/input.gro

while ! test $i = $LEN
do 
	i=$((i+1))
	cat $ORIG/reg.gro | sed "1"','"25"'!d' >> input.gro
done