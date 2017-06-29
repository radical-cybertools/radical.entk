#!/bin/sh

for f in `find . -type f -name "*.py"`
do
    sed -i 's/\t/    /g' $f
done

