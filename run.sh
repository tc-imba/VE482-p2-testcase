#!/usr/bin/env bash

for i in {01..12}; do
    echo p2/pgroup-${i}
#    python3 test.py -p p2/pgroup-${i} --times=10
    python3 report.py -p p2/pgroup-${i} -t ${i}
    cp p2/pgroup-${i}/team${i}_report.pdf reports
done