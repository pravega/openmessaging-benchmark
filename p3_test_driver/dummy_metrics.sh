#!/bin/bash
while [ 1 ]
do
  echo -n dummy_metrics: $1 $2: ; date -u
  sleep $1
done

