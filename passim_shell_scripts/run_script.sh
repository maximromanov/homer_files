#!/bin/bash

SPARK_SUBMIT_ARGS='--master local[6] --driver-memory 50G --executor-memory 50G --conf spark.local.dir=/home/mromanov/passim_runs/tmp --conf spark.sql.shuffle.partitions=1000' spark-submit $SPARK_SUBMIT_ARGS align-partition.py output/align.parquet output/align-partition >& script.err
