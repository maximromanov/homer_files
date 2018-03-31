#!/bin/bash

SPARK_SUBMIT_ARGS='--master local[6] --driver-memory 50G --executor-memory 50G --conf spark.local.dir=/home/mromanov/passim_runs/tmp --conf spark.sql.shuffle.partitions=1000' passim --pairwise --output-format parquet --filterpairs 'gid != gid2' --n 4 --min-match 4 input output >& run.err

