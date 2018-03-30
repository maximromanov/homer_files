from __future__ import print_function
import sys
from pyspark.sql import SparkSession

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: align-partition.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Align partition').getOrCreate()

    spark.read.load(sys.argv[1])\
        .repartition('series1')\
        .sortWithinPartitions('series2', 'id1', 'id2')\
        .write\
        .partitionBy('series1')\
        .format('csv')\
        .options(header='true', escape='"', compression='gzip')\
        .save(sys.argv[2])

    spark.stop()
    
