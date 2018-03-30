from __future__ import print_function
import sys, os, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: pair-partition.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Pair partition').getOrCreate()

    spark.read.load(sys.argv[1])\
        .withColumn('s1', regexp_replace('s1', '\n', ' '))\
        .withColumn('s2', regexp_replace('s2', '\n', ' '))\
        .repartition('series1', 'series2')\
        .sortWithinPartitions('id1', 'id2')\
        .write\
        .partitionBy('series1', 'series2')\
        .format('csv')\
        .options(header='true', sep='\t', compression='gzip')\
        .save(sys.argv[2])

    ## Put filenames in desired format.
    for root, dirs, files in os.walk(sys.argv[2], topdown=False):
        if '/series2=' in root:
            rfiles = filter(lambda s: not s.startswith('.'), files)
            if ( len(rfiles) > 0 ) and ( rfiles[0].startswith('part') ):
                os.rename(os.path.join(root, rfiles[0]), re.sub('series2=', '', root) + '.csv.gz')
            for dotfile in filter(lambda s: s.startswith('.'), files):
                os.remove(os.path.join(root, dotfile))
            os.rmdir(root)
        elif '/series1=' in root:
            os.rename(root, re.sub('series1=', '', root))

    spark.stop()
    
