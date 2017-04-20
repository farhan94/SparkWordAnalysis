#! /bin/bash -xe

class=$1
jar=$2

$SPARK_HOME/bin/spark-submit \
    --class $class \
    --master local[4] \
    $jar

