#! /bin/bash -xe

class=$1
jar=$2

time $SPARK_HOME/bin/spark-submit \
    --class $class \
    --master local[4] \
    $jar file:///oh_theese_files/input/book/ file:///results