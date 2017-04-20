#! /bin/bash -xe

class=$1

mkdir -p build
javac -classpath `$HADOOP_PREFIX/bin/hadoop classpath` -d build $class.java
jar -cvf $class.jar -C build/ .

$HADOOP_PREFIX/bin/hdfs dfs -rm -r output || echo "No Output"
rm -rf output

$HADOOP_PREFIX/bin/hadoop jar $class.jar $class input output
$HADOOP_PREFIX/bin/hdfs dfs -get output output

cat output/part-r-*
