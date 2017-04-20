# $HADOOP_PREFIX/bin/hdfs dfs -rm -r input
$HADOOP_PREFIX/bin/hdfs dfs -rm -r input
$HADOOP_PREFIX/bin/hdfs dfs -mkdir input
$HADOOP_PREFIX/bin/hdfs dfs -put input/book/* input
