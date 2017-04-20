#! /bin/bash -xe

cd ../
mvn package
cp -r target docker/

cd docker/

docker stop spark
docker rm spark
./start_spark.sh

echo done
