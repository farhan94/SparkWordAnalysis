docker run -it \
  --name hadoop \
  -p 50010:50010 \
  -p 50020:50020 \
  -p 50070:50070 \
  -p 50075:50075 \
  -p 50090:50090 \
  -p 8020:8020 \
  -p 8088:8088 \
  -p 9000:9000 \
  -v "$(pwd):/oh_theese_files" \
  sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash
