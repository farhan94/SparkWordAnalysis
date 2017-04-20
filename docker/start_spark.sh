docker run -it \
   --name spark \
   -p 8080:8080 \
   -p 8042:8042 \
   -p 4040:4040 \
   -v "$(pwd):/oh_theese_files" \
   sequenceiq/spark:1.6.0 /etc/bootstrap.sh bash
