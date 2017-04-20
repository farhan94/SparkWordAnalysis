docker run -it \
   --name spark \
   -p 8080:8080 \
   -p 8042:8042 \
   -p 4040:4040 \
   -v "$(pwd):/oh_theese_files" \
   michaelmior/spark:2.0.0 /etc/bootstrap.sh bash
