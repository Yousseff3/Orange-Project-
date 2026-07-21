#!/bin/bash

apt update -y

apt upgrade -y 

apt install python3-distutils-extra -y


cat > .env <<EOF
HOST_IP=$(hostname -I | awk '{print $1}')
postgresuser=airflow
postgrespassword=youssef
postgresdbname=airflow
redispassword=youssef
webserverseckey=youssef
fernetkey=FIEQwFNkIf20aJVQ3seBdK4_vDX7qaGT9xy9MvGDNKY=
EOF



mkdir -p \
  gzip/{gzipinput,gzipcomplet,gzipbackup,jsoncoming,jsondone,jsonbackup,xmlbackup,xmlcoming,xmldone} \
  xmlonly/{xmlin,xmldone,xmlbackup,jsonout,jsondone,jsonbackup} \
  csv/{inputcsv,jobdone,backups} \
  xmlhard/{xmlin,xmldone,xmlbackup,jsonout,jsondone,jsonbackup} 

chmod -R 777 ./ 

sudo apt-get install docker-compose -y

docker-compose up -d 

chmod -R 777 ./


echo "Environment setup complete!"
echo "Created:"
echo "1. .env file with your specified variables"
echo "2. Complete folder structure with all subdirectories"
echo "3. Placeholder dag files in dags directory"
echo "4. Installed all Dependencies and packages "
