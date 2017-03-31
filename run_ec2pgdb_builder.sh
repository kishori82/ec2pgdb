#!/bin/bash 

#sudo mv /filename /etc/init.d/
#sudo chmod +x /etc/init.d/filename 
#sudo update-rc.d filename defaults 

python /home/ubuntu/ec2pgdb/ec2pgdb_builder.py  --role-type worker --worker_dir /home/ubuntu/worker_dir/ --key /home/ubuntu/.ssh/kishori.csail.csv --readyqueue ready_large
