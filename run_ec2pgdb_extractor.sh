#!/bin/bash 

#sudo mv /filename /etc/init.d/
#sudo chmod +x /etc/init.d/filename 
#sudo update-rc.d filename defaults 

#python /home/ubuntu/ec2pgdb/ec2pgdb_builder.py  --role-type worker --worker_dir /home/ubuntu/worker_dir/ --key /home/ubuntu/.ssh/kishori.csail.csv --readyqueue ready_large

python /home/ubuntu/ec2pgdb/ec2pgdb_builder.py --key ~/.ssh/kishori.konwar.csv --role-type worker  --process extract --readyqueue ready_extract --worker_dir ~/ec2pgdb/ --runningqueue running_extract --completequeue complete_extract
