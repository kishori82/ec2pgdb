#!/bin/bash 

# Warn and exit if something is already listening on port 1555.
if [ `netstat -a | fgrep 1555 | fgrep -c LISTEN` == 1 ]; then
  echo Something is already listening on port 1555.
  echo Please stop whatever it is, and then try $0 again.
  exit
fi
# Start Xvfb as X-server display #1. If it's already been started, no harm done.
# This works in conjunction with the pathway-tools script which needs to have:
#   setenv DISPLAY localhost:1
#Xvfb :1 > & /dev/null &
Xvfb :99 & 
export DISPLAY=:99
# Start GNU screen (hit Ctrl-A Ctrl-D to detach). Within it, start Pathway Tools.
# screen -m -d /home/aic-export/pathway-tools/ptools/[version]/pathway-tools -www -www-publish all  #(for version 10.0 or later)

#sudo mv /filename /etc/init.d/
#sudo chmod +x /etc/init.d/filename 
#sudo update-rc.d filename defaults 

source /home/ubuntu/ec2pgdb/pwy_extract/MetaPathwaysrc

python /home/ubuntu/ec2pgdb/ec2pgdb_builder.py --key /home/ubuntu/.ssh/kishori.konwar.csv --role-type worker  --process extract --readyqueue ready_extract --worker_dir /home/ubuntu/ec2pgdb/pwy_extract/ --runningqueue running_extract --completequeue complete_extract > /tmp/ec2extract.log
