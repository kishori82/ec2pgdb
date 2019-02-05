
run_ec2pgdb_builder.sh
# this script takes input from bucket 
   pgdbinput1
   pgdboutput1
   ready_large 


run_ec2pgdb_extractor.sh
# extracts the epgds
   ready_extract
   running_extract
   complete_extract

run_worker.sh
