aws ec2 describe-instances --output text |  grep INSTANCES | awk '{print $14}'
