#!/bin/bash
source MetaPathwaysrc

#X=$1

PLATOFRM=ubuntu
for X in `cat $1`
do
      echo "SAMPLE "${X} "==================>"
      Y=`echo ${X} | sed -e 's/.*/\L&/g'`
      
      #download sample pgdb extract  input
      aws s3 cp s3://pgdbextractinput/${X}.tar.gz /home/${PLATFORM}/ec2pgdb/pwy_extract/input/
      tar -zxvf /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${X}.tar.gz --directory /home/${PLATFORM}/ec2pgdb/pwy_extract/input/
      
      #download  pgdb cyc  input
      echo "downloading ${Y}cyc.tar.gz"
      aws s3 cp s3://pgdboutput1/${Y}cyc.tar.gz /home/${PLATFORM}/ec2pgdb/pwy_extract/input/
      
      echo "extracting ${Y}cyc.tar.gz to input/${Y}"
      tar -zxvf /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${Y}cyc.tar.gz  --directory  ~/ptools-local/pgdbs/user/
      
      echo "moving  ~/ptools-local/pgdbs/user/${Y} ~/ptools-local/pgdbs/user/${Y}cyc"
      mv  ~/ptools-local/pgdbs/user/${Y} ~/ptools-local/pgdbs/user/${Y}cyc
      
      
      echo python libs/python_scripts/MetaPathways_run_pathologic.py --reactions /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${X}.metacyc.orf.annots.txt --ptoolsExec /home/${PLATFORM}/pathway-tools/pathway-tools -i /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${X}/ptools/ -p /home/${PLATFORM}/ptools-local/pgdbs/user/${Y}cyc -s ${X}  --wtd --annotation-table /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${X}/${X}.functional_and_taxonomic_table.txt  --ncbi-tree /home/${PLATFORM}/ec2pgdb/pwy_extract/resouces_files/ncbi_taxonomy_tree.txt --ncbi-megan-map /home/${PLATFORM}/ec2pgdb/pwy_extract/resouces_files/ncbi.map --output-pwy-table /home/${PLATFORM}/ec2pgdb/pwy_extract/output/${Y}.pwy.txt
      echo ""

      python libs/python_scripts/MetaPathways_run_pathologic.py --reactions /home/${PLATFORM}/ec2pgdb/pwy_extract/output/${X}.metacyc.orf.annots.txt --ptoolsExec /home/${PLATFORM}/pathway-tools/pathway-tools -i /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${X}/ptools/ -p /home/${PLATFORM}/ptools-local/pgdbs/user/${Y}cyc -s ${X}  --wtd --annotation-table /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${X}/${X}.functional_and_taxonomic_table.txt  --ncbi-tree /home/${PLATFORM}/ec2pgdb/pwy_extract/resouces_files/ncbi_taxonomy_tree.txt --ncbi-megan-map /home/${PLATFORM}/ec2pgdb/pwy_extract/resouces_files/ncbi.map --output-pwy-table /home/${PLATFORM}/ec2pgdb/pwy_extract/output/${Y}.pwy.txt
      
      aws s3 cp /home/${PLATFORM}/ec2pgdb/pwy_extract/output/${Y}.pwy.txt  s3://pgdbextractoutput/${Y}.pwy.txt
      aws s3 cp /home/${PLATFORM}/ec2pgdb/pwy_extract/output/${X}.metacyc.orf.annots.txt s3://pgdbextractoutput/${X}.metacyc.orf.annots.txt 
      
      rm  /home/${PLATFORM}/ec2pgdb/pwy_extract/output/${Y}.pwy.txt
      rm  /home/${PLATFORM}/ec2pgdb/pwy_extract/output/${X}.metacyc.orf.annots.txt 

      rm  /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${X}.tar.gz
      rm -rf /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${X}
      rm -rf /home/${PLATFORM}/ec2pgdb/pwy_extract/input/${Y}cyc.tar.gz 
      rm -rf  ~/ptools-local/pgdbs/user/${Y}cyc

done;
