#!/bin/bash
source MetaPathwaysrc

#X=$1

for X in `cat $1`
do
      echo "SAMPLE "${X} "==================>"
      Y=`echo ${X} | sed -e 's/.*/\L&/g'`
      
      #download sample pgdb extract  input
      aws s3 cp s3://pgdbextractinput/${X}.tar.gz input/
      tar -zxvf input/${X}.tar.gz --directory input/
      
      #download  pgdb cyc  input
      echo "downloading ${Y}cyc.tar.gz"
      aws s3 cp s3://pgdboutput1/${Y}cyc.tar.gz input/
      
      echo "extracting ${Y}cyc.tar.gz to input/${Y}"
      tar -zxvf input/${Y}cyc.tar.gz  --directory  ~/ptools-local/pgdbs/user/
      
      echo "moving  ~/ptools-local/pgdbs/user/${Y} ~/ptools-local/pgdbs/user/${Y}cyc"
      mv  ~/ptools-local/pgdbs/user/${Y} ~/ptools-local/pgdbs/user/${Y}cyc
      
      
      echo python libs/python_scripts/MetaPathways_run_pathologic.py --reactions /home/ubuntu/pwy_extract/input/${X}.metacyc.orf.annots.txt --ptoolsExec /home/ubuntu/pathway-tools/pathway-tools -i /home/ubuntu/pwy_extract/input/${X}/ptools/ -p /home/ubuntu/ptools-local/pgdbs/user/${Y}cyc -s ${X}  --wtd --annotation-table /home/ubuntu/pwy_extract/input/${X}/${X}.functional_and_taxonomic_table.txt  --ncbi-tree /home/ubuntu/pwy_extract/resouces_files/ncbi_taxonomy_tree.txt --ncbi-megan-map /home/ubuntu/pwy_extract/resouces_files/ncbi.map --output-pwy-table /home/ubuntu/pwy_extract/output/${Y}.pwy.txt
      python libs/python_scripts/MetaPathways_run_pathologic.py --reactions /home/ubuntu/pwy_extract/input/${X}.metacyc.orf.annots.txt --ptoolsExec /home/ubuntu/pathway-tools/pathway-tools -i /home/ubuntu/pwy_extract/input/${X}/ptools/ -p /home/ubuntu/ptools-local/pgdbs/user/${Y}cyc -s ${X}  --wtd --annotation-table /home/ubuntu/pwy_extract/input/${X}/${X}.functional_and_taxonomic_table.txt  --ncbi-tree /home/ubuntu/pwy_extract/resouces_files/ncbi_taxonomy_tree.txt --ncbi-megan-map /home/ubuntu/pwy_extract/resouces_files/ncbi.map --output-pwy-table /home/ubuntu/pwy_extract/output/${Y}.pwy.txt
      
      rm  input/${X}.tar.gz
      rm -rf input/${X}
      rm -rf input/${Y}cyc.tar.gz 
      rm -rf  ~/ptools-local/pgdbs/user/${Y}cyc

done;
