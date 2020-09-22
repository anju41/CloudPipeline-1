#!/bin/bash
schema=mydb
tables=(emp dept)

while true
 do
  for t in "${tables[@]}";
  do
  v=`gsutil -q stat gs://shuva-buc/$t/*.parquet; echo $?`
   if [ $v == 0 ]; then
      echo "loading data into $t"
      path="gs://shuva-buc/$t/*.parquet"   
   
      bq load \
	      --source_format=PARQUET \
	      "$schema.$t" \
	      "$path"

      gsutil -q rm gs://shuva-buc/$t/*.parquet;
   
   fi;
  done 
done
