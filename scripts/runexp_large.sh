#!/bin/bash

start=$SECONDS

dataset="data/sf100/store_sales.dat"

current_dir="$(pwd)/"
for last; do true; done
if [ "$last" == "1" ] || [ "$last" == "2" ] || [ "$last" == "3" ]; then
   last=""
fi
sample_size_eff_base="${last}"


overheadanalysis="spark-submit \
   --class dbest.OverheadAnalysis \
   --jars $(echo ./lib/*.jar | tr ' ' ',') \
   ${current_dir}target/scala-2.11/dbest.jar \
   ${dataset}"

sensitivsamplesize="spark-submit \
   --class dbest.SensitivityAnalysisSampleSizeEffect${sample_size_eff_base} \
   --jars $(echo ./lib/*.jar | tr ' ' ',') \
   ${current_dir}target/scala-2.11/dbest.jar \
   ${dataset}"

sensitiverangeeff="spark-submit \
   --class dbest.SensitivityAnalysisQueryRangeEffect \
   --jars $(echo ./lib/*.jar | tr ' ' ',') \
   ${current_dir}target/scala-2.11/dbest.jar \
   ${dataset}"

echo "$current_dir"
echo "Starting spark jobs..."
for i in "${@:1:$#}"; do
   if [ "$i" == "1" ]; then
      echo "Starting dbest.OverheadAnalysis..."
      eval "$overheadanalysis"
      echo "dbest.OverheadAnalysis finished"
   fi
   if [ "$i" == "2" ]; then
      echo "Starting dbest.SensitivityAnalysisSampleSizeEffect${sample_size_eff_base}..."
      eval "$sensitivsamplesize"
      echo "dbest.SensitivityAnalysisSampleSizeEffect${sample_size_eff_base} finished"
   fi
   if [ "$i" == "3" ]; then
      echo "Starting dbest.SensitivityAnalysisQueryRangeEffect..."  
      eval "$sensitiverangeeff"
      echo "dbest.SensitivityAnalysisQueryRangeEffect finished"
   fi
   ret_code=$?
   if [ $ret_code -ne 0 ]; then 
      exit $ret_code
   fi
done
echo "Spark jobs finished."

echo ""Experimental Analysis duration: "$(( SECONDS - start  )) seconds"