#!/bin/bash

start=$SECONDS

overheadanalysis="spark-submit --class dbest.OverheadAnalysis --jars $(echo ./lib/*.jar | tr ' ' ',') target/scala-2.11/dbest.jar data/sf10/store_sales.dat"
sensitivsamplesize="spark-submit --class dbest.SensitivityAnalysisSampleSizeEffect --jars $(echo ./lib/*.jar | tr ' ' ',') target/scala-2.11/dbest.jar data/sf10/store_sales.dat"
sensitiverangeeff="spark-submit --class dbest.SensitivityAnalysisQueryRangeEffect --jars $(echo ./lib/*.jar | tr ' ' ',') target/scala-2.11/dbest.jar data/sf10/store_sales.dat"

echo "Starting spark jobs..."
for i in "$@"; do
   if [ "$i" == "1" ]; then
      echo "Starting dbest.OverheadAnalysis..."
      eval "$overheadanalysis"
      echo "dbest.OverheadAnalysis finished"
   fi
   if [ "$i" == "2" ]; then
      echo "Starting dbest.SensitivityAnalysisSampleSizeEffect..."
      eval "$sensitivsamplesize"
      echo "dbest.SensitivityAnalysisSampleSizeEffect finished"
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
