#!/bin/bash

start=$SECONDES

overheadanalysis="spark-submit --class dbest.TrainModels --jars $(echo ./lib/*.jar | tr ' ' ',') target/scala-2.11/dbest.jar"

echo "Starting spark models training..."
if [ "$1" == "1" ]; then
    echo "Training on data/store_sales_sample.dat"
     eval "$overheadanalysis data/store_sales_sample.dat" 
elif [ "$1" == "2" ]; then
    echo "Training on data/sf10/store_sales.dat"
    eval "$overheadanalysis data/sf10/store_sales.dat" 
else
    echo "Training on data/sf100/store_sales.dat"
    eval "$overheadanalysis data/sf100/store_sales.dat" 
fi
echo "Spark models training finished!"

echo ""Models training process took: "$(( SECONDS - start  )) seconds"