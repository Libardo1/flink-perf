#!/bin/bash

# this script is not as flexible as the other scripts in this directory.
# The overhead of "overgeneralizing" is just too high.
# Author: rmetzger@apache.org

echo "Running fully fledged automated benchmarks"

. ./configDefaults.sh

#export all these variables
set -a

export CORES_PER_MACHINE=16
# for adding new experiments, you also have to edit the loop in runExperiments()
#FLINK_EXPERIMENTS="flink-wordcount-wo-combine flink-wordcount"
FLINK_EXPERIMENTS=""
SPARK_EXPERIMENTS="spark-wordcount-wo-combine spark-wordcount"

# Preparation
if [[ ! -e "_config-staging" ]]; then
	mkdir "_config-staging";
fi

benchId="fullbench-"`date +"%d-%m-%y-%H-%M"`
export LOG_dir="benchmarks/"$benchId
mkdir -p "${LOG_dir}"
LOG="$LOG_dir/control"
export TIMES="${LOG_dir}/times"
touch $TIMES

# set output for this file (http://stackoverflow.com/questions/3173131/redirect-copy-of-stdout-to-log-file-from-within-bash-script-itself)
exec > >(tee $LOG)
# also for stdout
exec 2>&1


echo -n "machines;memory;" >> $TIMES
# write experiments to headers
MERGED="$FLINK_EXPERIMENTS $SPARK_EXPERIMENTS"
for EXP in ${MERGED// / } ; do
	echo -n "$EXP;" >> $TIMES
	echo "EXP=$EXP"
done
# write newline
echo "" >> $TIMES


# arguments
# 1. Number of machines
# 2. Memory in MB per machine.
runExperiments() {
	MACHINES=$1
	MEMORY=$2
	echo "Generate configuration for $MACHINES machines and $MEMORY MB of memory"
	export DOP=`echo $MACHINES*$CORES_PER_MACHINE | bc`
	echo "Set DOP=$DOP"
	head -n $MACHINES flink-conf/slaves > _config-staging/slaves
	cp _config-staging/slaves $FILES_DIRECTORY/flink-build/conf/
	cp _config-staging/slaves $SPARK_HOME/conf/

	# Flink config
	cat flink-conf/flink-conf.yaml > _config-staging/flink-conf.yaml
	# appending ...
	echo "taskmanager.heap.mb: $MEMORY" >> _config-staging/flink-conf.yaml
	cp _config-staging/flink-conf.yaml $FILES_DIRECTORY/flink-build/conf/

	# Spark config
	cat spark-conf/spark-defaults.conf > _config-staging/spark-defaults.conf
	echo "spark.executor.memory            $MEMORY" >> _config-staging/spark-defaults.conf
	cp _config-staging/spark-defaults.conf $SPARK_HOME/conf/

	# Prepare timing file : write config WITHOUT \n to timing file
	echo -n "$MACHINES;$MEMORY;" >> $TIMES

	# do Flink experiments

	# start Flink
	./startFlink.sh
	echo "waiting for 60 seconds"
	sleep 60

	for EXP in ${FLINK_EXPERIMENTS// / } ; do
		echo "Starting experiment $EXP"
		start=$(date +%s)
		case "$EXP" in
		"flink-wordcount-wo-combine")
			HDFS_WC_OUT="$HDFS_WC_OUT-$EXP-$benchId"
			./runWC-JAPI-withoutCombine.sh &>> $LOG_dir"/$EXP-log"
			;;
		"flink-wordcount")
			HDFS_WC_OUT="$HDFS_WC_OUT-$EXP-$benchId"
			./runWC-JAPI.sh &>> $LOG_dir"/$EXP-log"
			;;
		*)
			echo "Unknown experiment $EXP"
			;;
		esac
		end=$(date +%s)
		expTime=$(($end - $start))
		echo "Experiment took $expTime seconds"
		echo -n "$expTime;" >> $TIMES
		sleep 2
	done
	./stopFlink.sh
	experimentsFlinkLog="$LOG_dir/flink-$MACHINES-$MEMORY-log/"
	mkdir -p $experimentsFlinkLog
	cp $FILES_DIRECTORY/flink-build/log/* $experimentsFlinkLog

	# Start Spark
	./startSpark.sh
	echo "waiting again, this time for Spark"
	sleep 60

	for EXP in ${SPARK_EXPERIMENTS// / } ; do
		start=$(date +%s)
		case "$EXP" in
		"spark-wordcount-wo-combine")
			HDFS_WC_OUT="$HDFS_WC_OUT-$EXP-$benchId"
			./runSpark-WC-Grouping-Java.sh &>> $LOG_dir"/$EXP-log"
			;;
		"spark-wordcount")
			export HDFS_WC_OUT="$HDFS_WC_OUT-second"
			HDFS_WC_OUT="$HDFS_WC_OUT-$EXP-$benchId"
			./runSpark-WC-Java.sh &>> $LOG_dir"/$EXP-log"
			;;
		*)
			echo "Unknown experiment $EXP"
			;;
		esac
		end=$(date +%s)
		expTime=$(($end - $start))
		echo "Experiment took $expTime seconds"
		echo -n "$expTime;" >> $TIMES
		sleep 2
	done
	./stopSpark.sh
	experimentsSparkLog="$LOG_dir/spark-$MACHINES-$MEMORY-log/"
	mkdir -p $experimentsSparkLog
	cp $SPARK_HOME/logs/* $experimentsSparkLog

	# write newline in timing
	echo "" >> $TIMES
}

echo "Building Flink, Spark, and Testjobs"
#./prepareFlink.sh
#./prepareSpark.sh
#./prepareTestjob.sh
echo "Testing scaling capabilities from 5 to 25 machines."

# 5 machines, 20GB mem each
runExperiments 5 20000
runExperiments 15 20000
runExperiments 25 20000

echo "Testing memory behavior"

runExperiments 5 5000
runExperiments 5 10000
runExperiments 5 15000
# the 5 / 20000 datapoint is available.

runExperiments 25 5000


echo "Experiments done "

tar czf "benchmarks/$LOG_dir.tgz" benchmarks/$LOG_dir
