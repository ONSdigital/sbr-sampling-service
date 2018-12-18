#! /bin/bash
# may need to remove quotes => $#
if ["$#" -ne 2]; then
    echo "[WARN] Usage: Calling this script requires 2 arguments i.e. <NODE> = the node the JAR will be stored, <USER> = the user the job will be executed by."
    exit 1
fi

NODE=$1
USER=$2
APP_DOMAIN=${3:-uk.gov.ons.sbr.service}
APP_MAIN=${4:-SamplingServiceMain}
APP_VERSION=${5:-1.0}

#SPARK_PRINT_LAUNCH_COMMAND=1
#./bin/
spark2-submit \
    --verbose \
    --class ${APP_DOMAIN}.${APP_MAIN} \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 6 \
    --driver-memory 4G \
    --executor-memory 30G \
    hdfs://${NODE}/user/${USER}/lib/sampling-service/sampling-${APP_DOMAIN}-${APP_VERSION}-all.jar
