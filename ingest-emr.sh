#!/usr/bin/env bash

this_script_name=$0
output=$1               # Output to
name=$3                 # name of provider
input=$4                # input
ec2_type=$5             # ec2 instance for cluster


harvest_input=${input}${name}/harvest/

echo "Running with parameters:
  - name=$name
  - output=$output
  - input=$input
  - harvest_input=$harvest_input
  - ec2_type=$ec2_type
  " 2>&1 | tee /tmp/spark-submit-log

# build and deploy ingestion3 to s3
sbt assembly
echo "Copying to s3://dpla-ingestion3/"
aws s3 cp ./target/scala-2.11/ingestion3-assembly-0.1.0.jar s3://dpla-ingestion3/

# spin up EMR cluster and run job
# subnet-1bb90e53 = public-us-east-1b
# Old config ec2-attributs
#  "ServiceAccessSecurityGroup": "sg-07459c7a",
#  "EmrManagedSlaveSecurityGroup": "sg-0a459c77",
#  "EmrManagedMasterSecurityGroup": "sg-08459c75"
aws emr create-cluster \
--auto-terminate \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Spark \
--ebs-root-volume-size 75 \
--ec2-attributes '{
  "KeyName": "general",
  "InstanceProfile": "EMR_EC2_DefaultRole",
  "SubnetId": "subnet-90afd9ba",
  "ServiceAccessSecurityGroup": "sg-07459c7a",
  "EmrManagedSlaveSecurityGroup": "sg-0a459c77",
  "EmrManagedMasterSecurityGroup": "sg-08459c75"
}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.32.0 \
--log-uri 's3n://aws-logs-283408157088-us-east-1/elasticmapreduce/' \
--steps '[
  {
    "Args": [
      "spark-submit",
      "--deploy-mode",
      "cluster",
      "--driver-memory",
      "18G",
      "--executor-memory",
      "18G",
      "--num-executors",
      "4",
      "--executor-cores",
      "2",
      "--class",
      "dpla.ingestion3.entries.ingest.IngestRemap",
      "s3://dpla-ingestion3/ingestion3-assembly-0.1.0.jar",
      "'"--output $output"'",
      "'"--name $name"'",
      "'"--input $harvest_input"'"
    ],
    "Type": "CUSTOM_JAR",
    "ActionOnFailure": "TERMINATE_CLUSTER",
    "Jar": "command-runner.jar",
    "Properties": "",
    "Name": "spark-ingest"
  }
]' \
--name 'ingest' \
--instance-groups '[
  {
    "InstanceCount": 4,
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 250,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "CORE",
    "InstanceType": "'"$ec2_type"'",
    "Name": "Core - 2"
  },
  {
    "InstanceCount": 10,
    "BidPrice": "OnDemandPrice",
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 250,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "TASK",
    "InstanceType": "'"$ec2_type"'",
    "Name": "Task - 3"
  },
  {
    "InstanceCount": 1,
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 250,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "MASTER",
    "InstanceType": "'"$ec2_type"'",
    "Name": "Master - 1"
  }
]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-east-1