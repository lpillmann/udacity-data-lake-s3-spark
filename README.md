# Data Lake with AWS S3 and Spark
This repository contains the code for the fourth project in Udacity's Data Engineering Nanodegree program.

## Overview
The project consists of loading data from S3, transforming with Spark into a star schema and writing the result back to S3.


What is in this repository:
```
.
├── etl.py                               -> ETL main code
├── notebooks
│   └── sparkify.ipynb                   -> Notebook used during development
├── README.md                            -> Intro to project and how to use (this file) 
└── scripts
    ├── add_emr_step.sh                  -> Add EMR step to execute ETL
    ├── copy_etl_script_to_s3.sh         -> Copy local etl.py to S3 to be used by EMR Step
    ├── copy_output_from_hdfs_to_s3.sh   -> Copy output parquet files from HDFS to S3
    └── launch_cluster.sh                -> Launch EMR cluster
```

## Implementation notes

The implemenation has the following steps:

1. Load from S3 using Spark
1. Transform using Spark SQL API
1. Write processed data to HDFS
1. Use `s3-dist-cp` to copy from HDFS to S3

Using Spark SQL API was a personal choice, given my interest will be mostly using it instead of the Dataframe API.

Writing processed data to HDFS first and then copying to S3 was done to improve total processing time, since transfering directly to S3 was taking too long.

## Getting Started
Clone this repository and follow steps below within the project directory.
### Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/)
- An AWS profile configured (this project uses profile `udacity-emr`)
- Valid key pair (.pem) associated with your profile
- A valid and pubicly accessible S3 bucket (for development purposes only)

Note: You will need to update the parameters of Shell scripts with your own values (e.g. cluster id, profile name, bucket name, etc).

### Running the ETL steps

Launch AWS EMR cluster

```bash
bash scripts/launch_cluster.sh
```
> wait until cluster is ready...

Copy latest `etl.py` version to S3 bucket

```bash
bash scripts/copy_etl_script_to_s3.sh
```

Add EMR step (new job to be executed)

```bash
bash scripts/add_emr_step.sh
```
> wait until job finishes...

Copy output parquet files to S3

```bash
ash scripts/copy_output_from_hdfs_to_s3.sh
```

## Built With

  - [AWS EMR](https://aws.amazon.com/emr/) - Used as Big Data cluster
  - [AWS S3](https://aws.amazon.com/s3/) - Storage of data sources
  - [Apache Spark](https://spark.apache.org/) - Data processing engine
  - [PySpark](https://spark.apache.org/docs/latest/api/python/) - Python module to interact with Spark

## Acknowledgments

  - Based on Udacity's code template and project guide
