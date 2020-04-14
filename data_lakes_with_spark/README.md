# Project 4: Data Lake with Spark

The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository. Comments are used effectively and each function has a docstring

## Introduction
As data engineer, we are developing a data lake for the analytics team at Sparkify using existing datawarehouse house and applying processing with spark.

# Run The Scripts
The root or core of project is the `etl.py`, which will read in files from S3 buckets, process using Spark, and store them back to S3 buckets, partitioned appropriately.


### Song Dataset
The first dataset is a subset of real data from the [Million Song](https://labrosa.ee.columbia.edu/millionsong). File format is json and partitioned by first three alphabets of song's track ID

### Log Dataset
The second dataset is the user activity log for the above given dataset, it's user playing different songs on the app. Dataset is partitioned by year and month. 