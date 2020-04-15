### S3 bucket monitoring 

The attached code solution is based on a problem scenario which may helpful in some cases 

#### Problem 
To monitor whether there is a new file added to s3 bucket.

* Which buckets to monitor :
    * Only the bucket where data exists and queryable from athena
* How to get the bucket prefix :
    * From Glue metastore
* How to get latest data arrival date :
    * In some cases glue metastore may not have information about arrival of data if the crawler did not run, So `paginator` option of boto3 is being used. This can also be done through `Amazon S3 inventory` if the number objects are very huge.

* What it will do : 
    * If it finds any s3 prefix which has not been updated with latest file then it will generate a report 
    * Report can accessible through rest, dashboard and email notification
    * Scheduler can be enabled to send email notification
    * Every hit to API will refresh the data
    * The logger has also been enabled for monitoring purpose
* How to run :
    * Set the aws profile, email server details 
    * Install the pre-requisites `pip3 install requirements.txt`
    * run `python3 main.py` 

Probably need to add one more caching layer in-case the number objects are huge 

#### How to run exporter
Please run the s3-exporter

`python3 s3-exporter.py`

Metrics endpoint: 

http://localhost:8000/

Sample prometheus config setup

```
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'dl-monitoring'

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
    - targets: ['127.0.0.1:8000']

  - job_name: 'node-exporter'

```
