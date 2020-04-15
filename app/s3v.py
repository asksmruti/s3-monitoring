import boto3
from botocore.exceptions import ClientError
import logging.config
from os import path, makedirs
from datetime import date, timedelta, datetime
from urllib.parse import urlparse
import sys
from configparser import ConfigParser

today = date.today().strftime('%Y-%m-%d')

# Default old date
N = 4000
date_N_days_ago = datetime.now() - timedelta(days=N)

# Setting logger

if not path.exists(path.join(path.dirname(path.abspath(__file__)), '../logs')):
    makedirs(path.join(path.dirname(path.abspath(__file__)), '../logs'))
log_conf_file_path = path.join(path.dirname(path.abspath(__file__)), '../conf/log.conf')
logging.config.fileConfig(log_conf_file_path, disable_existing_loggers=False)


# create logger
logger = logging.getLogger('datalake')

# Read whitelist DBs
conf_file_path = path.join(path.dirname(path.abspath(__file__)), '../conf/conf.ini')
config = ConfigParser()
config.read(conf_file_path)
ignore_db_list = ""

try:
    ignore_db_list = config['db']['ignore']
except KeyError:
    logger.error("Value does not exist")

# Glue operations


def init_session(profile):
    session = boto3.Session(region_name='eu-central-1',
                            profile_name=profile)
    glue_client = session.client('glue')
    s3_client = session.client('s3')

    return glue_client, s3_client


def s3_metadata(sc, bucket, prefix):
    all_dt = []
    s3_response = {}
    paginator = sc.get_paginator('list_objects')
    s3_response = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in s3_response:
        if "Contents" in page:
            for key in page["Contents"]:
                last_modified = key["LastModified"].strftime('%Y-%m-%d')
                all_dt.append(last_modified)

    if today > max(all_dt, default='xxxx-xx-xx'):
        logger.error("s3://%s/%s is not updated", bucket, prefix)
        return max(all_dt)
    else:
        return today


def get_db(c):
    all_db_list = []
    db_response = c.get_databases()
    raw_db_list = db_response['DatabaseList']
    for db in raw_db_list:
        all_db_list.append(db['Name'])
    db_list = [x for x in all_db_list if x not in ignore_db_list]
    return db_list


# Count based on Glue meta data
def table_record_count(tab):
    tab_dict = tab
    try:
        rc = tab_dict['Parameters']['recordCount']
    except KeyError as e:
        rc = "No records found"
    return rc


def get_tab(sc, gc, dbl):
    red_dict = {}
    tab_response = {}
    for db in dbl:
        next_token = ""
        try:
            tab_response = gc.get_tables(DatabaseName=db,
                                         NextToken=next_token)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExpiredTokenException':
                logger.error("AWS token expired")
                sys.exit(1)

        raw_tab_list = tab_response['TableList']

        for tbl in raw_tab_list:
            s3_location = ""
            try:
                s3_location = tbl['StorageDescriptor']['Location']
            except KeyError as e:
                logger.error("db = %s, table = %s, records = 0", db, tbl['Name'])

            if s3_location:
                bucket_parser = urlparse(s3_location, allow_fragments=False)
                bucket, prefix = bucket_parser.netloc, bucket_parser.path.lstrip('/')
                last_update_date = ""

                try:
                    last_update_date = s3_metadata(sc, bucket, prefix)
                except ClientError as e:
                    logger.error(e)

                if last_update_date != today:
                    red_dict.setdefault("Database", []).append(db)
                    red_dict.setdefault("Table", []).append(tbl['Name'])
                    red_dict.setdefault("S3 Location", []).append(s3_location)
                    red_dict.setdefault("Last updated", []).append(last_update_date)
                else:
                    logger.info("%s is updated", s3_location)
            else:
                logger.error("Invalid s3 path %s", s3_location)

            logger.info("db = %s, table = %s, records = %s",
                            db, tbl['Name'], table_record_count(tbl))
        next_token = tab_response.get('NextToken')
    return red_dict
