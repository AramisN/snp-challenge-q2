from datetime import datetime

import click
import findspark
from pyspark.sql.types import IntegerType, DateType, StringType

findspark.init()
import pyspark
from pyspark.sql import SparkSession, functions, Window
from pyspark import SparkContext, SparkConf
# from pyspark.sql import functions
# from pyspark.sql.functions import *
from pyspark.sql.functions import col, asc, desc, row_number, regexp_replace, udf, lit  # pip install pyspark-stubs
import os


def init_spark_connection(appname, sparkmaster, minio_url,
                          minio_access_key, minio_secret_key):
    """ Init Spark connection and set hadoop configuration to read
    data from MINIO.

    Args:
        appname: spark application name.
        sparkmaster: spark master url.
        minio_url: an url to access to MINIO.
        minio_access_key: specific access key to MINIO.
        minio_secret_key: specific secret key to MINIO.

    Return:
         ss: spark session object
    """
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

    # spark configuration
    conf = SparkConf().set('spark.executor.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true').set(
        'spark.driver.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true').setAppName(
        'pyspark_aws').setMaster(
        'local')

    sc = SparkContext(conf=conf)
    sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

    print('modules imported')

    accessKeyId = 'UKfh8vjv0lYxTrBn'
    secretAccessKey = '23T1g5flwWbj9hrLmS6W7PBzjZTXfyxt'
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('fs.s3a.endpoint', 'http://172.18.60.9:9007')
    hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    ss = SparkSession(sc)

    return ss


def extract(ss, bucket_name, raw_data_path):
    """ Extract csv files from Minio.

    Args:
        sc: spark connection object.
        bucket_name: name of specific bucket in minio that contain data.
        raw_data_path: a path in bucket name that specifies data location.

    Return:
        df: raw dataframe.
    """
    # Print context
    # print('Created SparkContext: ', ss.sparkContext)
    # print('Hadoop version: ', ss.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion())

    df = ss.read.json('s3a://' + bucket_name + "/" + raw_data_path + "/*.json")

    return df


def transform(df):
    """ Transform dataframe to an acceptable form.

    Args:
        df: raw dataframe

    Return:
        df_before_load: processed dataframe
    """
    # todo: write the your code here

    spaceDeleteUDF = udf(lambda s: str(s).replace(" ", ""), StringType())
    datetimefunc = udf(lambda x: datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'), DateType())

    # 2.	Remove the user field. + retweeted_status and quoted_status
    selected_tweets = df.select('message.*')
    st1 = selected_tweets.drop('user').drop("quoted_status").drop('retweeted_status')

    # 3.	Remove retweeted_status and quoted_status if they are available in JSON objects and add them to dataframe as new rows.
    st1.withColumn("quoted_status", lit("")).withColumn("retweeted_status", lit(""))

    # 5.	Remove space characters from text fields.
    trimmed_text = st1.withColumn("text_new", spaceDeleteUDF("text"))

    # 4.	Remove duplicate tweets.4.	Remove duplicate tweets.
    asdasd = trimmed_text.drop_duplicates(['text_new']).drop('text')

    # 6.	Convert created_at field to DateTime with (year-month-day) format.
    conv_df = asdasd.withColumn('created_at_new', datetimefunc(col('created_at'))).withColumnRenamed("text_new", "text")

    # 7.	Partition dataframe based on created_at date.
    conv_df.drop("created_at")
    df_before_load = conv_df.repartition("created_at_new")

    return df_before_load


def load(df, bucket_name, processed_data_path):
    """ Load clean dataframe to MINIO.

    Args:
        df: a processed dataframe.
        bucket_name: the name of specific bucket in minio that contain data.
        processed_data_path: a path in bucket name that
            specifies data location.

    Returns:
         Nothing!
    """
    # todo: change this function if

    # 6.	Load data in MINIO.
    df.write.mode("overwrite").json('s3a://' + bucket_name + "/" + processed_data_path)


@click.command('ETL job')
@click.option('--appname', '-a', default='ETL Task', help='Spark app name')
@click.option('--sparkmaster', default='local',
              help='Spark master node address:port')
@click.option('--minio_url', default='http://172.18.60.9:9007',
              help='import a module')
@click.option('--minio_access_key', default='UKfh8vjv0lYxTrBn')
@click.option('--minio_secret_key', default='23T1g5flwWbj9hrLmS6W7PBzjZTXfyxt')
@click.option('--bucket_name', default='nasirianfarbucket')
@click.option('--raw_data_path_a', default='tweets_data')
@click.option('--raw_data_path_b', default='users_data')
@click.option('--processed_data_path', default='results')
# @click.option('--packages', default='software.amazon.awssdk:s3:2.17.52,org.apache.hadoop:hadoop-aws:3.1.2')
def main(appname, sparkmaster, minio_url,
         minio_access_key, minio_secret_key,
         bucket_name, raw_data_path_a, raw_data_path_b, processed_data_path):
    ss = init_spark_connection(appname, sparkmaster, minio_url,
                               minio_access_key, minio_secret_key)

    # extract data from MINIO
    df_tweets = extract(ss, bucket_name, raw_data_path_a)

    # transform data to desired form
    clean_df_tweets = transform(df_tweets)

    # load clean data to MINIO
    load(clean_df_tweets, bucket_name, processed_data_path)


if __name__ == '__main__':
    main()
