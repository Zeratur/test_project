import pymysql
import configparser
from configparser import ConfigParser
from pyspark.sql import SparkSession
import os


def getConfig(file_name, section, option):
    # create configparser object
    conf = configparser.ConfigParser()

    # read config contents
    conf.read(file_name)
    config = conf.get(section, option)
    return config


host = getConfig("config.ini", "mysql", "host")
port = getConfig("config.ini", "mysql", "port")
user = getConfig("config.ini", "mysql", "user")
passwd = getConfig("config.ini", "mysql", "password")
database = getConfig("config.ini", "mysql", "database")
properties = {'user': user, 'password': passwd}


def read_db(table, spark):
    url = 'jdbc:mariadb://' + host + ':' + port + '/' + database + '?useSSL=false'
    return spark.read.option("driver", "org.mariadb.jdbc.Driver") \
        .jdbc(url=url, table=table, properties=properties)


def write_xlsx(df, mode, data_address, file):
    df.write.format("com.crealytics.spark.excel") \
        .option("dataAddress", "'My Sheet'!B3:C35") \
        .option("useHeader", "false") \
        .option("dateFormat", "yyyy-mm-dd hh:mm:ss") \
        .option("timestampFormat", "yyyy-mm-dd hh:mm:ss") \
        .mode("append") \
        .save("/file.xlsx")  # 要输出的HDFS文件路径.
