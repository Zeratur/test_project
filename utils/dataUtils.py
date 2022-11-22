import os
import sys

HOME_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(HOME_DIR)
print(HOME_DIR)
import pymysql
import configparser
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql import dataframe as df


def getConfig(file_name, section, option):
    """
    :param file_name: config file name
    :param section: config section
    :param option: config option
    :return: the Minimum config object
    """
    conf = configparser.ConfigParser()
    conf.read(file_name)
    config = conf.get(section, option)
    return config


host = getConfig("../config/config.ini", "mysql", "host")
port = getConfig("../config/config.ini", "mysql", "port")
user = getConfig("../config/config.ini", "mysql", "user")
passwd = getConfig("../config/config.ini", "mysql", "password")
database = getConfig("../config/config.ini", "mysql", "database")
properties = {'user': user, 'password': passwd}


def read_db(table, spark):
    """
    :param table: the table name of the target table
    :param spark: sparkSession object
    :return: return nothing
    """
    url = 'jdbc:mariadb://' + host + ':' + port + '/' + database + '?useSSL=false'
    return spark.read.option("driver", "org.mariadb.jdbc.Driver") \
        .jdbc(url=url, table=table, properties=properties)


def write_db(df, table, mode):
    """
    :param df: the data frame which you are writing in the database
    :param table: the target table to which you are writing data
    :param mode: "append" or "overwrite"
    :return: return nothing
    """
    url = 'jdbc:mariadb://' + host + ':' + port + '/' + database + '?useSSL=false'
    return df.write.mode(mode).option("truncate", "false").option("driver", "org.mariadb.jdbc.Driver") \
        .jdbc(url=url, table=table, properties=properties)


def write_xlsx(df, mode, data_address, file_name):
    """
    :param df: data_frame create by spark session
    :param mode: "append" or "overwrite" AS STRING
    :param data_address: "'sheet1'!A1" or "'sheet1'!A1:C35" AS STRING
    :param file_name: "file_name" AS STRING
    :return: return nothing
    """
    df.write.format("com.crealytics.spark.excel") \
        .option("dataAddress", data_address) \
        .option("header", "true") \
        .option("dateFormat", "yyyy-mm-dd hh:mm:ss") \
        .option("timestampFormat", "yyyy-mm-dd hh:mm:ss") \
        .mode(mode) \
        .save(HOME_DIR + "/output/" + file_name + ".xlsx")


spark = SparkSession.builder \
    .getOrCreate()

df01 = read_db("emp_test01", spark)
df01.show()
write_xlsx(df01, mode="append", data_address="A1", file_name="test01")
