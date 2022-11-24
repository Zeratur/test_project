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


class dataUtils:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        conf = configparser.ConfigParser()
        conf.read("../config/config.ini")
        global host, port, user, password, database, properties
        host = conf.get("mysql", "host")
        port = conf.get("mysql", "port")
        user = conf.get("mysql", "user")
        password = conf.get("mysql", "password")
        database = conf.get("mysql", "database")
        properties = {'user': user, 'password': password}

    def read_db(self, table):
        """
        :param table: the table name of the target table
        :return: return nothing
        """
        url = 'jdbc:mariadb://' + host + ':' + port + '/' + database + '?useSSL=false'
        return self.spark.read.option("driver", "org.mariadb.jdbc.Driver") \
            .jdbc(url=url, table=table, properties=properties)

    def write_db(self, df, table, mode):
        """
        :param df: the data frame which you are writing in the database
        :param table: the target table to which you are writing data
        :param mode: "append" or "overwrite"
        :return: return nothing
        """
        url = 'jdbc:mariadb://' + host + ':' + port + '/' + database + '?useSSL=false'
        return df.write.mode(mode).option("truncate", "false").option("driver", "org.mariadb.jdbc.Driver") \
            .jdbc(url=url, table=table, properties=properties)

    def read_xlsx(self, file_name, data_addr) -> df.DataFrame:
        """
        :param file_name: The absolute path to the file
        :param data_addr: The starting cell of the file
        :return:
        """
        df = self.spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("dataAddress", data_addr) \
            .load(file_name)
        return df

    def write_xlsx(self, df,  mode, data_address, file_name):
        """
        :param df: data_frame create by spark session
        :param mode: "append" or "overwrite" AS STRING
        :param data_address: "'sheet1'!A1" or "'sheet1'!A1:C35" AS STRING
        :param file_name: "file_name" AS STRING
        :return: return nothing
        """
        df.write.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("dataAddress", data_address) \
            .option("dateFormat", "yyyy-mm-dd hh:mm:ss") \
            .option("timestampFormat", "yyyy-mm-dd hh:mm:ss") \
            .mode(mode) \
            .save(HOME_DIR + "/output/" + file_name + ".xlsx")

du=dataUtils()
df01 = du.read_db("emp_test01")
df01.show()
