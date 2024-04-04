import json
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime as dt
from datetime import timedelta as td
import shutil
import os
from pyspark.sql.types import StructType

spark = SparkSession.builder.getOrCreate()

def file_exist_validation(file_path):
  try:
    for file in os.listdir(file_path):
      if '.parquet' in file.lower() :
        return True
  except :
      return False

def get_cart_details(date):
    bronze_input_folder = 's3://dataconnectbronze/cart/input/'
    bronze_archive_folder = '/content/drive/MyDrive/DataCartConnect/bronze/cart/archive/'
    bronze_issue_folder = '/content/drive/MyDrive/DataCartConnect/bronze/cart/issue/'
    bronze_silver_folder = '/content/drive/MyDrive/DataCartConnect/silver/cart/'
    bronze_file_name = "cart_daily_data_"+date.replace("-","")
    product_silver_folder = '/content/drive/MyDrive/DataCartConnect/silver/product/'
    # print(bronze_file_name)

    try:
      year = date.split("-")[0]
      month = date.split("-")[1]
      day = date.split("-")[2]

      if file_exist_validation(bronze_input_folder+bronze_file_name):

        df = spark.read.parquet(bronze_input_folder+bronze_file_name)
        product_silver_df = spark.read.parquet(product_silver_folder).filter((F.col('START_DATE') <= date) & (F.col('END_DATE') > date))
        final_df = product_silver_df.alias('product').join(df.alias('cart')\
                                    ,F.col('product.ID') == F.col('cart.PRODUCT_ID')\
                                    ,'inner')
        # final_df.show()
        final_df =final_df.select('product.PRODUCT_KEY'
                        ,'cart.DATE'
                        ,'cart.ID'
                        ,'cart.USER_ID'
                        ,'cart.PRODUCT_ID'
                        ,'cart.PRODUCT_QUANTITY')


        final_df = final_df.withColumn('YEAR',F.lit(year)).withColumn('MONTH',F.lit(month)).withColumn('DAY',F.lit(day)).withColumn('INSERTED_ON',F.lit(dt.now()))

        try :
          partition_path = bronze_silver_folder+'YEAR='+year+'/MONTH='+month+'/DAY='+day
          existing_partitions = spark.read.parquet(partition_path)
        except :
          emptRDD = spark.sparkContext.emptyRDD()
          schema = StructType([])
          existing_partitions = spark.createDataFrame(emptRDD,schema)

        if existing_partitions.count() > 0:
          delete_path = bronze_silver_folder+'YEAR='+year+'/MONTH='+month+'/DAY='+day
          shutil.rmtree(delete_path)

        final_df.write.option("header",True).mode("append")\
        .partitionBy("YEAR","MONTH","DAY")\
        .parquet(bronze_silver_folder)
        shutil.move(bronze_input_folder+bronze_file_name,bronze_archive_folder+bronze_file_name)
        return {'statusCode': 200,'Message' : 'Data Load Successfully in Silver Layer'}
      else:
         return {'statusCode': 400,'Message' : 'File Does not exist in Bronze Input Layer.'}
    except Exception as e:
      shutil.move(bronze_input_folder+bronze_file_name,bronze_issue_folder+bronze_file_name)
      return {'statusCode': 400,'Message' : str(e)}

get_cart_details('2024-03-15')

#Display Silver layer file
parquet_file_path = '/content/drive/MyDrive/DataCartConnect/silver/cart/'
df = spark.read.parquet(parquet_file_path)
df.show()

silver_input_folder = '/content/drive/MyDrive/DataCartConnect/silver/product/'
df = spark.read.parquet(silver_input_folder)
df = df.filter(df['START_DATE'] == '2024-03-18')
df.show()

