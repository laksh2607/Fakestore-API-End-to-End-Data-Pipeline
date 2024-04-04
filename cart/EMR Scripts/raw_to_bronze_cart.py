import json
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import shutil
import boto3

spark = SparkSession.builder.getOrCreate()
session = boto3.session.Session()
secret_name = "cart"
region_name = "eu-west-3"
client = session.client(service_name='secretsmanager', region_name=region_name)
get_secret_value_response = client.get_secret_value(SecretId=secret_name)

def data_validation(col_names):
      date_flag = 'date' in col_names
      id_flag = 'id' in col_names
      products_flag = 'products' in col_names
      userId_flag = 'userId' in col_names

      return (date_flag and id_flag and products_flag and userId_flag)

def get_cart_details(date):
    print(get_secret_value_response)
    raw_input_folder =  json.loads(get_secret_value_response['SecretString'])['cart_raw_input_folder']
    raw_archive_folder = json.loads(get_secret_value_response['SecretString'])['cart_raw_archive_folder']
    raw_issue_folder = json.loads(get_secret_value_response['SecretString'])['cart_raw_issue_folder']
    raw_bronze_folder = json.loads(get_secret_value_response['SecretString'])['cart_bronze_input_folder']
    raw_file_name = "cart_daily_data_"+date.replace("-","")+".json"

    try:
      df = spark.read.json(raw_input_folder+raw_file_name)
      # df.show()
      df = df.drop(df['date'])
      df = df.withColumn('date',F.lit(date))

      col_names = df.columns

      if(data_validation(col_names)):
        df = df.select(df['date'].alias('DATE'),df['id'].alias('ID'),F.explode(df['products']).alias('products'),df['userId'].alias('USER_ID'))
        df = df.select('DATE','ID','USER_ID',df['products.productId'].alias('PRODUCT_ID'),df['products.quantity'].alias('PRODUCT_QUANTITY'))
        # df.show()
        df.write.option("header",True).mode("overwrite").parquet(raw_bronze_folder+raw_file_name.split('.')[0])

        #Run successful-> put input file to archive
        shutil.move(raw_input_folder+raw_file_name,raw_archive_folder+raw_file_name)
        return {'statusCode': 200,'Message' : 'Data Load Successfully in Bronze Layer'}
      else:
        shutil.move(raw_input_folder+raw_file_name,raw_issue_folder+raw_file_name)
        return {'statusCode' : 400, 'Message': 'Required Columns are missing'}

    except Exception as e:
      #there is some issue in file -> move input file to issue folder
      shutil.move(raw_input_folder+raw_file_name,raw_issue_folder+raw_file_name)
      print(e)
      return {'statusCode': 400,'Message' : str(e)}


if __name__ == '__main__' :
  get_cart_details('2024-03-21')


