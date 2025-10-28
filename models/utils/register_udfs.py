import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

# from encrypt_utils import encrypt_base64, encrypt_sha256, complex_encrypt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def model(dbt, session):
    # 设置物化方式
    # dbt.config(materialized="table")

    # 注册 Python 函数为 Spark UDF
    session.udf.register("upp2", lambda x:str(x).upper(), StringType())
    # session.udf.register("encrypt_base64", lambda x:str(x).upper(), StringType())
    # session.udf.register("encrypt_sha256", lambda x:str(x).upper(), StringType())
    # session.udf.register("complex_encrypt", lambda x:str(x).upper(), StringType())

    # 获取上游表
    return session.createDataFrame([(1, "UDFs registered")], ["id", "status"])