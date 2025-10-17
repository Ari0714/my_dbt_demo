
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

# from encrypt_utils import encrypt_base64, encrypt_sha256, complex_encrypt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def model(dbt, session):
    # 设置物化方式
    dbt.config(materialized="table")

    # 注册 Python 函数为 Spark UDF
    session.udf.register("upp", lambda x:str(x).upper(), StringType())
    # session.udf.register("encrypt_base64", lambda x:str(x).upper(), StringType())
    # session.udf.register("encrypt_sha256", lambda x:str(x).upper(), StringType())
    # session.udf.register("complex_encrypt", lambda x:str(x).upper(), StringType())

    # 获取上游表
    df = dbt.ref("my_first_dbt_model")

    # # 调用注册的 UDF
    # df = df.select(
    #     "id",
    #     F.expr("encrypt_base64(phone)").alias("phone_base64"),
    #     F.expr("encrypt_sha256(phone)").alias("phone_sha256"),
    #     F.expr("complex_encrypt(phone)").alias("phone_complex")
    # )

    return df


# def model(dbt, spark):
#     def encrypt_base64(val):
#         if val is None:
#             return None
#         return str(val).upper()
#
#     # ✅ 在 SparkSession 注册 UDF，全局有效
#     spark.udf.register("encrypt_base64", encrypt_base64, StringType())
#
#     return dbt.ref("my_first_dbt_model")
#     # df = df.withColumn("phone_encrypted", F.expr("encrypt_base64(phone)"))
#     #  df