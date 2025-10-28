from pyspark.sql.types import StringType
import dbt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def model(dbt, session):
    # 1️⃣ 定义 UDF
    def to_uppercase(s):
        if s is None:
            return None
        return s.upper()

    # 2️⃣ 注册到 SparkSession
    session.udf.register("to_uppercase", to_uppercase, StringType())

    # 3️⃣ 创建 DataFrame（示例数据）
    data = [("alice",), ("bob",), ("charlie",)]
    columns = ["name"]
    df = session.createDataFrame(data, columns)

    # ✅ 在 dbt Python model 中 return dataframe
    return df
