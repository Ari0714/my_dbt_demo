
-- 这个模型用于注册UDF函数
{{ config(
    materialized='table',
    pre_hook=[
        "CREATE FUNCTION upp as 'com.itbys.hive_test.SelfHiveFunction' using jar '/opt/data/02-hdp-base-1.0-SNAPSHOT-jar-with-dependencies.jar';"
    ]
) }}

-- 这是一个空表，只是为了执行pre_hook
SELECT 1 as dummy
