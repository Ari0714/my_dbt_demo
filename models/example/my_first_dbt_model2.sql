
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{
    config(
    materialized='table',
    pre_hook=[
        "CREATE temporary FUNCTION getLength as 'com.itbys.hive_test.SelfHiveFunction' using jar '/opt/data/02-hdp-base-1.0-SNAPSHOT-jar-with-dependencies.jar';"
    ]
) }}

with source_data as (

    select '1' as id, getLength('aaa') as data, current_timestamp() as ts

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
