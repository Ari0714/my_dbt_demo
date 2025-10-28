-- macros/check_and_update_schema.sql
{% macro check_and_update_schema() %}
  CREATE temporary FUNCTION getLength as 'com.itbys.hive_test.SelfHiveFunction' using jar '/opt/data/02-hdp-base-1.0-SNAPSHOT-jar-with-dependencies.jar';
{% endmacro %}