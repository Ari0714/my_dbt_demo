{% macro creating_udf() %}

create function if not exists {{target.schema}}.sha3_512(str varchar)
returns varchar
language python
runtime_version = '3.8'
handler = 'hash'
as

$$
import hashlib

def hash(str):
    # create a sha3 hash object
    hash_sha3_512 = hashlib.new("sha3_512", str.encode())

    return hash_sha3_512.hexdigest()
$$
;

{% endmacro %}