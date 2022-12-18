## Batch Processing



1. Open API - MongoDB - Spark - MySQL Batch processing

```python
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
from xml.etree import ElementTree


from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import array_contains, udf

from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd
import json

# pymongo connect
client = MongoClient('localhost',27017) # mongodb 27017 port
db = client.datalake

skey = "*******************"
today = datetime.today() - timedelta(4)
today_4 = today.strftime("%Y%m%d")

url = "http://openapi.seoul.go.kr:8088/{0}/json/CardSubwayStatsNew/1/1000/".format(skey)

# pymongo insert
try:
    response_body = urlopen(url)
    body = json.loads(response_body.read())
    responseBody = urlopen(url+today_4).read().decode('utf-8')
    jsonArray = json.loads(responseBody)
    storeInfosArray= jsonArray["CardSubwayStatsNew"]["row"]
    if storeInfosArray is not None:
        for j in range(len(storeInfosArray)):
            db.i.insert_one(storeInfosArray[j])
except KeyError :
    print('NOT EXIST DATA')

# spark session connect
spark = SparkSession \
    .builder \
    .appName("multi") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017") \
    .config("spark.mongodb.input.database","datalake") \  ## database
    .config("spark.mongodb.input.collection", "i") \	  ## table 
    .config("packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

sc =spark.sparkContext

# 데이터 스키마 지정
subwaySchema =  StructType([
    StructField("USE_DT", StringType(),True),
    StructField("LINE_NUM", StringType(),True),
    StructField("SUB_STA_NM", StringType(),True),
    StructField("RIDE_PASGR_NUM", IntegerType(),True),
    StructField("ALIGHT_PASGR_NUM", IntegerType(),True),
    StructField("WORK_DT", StringType(),True),
  ])

# spark.sql로 현재 날짜를 추출해서 WORK_DT 컬럼 DROP
df = spark.read.schema(subwaySchema).format("com.mongodb.spark.sql.DefaultSource").load() 
df.createOrReplaceTempView("df2021")
df2021 = spark.sql("SELECT * FROM df2021 WHERE USE_DT = {}".format(today_4))
df2021 = df2021.drop('WORK_DT')

# Mysql proj tot2021 table에 append 
df2021.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/proj") \
    .option("dbtable", "proj.tot2021") \
    .option("user", "root") \
    .option("password", "1234") \
    .mode('append').save()
```





---





## crontab

[Linux] Python (.py)을 가상환경(virtualenv)에서 주기별로 실행하기



crontab 문법(주기)

```
*        *        *       *        *
분(0-59) 시(0-23) 일(1-31) 월(1-12) 요일(0-7)
```

요일에서 **0 - 일요일** 



1. 매분 실행

   ```
   * * * * * python test.py
   ```

2. 1시간마다 실행

   ```
   * */1 * * * python test.py
   ```

3. 매시 10분에 실행

   ```
   10 * * * * python test.py
   ```

4. 매시 0, 10, 20, 30, 40, 50분에 실행

   ```
   0,10,20,30,40,50 * * * * python test.py
   ```

5. 5분마다 실행

   ```
   */5 * * * python test.py
   ```

6. 10일~15일, 1시, 5시, 9시에 15분마다 실행

   ```
   */15 1,5,9 10-15 * * python test.py
   ```



### crotab 기본 명령어

`crontab -e` 편집

`crontab -l` 리스트 보기

`crontab -r` 삭제



### Anaconda 가상환경에서 .py 파일 실행

가상환경의 python을 먼저 입력해 주고 그 다음에는 .py 파일이 위치한 경로와 파일명을 적어줍니다.

```
# 매시 10분, (가상환경 경로)                     (파일 경로)
10 * * * * ~/anaconda3/envs/my_env/bin/python ~/workspace/test.py
```



