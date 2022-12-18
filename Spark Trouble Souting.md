## Trouble Shooting

Spark가 `com.mongodb.spark.sql.DefaultSource`패키지를 찾을 수 없는 오류

```
py4j.protocol.Py4JJavaError: An error occurred while calling o56.load.
: java.lang.ClassNotFoundException: Failed to find data source: com.mongodb.spark.sql.DefaultSource. Please find packages at http://spark.apache.org/third-party-projects.html
```



해결책 :

spark와 mongo 사이에 processing이 필요할 땐 **jar**들이 필요한데 버전영향을 많이 받는다.

따라서 나의 Spark버전에 맞는 MongoDB Spark Connector를 다운받는다.

1. MongoDB Spark Connector를 다운 ( https://spark-packages.org/package/mongodb/mongo-spark )
2. wget 명령어로 ubuntu에서 다운

```
/$SPARK_HOME/jars directory 이동 후

>wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar
```

| Mongo-Spark | Spark |
| ----------- | ----- |
| 3.0.1       | 3.0.x |
| 2.4.3       | 2.4.x |
| 2.3.5       | 2.3.x |
| 2.2.9       | 2.2.x |
| 2.1.8       | 2.1.x |



3. 추가 jars download
   mongo-java-driver : https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver/3.11.2

   ```
   >wget https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.11.2/mongo-java-driver-3.11.2.jar
   ```

   

   bson : https://mvnrepository.com/artifact/org.mongodb/bson/3.11.2

   ```
   >wget https://repo1.maven.org/maven2/org/mongodb/bson/3.11.2/bson-3.11.2.jar
   ```

   

참고 : https://qkqhxla1.tistory.com/1115 (mongoDB - spark - aws s3)





---



### PySpark와 MongoDB Connection

```python
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# conf = SparkConf()
# conf.set('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
# spark = SparkSession.builder.appName("multi").config(conf=conf).getOrCreate()

spark = SparkSession \
    .builder \
    .appName("multi") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017") \
    .config("spark.mongodb.input.database","datalake") \
    .config("spark.mongodb.input.collection", "i") \
    .config("packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

sc =spark.sparkContext
```



### 스키마 생성

```python
subwaySchema =  StructType([
    StructField("USE_DT", StringType(),True),
    StructField("LINE_NUM", StringType(),True),
    StructField("SUB_STA_NM", StringType(),True),
    StructField("RIDE_PASGR_NUM", IntegerType(),True),
    StructField("ALIGHT_PASGR_NUM", IntegerType(),True),
    StructField("WORK_DT", StringType(),True),
  ])
```



### 출력

```python
df = spark.read.schema(subwaySchema).format("com.mongodb.spark.sql.DefaultSource").load() 
df.limit(100).show()

+--------+--------+----------------------+--------------+----------------+--------+
|  USE_DT|LINE_NUM|            SUB_STA_NM|RIDE_PASGR_NUM|ALIGHT_PASGR_NUM| WORK_DT|
+--------+--------+----------------------+--------------+----------------+--------+
|20190101|   1호선|                서울역|         39420|           31121|20190104|
|20190101|   1호선|                  시청|         11807|           10322|20190104|
|20190101|   1호선|                  종각|         20944|           16658|20190104|
|20190101|   1호선|               종로3가|         17798|           15762|20190104|
|20190101|   1호선|               종로5가|         13578|           13282|20190104|
|20190101|   1호선|                동대문|          9337|           10457|20190104|
|20190101|   1호선|                신설동|          6832|            6930|20190104|
|20190101|   1호선|                제기동|         10187|           10178|20190104|
|20190101|   1호선|청량리(서울시립대입구)|         15007|           15397|20190104|
|20190101|   1호선|                동묘앞|          8045|            8504|20190104|
|20190101|   2호선|                  시청|          8381|            6049|20190104|
|20190101|   2호선|            을지로입구|         22478|           21330|20190104|
|20190101|   2호선|             을지로3가|          8104|            7554|20190104|
|20190101|   2호선|             을지로4가|          3862|            3728|20190104|
|20190101|   2호선|    동대문역사문화공원|         10995|           11808|20190104|
|20190101|   2호선|                  신당|          6811|            7324|20190104|
|20190101|   2호선|              상왕십리|          5723|            5960|20190104|
|20190101|   2호선|      왕십리(성동구청)|          9379|            8332|20190104|
|20190101|   2호선|                한양대|          2340|            2751|20190104|
|20190101|   2호선|                  뚝섬|          4882|            5204|20190104|
+--------+--------+----------------------+--------------+----------------+--------+
only showing top 20 rows
```





---





### 데이터 수집 과정 중 이슈

---



2019년 1월 1일 부터 현재에서 3일 전까지 날짜를 카운트 하기 위해 pd.date_range 를 통해 불러오기

```python
from datetime import datetime

today = datetime.today() - timedelta(3)
today_3 = today.strftime("%Y%m%d")
start = '20190101'

def date_range(start, end):
    start = datetime.strptime(start, "%Y%m%d")
    end = datetime.strptime(end, "%Y%m%d")
    dates = [date.strftime("%Y%m%d") for date in pd.date_range(start, periods=(end-start).days+1)]
    return dates
    
dates = date_range(start, today_3)
print(dates)

['20190101', '20190102', '20190103', '20190104', '20190105', '20190106', '20190107', '20190108', '20190109' ......'20210727', '20210728', '20210729', '20210730', '20210731', '20210801', '20210802', '20210803', '20210804', '20210805', '20210806', '20210807']
```







---





### PySpark와 MySQL 연결과정 이슈

---



- 스파크가 MySQL과 커넥션을 하기 위한 JDBC Connector를 다운
  1. https://dev.mysql.com/downloads/connector/j/  tar.gz 파일을 저장
  2. `tar -zxvf [파일명.tar.gz]` 으로 압축풀기
  3. `$SPARK_HOME/jars/` 디렉토리에 mysql-connector-java-8.0.17.jar 파일 저장
  4.  pyspark에서 Session 연결



문제 : 

기본적으로 초기설정되어 있는 mysql의 root계정의 패스워드 타입 때문에 이와 같은 에러가 발생

```
'ERROR 1698 (28000): Access denied for user 'root'@'localhost'
```



해결책:

1.

```mysql
$ sudo mysql -u root # 터미널에서 sudo를 사용하여 root계정으로 mysql에 접속한다. 


mysql> select User, Host, plugin from mysql.user;

+------------------+-----------+----------------------------------------+

| User                            | Host      | plugin                                          |

+------------------+-----------+-----------------------------------------+

| root                             | localhost | auth_socket                       |

| mysql.session                    | localhost | mysql_native_password |

| mysql.sys                        | localhost | mysql_native_password |

| debian-sys-maint                 | localhost | mysql_native_password |

+------------------+-----------+----------------------------------------+
```



2. mysql_native_password로 변경해 준 다음 비밀 번호를 설정하면 접속이 가능해 진다.

```mysql
mysql> update user set plugin='mysql_native_password' where user='root';

mysql> flush privileges; # mysql user table을 변경 후 꼭 해주어야 적용이 된다

mysql> select User, Host, plugin from mysql.user;

+------------------+-----------+----------------------------------------+

| User                            | Host      | plugin                                          |

+------------------+-----------+-----------------------------------------+

| root                             | localhost | mysql_native_password |

| mysql.session        | localhost | mysql_native_password |

| mysql.sys                 | localhost | mysql_native_password |

| debian-sys-maint | localhost | mysql_native_password |

+------------------+-----------+----------------------------------------+
```



3. 위와 같이 plugin이 변경된 것을 확인 한 후 비밀번호를 설정한다.

```mysql
mysql>SET PASSWORD FOR 'root'@'localhost' = PASSWORD('변경할 비밀번호');



mysql> flush privileges; # mysql user table을 변경 후 꼭 해주어야 적용이 된다
```



다시 우분투 터미널로 가서 접속을 시도하면 정상적으로 접속되는 걸 확인 할 수 있다

참고 :  https://jcon.tistory.com/130 (mysql root 계정 접속 문제)





---





### JDBC Connector를 통해 다른 database에 DataFrame 삽입하기



1. Spark를 통해  MySQL 읽어오기

   ```python
   jdbcDF = spark.read \
       .format("jdbc") \
       .option("url", "jdbc:mysql:dbserver") \
       .option("driver", "com.mysql.jdbc.Driver") \
       .option("dbtable", "schema.tablename") \
       .option("user", "username") \
       .option("password", "password") \
       .load()
   ```



2. Spark를 통해 MySQL 저장하기

   ```python
   jdbcDF.write \
       .format("jdbc") \
       .option("url", "jdbc:mysql:dbserver") \
       .option("dbtable", "schema.tablename") \
       .option("user", "username") \
       .option("password", "password") \
       .save()
   ```










## PandasDataFrame To SparkDataFrame 



큰 데이터를 만지다보면 Spark의 DataFrame과 Pandas의 DataFrame의 서로 변환이 꼭 필수이다.

Pandas의 DataFrame을 Spark SQL의 테이블로 등록하고, Spark에서 작업을 하기도 한다.



- Pandas의 DataFrame을 Spark의 DataFrame으로 변환하기 위해서는 

  `spark.createDataFrame(df)`를 하면된다.



- spark의 DataFrame을 Pandas의 DataFrame으로 변환하기 위해서는

  `df.toPandas()` 를 하면된다.







----



 ## MySQL Workbench에서 문제



mysql에서 특정한 sql을 실행을 하는데 아래와 같은 에러가 리턴된다.

```
Error Code: 1175. You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column To disable safe mode, toggle the option in Preferences -> SQL Editor and reconnect.
```



#### 에러원인

테이블에서 키값을 이용한 update나 delete만을 허용하도록 되어 있는데, 그렇지 않게 좀 더 넓은 범위의

sql을 적용하려고 할 때 workbench에서 경고를 주는 것임

즉 하나의 레코드만을 update, delete하도록 설정되어 있는데, 다수의 레코드를 update나 delete 하는

sql명령어가 실행되기 때문에 발생한다.



#### 해결방법 (1)

아래와 같은 sql로 환경변수를 변경해준다. (일시적인 Safe모드 해제)
set sql_safe_updates=0;



#### 해결방법 (2)

Workbench Preferences에서 안전모드(Safe mode)를 해제한다.
아래의 그림에 있는 부분에서 체크를 해제한후에 다시 workbench를 시작한다.

참고 : https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html (spark JDBC)





pyspark example: https://sparkbyexamples.com/pyspark/pyspark-read-json-file-into-dataframe/

https://www.mongodb.com/blog/post/getting-started-with-mongodb-pyspark-and-jupyter-notebook

