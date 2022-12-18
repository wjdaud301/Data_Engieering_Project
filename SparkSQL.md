## SparkSQL 



데이터 프레임에 SQL을 적용시킬 수 있는 객체를 별도로 만든다. 이때,
`SparkDataFrame.createOrReplaceTempView("객체명")` 메소드를 사용한다. `spark.sql()` 메쏘드를 사용해서 SQL 문을 던져 원하는 결과를 얻을 수 있다.

```python
iris_df.createOrReplaceTempView("iris")

spark.sql("SELECT * FROM iris LIMIT 5").show()
```





### 데이터 프레임 생성

`createDataFrame()` 메쏘드를 사용해서 스파크 데이터프레임을 작성한다. 그리고, 판다스 데이터프레임에서 스파크 데이터프레임도 생성이 가능하다. 앞써 `spark.read_csv()` 메쏘드, DataFrameReader를 사용해서 스파크 데이터프레임 생성하는 것도 가능하다.

```python
df1 = spark.createDataFrame([(1, "andy", 20, "USA"), 
                             (2, "jeff", 23, "China"), 
                             (3, "james", 18, "USA")]).toDF("id", "name", "age", "country")

df1.show()
+---+-----+---+-------+
| id| name|age|country|
+---+-----+---+-------+
|  1| andy| 20|    USA|
|  2| jeff| 23|  China|
|  3|james| 18|    USA|
+---+-----+---+-------+
```



```python
df2 = spark.createDataFrame(df1.toPandas())
df2.show()
+---+-----+---+-------+
| id| name|age|country|
+---+-----+---+-------+
|  1| andy| 20|    USA|
|  2| jeff| 23|  China|
|  3|james| 18|    USA|
+---+-----+---+-------+
```





### 칼럼 추출 및 제거

`select`와 마찬가지로 원하는 변수 칼럼을 추출하고자 할 때는 `select` 메쏘드를 사용한다. 칼럼을 제거하고자 하는 경우 `drop`을 사용한다.

```python
df2 = df1.select("id", "name")
df2.show()

+---+-----+
| id| name|
+---+-----+
|  1| andy|
|  2| jeff|
|  3|james|
+---+-----+
```



```python
df1.drop("id", "name").show()

+---+-------+
|age|country|
+---+-------+
| 20|    USA|
| 23|  China|
| 18|    USA|
+---+-------+
```





### 관측점 행 추출

filter method 를 사용

```python
df1.filter(df1["age"] >= 20).show()

+---+----+---+-------+
| id|name|age|country|
+---+----+---+-------+
|  1|andy| 20|    USA|
|  2|jeff| 23|  China|
+---+----+---+-------+
```





### 그룹별 요약

그룹별 요약을 하는데 `groupBy`를 `agg`와 함께 사용한다.

```python
df1.groupBy("country").agg({"age": "avg", "id": "count"}).show()
+-------+---------+--------+
|country|count(id)|avg(age)|
+-------+---------+--------+
|  China|        1|    23.0|
|    USA|        2|    19.0|
+-------+---------+--------+
```





### 사용자 정의함수 (UDF)

사용자 정의함수(User Defined Function)을 작성하여 표준 SQL 구문에서 제공되지 않는 연산작업을 수행시킬 수 있다.

```python
from pyspark.sql.functions import udf
upper_character = udf(lambda x: x.upper())

df1.select(upper_character(df1["name"])).show()

+--------------+
|<lambda>(name)|
+--------------+
|          ANDY|
|          JEFF|
|         JAMES|
+--------------+
```





### DataFrame Join

두개의 서로 다른 스파크 데이터프레임을 죠인(join)하는 것도 가능하다.

```python
df1.join(df2, df1["id"] == df2["c_id"]).show()

+---+----+---+-------+----+------+
| id|name|age|country|c_id|c_name|
+---+----+---+-------+----+------+
|  1|andy| 20|    USA|   1|   USA|
|  2|jeff| 23|  China|   2| China|
+---+----+---+-------+----+------+
```





#### * 참고 LIKE 구문

```
'-' : 글자숫자를 정해줌(EX 컬럼명 LIKE '홍_동')

'%' : 글자숫자를 정해주지않음(EX 컬럼명 LIKE '홍%')
```

```sql
--A로 시작하는 문자를 찾기--
SELECT 컬럼명 FROM 테이블 WHERE 컬럼명 LIKE 'A%'

--A로 끝나는 문자 찾기--
SELECT 컬럼명 FROM 테이블 WHERE 컬럼명 LIKE '%A'

--A를 포함하는 문자 찾기--
SELECT 컬럼명 FROM 테이블 WHERE 컬럼명 LIKE '%A%'

--A로 시작하는 두글자 문자 찾기--
SELECT 컬럼명 FROM 테이블 WHERE 컬럼명 LIKE 'A_'

--첫번째 문자가 'A''가 아닌 모든 문자열 찾기--
SELECT 컬럼명 FROM 테이블 WHERE 컬럼명 LIKE'[^A]'

--첫번째 문자가 'A'또는'B'또는'C'인 문자열 찾기--
SELECT 컬럼명 FROM 테이블 WHERE 컬럼명 LIKE '[ABC]'
SELECT 컬럼명 FROM 테이블 WHERE 컬럼명 LIKE '[A-C]'

```



