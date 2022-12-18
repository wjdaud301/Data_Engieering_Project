(4) 서울시 지하철 승하차 인원 

1. 분석이유:

   코로나로 인하여 사회적거리두기가 시행되고 있지만 출퇴근 혼잡시간에는 대중교통에 많은 사람이 

   몰려있다. 따라서 모니터링을 통해 사람들이 대중교통에서 어느시간에 어디구간이 많이 몰리는지 파악하고

   하고 피할 수 있는 대안을 마련하고자한다.

2. 분석목적 : 
   1. 코로나 전후 역 별 이용자 수 데이터를 비교하여 코로나 이후 달라지고 있는 이동패턴에 대해 분석
   2. 어느 지하철 노선에서 가장 많이 타고 많이 내리는지 분석
   3.  실시간으로 모니터링하는 대쉬보드를 만들어  코로나 19에 대한 사람들의 우려 수준을 
      가늠해 보는데 활용

3. 파이프라인:

   1. 데이터 소스

      1. 외부데이터 : xml, json (비정형데이터)

   2. 수집 (jupyter notebook) :

      1. 서울시 지하철호선별 역별 승하차 인원정보 : http://data.seoul.go.kr/dataList/OA-12914/S/1/datasetView.do -> openAPI(일단위) 갱신날짜: 3일전
      2.  필요시 -> 크롤링

   3. 저장: (aws)Nosql(mongodb) 데이터레이크   pymongo

      ​			-> spark in aws(pyspark)

      ​			-> RDBMS in aws(mysql) 데이터웨어하우스

      ​			-> 로컬(mysql workbench)에서 접근

   4. 처리: pyspark (json -> csv -> aws RDS -> workbench연결 )

      ​		->  로컬 mysql과 django DB랑 연결

      ​		-> DB에서 데이터를 javascript하고 연결해서 웹시각화

   5. 시각화: 지하철 노선도 시각화, 추세 시각화 (googlemap API ,plotly)

   6. 대쉬보드 : wep으로 하루마다 대쉬보드 갱신

   

4. 사용툴

   1. aws

   2. jupyter notebook

   3. spark

   4. aws RDS

   5. mongodb

   6. mysql

   7. django(선택)




데이터 파이프라인 : https://pearlluck.tistory.com/264

파이프라인 예시 : https://qkqhxla1.tistory.com/1115 

open api로 받아온 Json파일을 mongodb에 저장하기 : https://e-hyun.tistory.com/3

​																							https://data-yul.postype.com/post/7091616

aws ec2 인스턴스에서 mysql 세팅하기 : https://devkingdom.tistory.com/84

​																	https://leveloper.tistory.com/18

