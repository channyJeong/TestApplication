-- Task4 Spark SQL Query

1. 월별 예약 수치
spark.sql(									\
"select 	A.ym								\
		, count(*) booking_count_per_month 	\
from (										\
select substr(date_create, 0, 7) ym 				\
from booking 									\
where substr(date_create, 0, 4) = '2016') A 		\
group by A.ym order by A.ym").show()			

2. 월별 평균 운전자 평가 
spark.sql(										\
"select 	A.ym, avg(rating) avg_rating_per_month 	\
from (											\
select 	substr(date_create, 0, 7) ym				\
		, id_driver								\
		, avg(rating) rating 						\
from booking 										\
where substr(date_create, 0, 4) = '2016' 				\
group by substr(date_create, 0, 7), id_driver) A 		\
group by A.ym 									\
order by A.ym").show()							

3. 전체 승객 별 월별 승객 이용률
spark.sql(										\
"select 	A.ym									\
		, count(id_passenger)*0.1 passenger_utilization_per_month 	\
from (											\
select 	substr(date_create, 0, 7) ym				\
		, id_passenger 							\
from booking 										\
where substr(date_create, 0, 4) = '2016' 				\
group by substr(date_create, 0, 7), id_passenger) A 	\
group by A.ym 									\
order by A.ym").show()							

4. 월별 자주 이용하는 시간대(top 10)
spark.sql(										\
"select 	B.ym									\
		, B.hour									\
		, B.call_count								\
		, B.row 									\
from (											\
select 	A.ym									\
		, A.hour									\
		, A.call_count								\
		, row_number() over(partition by A.ym order by A.call_count desc) row 	\
from (											\
select 	substring(date_create, 0, 7) ym				\
		, substring(date_create, 12, 2) hour			\
		, count(id_passenger) call_count 			\
from booking 										\
where substr(date_create, 0, 4) = '2016' 				\
group by substring(date_create, 0, 7), substring(date_create, 12, 2)) A) B 	\
where B.row <= 10").show()	
