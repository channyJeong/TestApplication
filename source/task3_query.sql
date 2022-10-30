-- Task3 Spark SQL Query

select 	B.id_passenger	\
		, B.id_driver		\
		, B.call_count		\
		, B.row 			\
from (					\
select 	A.id_passenger	\
		, A.id_driver		\
		, A.call_count		\
		, row_number() over(partition by A.id_passenger order by A.call_count desc) row \
from (					\
select id_passenger		\
, id_driver				\
, count(*) call_count 		\
from booking 				\
group by id_passenger, id_driver) A ) B \
where B.row <= 10
