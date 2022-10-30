-- Task2 Spark SQL query

select 	D.week_num  		\
		, D.id_driver		\
		, D.name			\
		, D.tour_value	\
		, D.rating			\
		, D.row			\
from (					\
select  C.week_num		\
	, C.id_driver			\
        , C.name				\
	, C.rating			\
	, C.tour_value		\
	, row_number() over(partition by C.week_num order by C.tour_value desc, C.rating asc) row		\
from(					\
select 					\
	Z. week_num			\
	, Z.id_driver			\
	, Z.name				\
	, avg(Z.rating) rating	\
        , max(Z.tour_value) tour_value	\
from(					\
select 					\
	case when weekofyear(substr(A.date_create, 0, 10)) > 50 and substr(A.date_create, 6, 2) = '01' then 0 else weekofyear(substr(A.date_create, 0, 10)) end as week_num 	\
	, A.id_driver			\
        , A.date_create		\
	, B.name				\
        , A.rating 	\
        , A.tour_value	\
from booking A 			\
left outer join driver B 		\
on A.id_driver = B.id 		\
where substr(A.date_create, 0, 4) > '2015' ) Z 	\
group by Z.week_num, Z.id_driver, Z.name  ) C ) D 				\
where row <= 10

