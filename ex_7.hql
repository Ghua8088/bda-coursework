# for running cli use hive command

show databases;
use YOUR_DATABASE;
show tables;

#Create
create table employees(id string,name string,age int,role string,salary float) row format delimited fields terminated by ",";
insert into table employees values('101','raghu',21,"sde",88000);
load data local inpath 'UR_DATA.csv' into table employees;

#Read
select * from employees;
select * from employees where salary>50000;
select role,count(*) from employees group by role having avg(salary)>50000;
