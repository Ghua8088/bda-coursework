pig -x local

#Create
data = load 'UR_DATA.csv' using PigStorage(',') AS (id:chararray, name:chararray, age:int, role:chararray, salary:float);

#Query
high_salary = Filter employees  salary>50000;
role_names = ForEach employees generate name,role;
group_roles = group employees by role;
role_count = foreach group_roles generate group as role,count(employees) as total_count;
Dump role_count;
Store role_count into 'outputs/role_counts' using PigStorage(',');