with cte as(
SELECT
   date_part(year, current_date ) - date_part(year, birth_date) age,
   date_part(year, current_date) - date_part(year,hire_date) lenght_of_service,
   first_name ||' '|| last_name AS fullname,
   *
FROM
    {{source("sources","employees")}})
SELECT
    *
FROM
    cte