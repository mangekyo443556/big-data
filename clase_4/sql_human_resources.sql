SELECT
    COUNTRY_ID,
    COUNTRY_NAME
FROM
    HR.COUNTRIES;


SELECT COUNT(COUNTRY_NAME), COUNT(DISTINCT COUNTRY_NAME)
FROM HR.COUNTRIES;

SELECT
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    MANAGER_ID,
    LOCATION_ID
FROM
    HR.DEPARTMENTS;

select * from hr.COUNTRIES;

select * from hr.departments;

select * from hr.EMPLOYEES;

select * from hr.JOBS;

select * from HR.LOCATIONS -- C

select * from hr.REGIONS

-- ejemplo 1. verirficando que no hay duplicados en la tabla de paises

select count(*), count(country_id), count(distinct country_id )
from hr.COUNTRIES;

select * from hr.departments;

-- ejemplo 2. Salario promedio por area

SELECT a.* 
from hr.EMPLOYEES a
where salary >10000
order by salary desc


SELECT
    DEPARTMENT_NAME departamento,
    round(AVG(SALARY),0) as salario_promedio,
    max(SALARY) as salario_maximo,
    COUNT(DISTINCT EMPLOYEE_ID) empleados_unicos
FROM
    (
-- tabla cruzada
        SELECT
            A.*,
            B.DEPARTMENT_NAME
        FROM
                 HR.EMPLOYEES A
            INNER JOIN HR.DEPARTMENTS B ON A.DEPARTMENT_ID = B.DEPARTMENT_ID
    )
GROUP BY
    DEPARTMENT_NAME
order by 3 desc


SELECT
    DEPARTMENT_NAME departamento,
    round(AVG(SALARY),0) as salario_promedio,
    max(SALARY) as salario_maximo,
    COUNT(DISTINCT EMPLOYEE_ID) empleados_unicos
FROM
    (
-- tabla cruzada
        SELECT
            A.*,
            B.DEPARTMENT_NAME
        FROM
                 HR.EMPLOYEES A
            LEFT JOIN HR.DEPARTMENTS B ON A.DEPARTMENT_ID = B.DEPARTMENT_ID
        WHERE DEPARTMENT_NAME IS NOT NULL
    )
GROUP BY
    DEPARTMENT_NAME
order by 3 desc

-- ejemplo 3. Salario promedio por ciudad
SELECT
    CITY as ciudad,
    round(AVG(SALARY),0) as salario_promedio,
    COUNT(DISTINCT EMPLOYEE_ID) empleados_unicos
FROM
    (
        SELECT
            A.*,
            C.CITY
        FROM
                 HR.EMPLOYEES A
            INNER JOIN HR.DEPARTMENTS B ON A.DEPARTMENT_ID = B.DEPARTMENT_ID
            INNER JOIN HR.LOCATIONS   C ON B.LOCATION_ID = C.LOCATION_ID
    )
GROUP BY
    CITY
order by 2 desc

select DEPARTMENT_NAME, rango_salarial, avg(salary), count(*) from (
WITH EMPLEADOS AS (
    SELECT
        A.*,
        CASE
            WHEN ( SALARY < 5000
                   AND SALARY > 0 ) THEN
                'Bs 0 a 4999'
            WHEN ( SALARY >= 5000
                   AND SALARY < 10000 ) THEN
                'Bs 5000 a 9999'
            WHEN SALARY >= 10000 THEN
                'Mas de 10000'
            ELSE
                'error'
        END AS RANGO_SALARIAL
    FROM
        HR.EMPLOYEES A
), DEPARATAMENTO AS (
    SELECT
        DEPARTMENT_ID,
        DEPARTMENT_NAME
    FROM
        HR.DEPARTMENTS
)
SELECT
    A.DEPARTMENT_ID,
    A.SALARY,
    A.RANGO_SALARIAL,
    B.DEPARTMENT_NAME
FROM
    EMPLEADOS A
INNER JOIN DEPARATAMENTO B ON A.DEPARTMENT_ID = B.DEPARTMENT_ID
)
group by DEPARTMENT_NAME, rango_salarial
order by 1 desc 