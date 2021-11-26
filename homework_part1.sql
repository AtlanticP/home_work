-- 1. Вывести отсортированный по количеству перелетов (по убыванию) и имени (по возрастанию) список пассажиров, совершивших хотя бы 1 полет.
SELECT name, COUNT(*) AS count FROM Pass_in_trip pit 
JOIN Passenger p ON pit.passenger = p.id
GROUP BY p.id, name
HAVING COUNT(*) > 0
ORDER BY COUNT(*) DESC, name; 

-- 2. Сколько времени обучающийся будет находиться в школе, учась со 2-го по 4-ый уч. предмет ?
SELECT TIMEDIFF(
    (SELECT end_pair FROM Timepair WHERE id=4),
    (SELECT start_pair FROM Timepair WHERE id=2)        
) AS time

-- 3. Выведите список комнат, которые были зарезервированы в течение 12 недели 2020 года.
SELECT DISTINCT * FROM Rooms
WHERE id in (
    SELECT room_id AS Rooms FROM Reservations 
    WHERE WEEK(start_date, 1) = 12 
          AND YEAR(start_date) = 2020

-- 4. Какой(ие) кабинет(ы) пользуются самым большим спросом?
SELECT classroom FROM Schedule
GROUP BY classroom
HAVING COUNT(classroom) = (SELECT MAX(cnt) FROM (
            SELECT classroom, COUNT(classroom) as cnt FROM Schedule
            GROUP BY classroom
            ORDER BY cnt DESC
        ) as t
    )          
)

SELECT classroom FROM Schedule 
GROUP BY classroom 
HAVING count(*) = (
    SELECT COUNT(*) FROM Schedule
    GROUP BY classroom
    ORDER BY COUNT(*) DESC 
    LIMIT 1
)


--5 Для каждой пары последовательных дат, dt1 и dt2, поступления средств (таблица Income_o) найти сумму выдачи денег (таблица Outcome_o) в полуоткрытом интервале (dt1, dt2]
--MYSQL
WITH cte AS (
    SELECT 
        LEAD(SUM(IF(oo.out IS NULL, 0, oo.out)), 1) OVER (ORDER BY io.date) as qty,
        io.date as dt1,
        LEAD (io.date, 1) OVER (ORDER BY io.date) as dt2
    FROM Income_o io
    LEFT JOIN Outcome_o oo ON io.date = oo.date
    GROUP BY io.date
)
SELECT qty, dt1, dt2 FROM cte
WHERE dt2 IS NOT NULL

--MSSQL
SELECT qty, dt1, dt2 FROM (
    SELECT
        LEAD(SUM(IIF(t1.out is NULL, 0, t1.out)), 1) OVER (ORDER BY io.date) as qty,
        io.date as dt1,
        LEAD(io.date, 1) OVER (ORDER BY io.date) as dt2
    FROM Income_o io 
    -- в задаче требовалось использовать OUTER APPLY
    OUTER APPLY (
        SELECT out FROM Outcome_o oo WHERE io.date = oo.date
    ) as t1
    GROUP BY io.date
) as t2
WHERE dt2 IS NOT NULL

-- 6 Cоставить отчет о битвах кораблей в два суперстолбца.
-- 2 решения. Проверить второе не представляется возможным.
WITH cte1 as (
    SELECT
        ROW_NUMBER () OVER (ORDER BY date, name) as rn,
        name,
        date,
        NTILE (2) OVER (ORDER BY date, name) as gr
    FROM Battles b 
), cte2 as (
    SELECT 
        *,
        ROW_NUMBER () OVER (PARTITION BY gr ORDER BY date, name) as rn2
    FROM cte1
)
SELECT
    MIN(IF(gr=1, rn, NULL)) as rn_1,
    MIN(IF(gr=1, name, NULL)) as name_1,
    MIN(IF(gr=1, date, NULL)) as date_1,
    MIN(IF(gr=2, rn, NULL)) as rn_2,
    MIN(IF(gr=2, name, NULL)) as name_2,
    MIN(IF(gr=2, date, NULL)) as date_2
FROM cte2
GROUP BY rn2

-- В MYSQL 8 работает.
-- sql-exe.ru: Error. 'CEIL' is not a recognized built-in function name.
WITH cte as (
    SELECT
        ROW_NUMBER () OVER (ORDER BY date, name) as rn,
        name,
        date
    FROM Battles b
), cte2 as (
    SELECT
        rn as rn_1,
        name as name_1,
        date as date_1,
        LEAD (rn, 3) OVER (ORDER BY date, name) as rn_2,
        LEAD (name, 3) OVER (ORDER BY date, name) as name_2,
        LEAD (date, 3) OVER (ORDER BY date, name) as date_2,
        RANK () OVER (ORDER BY date, name) as rnk
    FROM cte
)
SELECT 
    *   
FROM cte2
WHERE rnk <= (SELECT CEIL(COUNT(*)/2) FROM Battles b)

-- Число 3 (число строк) в LEAD необходимо вводить вручную. Способа решить динамически не нашел, чтоб mysql самостоятельно вычислял.
WITH cte as (
    SELECT
        ROW_NUMBER () OVER (ORDER BY date, name) as rn,
        name,
        date
    FROM Battles b
), cte2 as (
    SELECT
        rn as rn_1,
        name as name_1,
        date as date_1,
        LEAD (rn, 
            -- You have an error in your SQL syntax;
            (SELECT FLOOR(COUNT(*)/2) as nmb FROM Battles b)
            --        
        ) OVER (ORDER BY date, name) as rn_2,
        LEAD (name, 3) OVER (ORDER BY date, name) as name_2,
        LEAD (date, 3) OVER (ORDER BY date, name) as date_2,
        RANK () OVER (ORDER BY date, name) as rnk
    FROM cte
)