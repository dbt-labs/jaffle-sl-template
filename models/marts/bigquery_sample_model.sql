WITH
  Sequences AS (
    SELECT [0, 1, 1, 2, 3, 5] AS some_numbers UNION ALL
    SELECT [2, 4, 8, 16, 32] UNION ALL
    SELECT [5, 10]
  )
SELECT 1 as id,
        '2024-01-01' as date_day,
        * 
 FROM Sequences