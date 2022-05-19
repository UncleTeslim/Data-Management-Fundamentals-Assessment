USE `pollution-db2`;

SELECT `Date Time`, `Location`, `NOx`
FROM `readings`
WHERE YEAR(`Date Time`)=2019
ORDER BY NOx DESC LIMIT 1