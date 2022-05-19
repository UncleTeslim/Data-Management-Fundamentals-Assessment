USE `pollution-db2`;

SELECT AVG(`PM2.5`) AS `Mean PM2.5`, AVG(`VPM2.5`) AS `Mean VPM2.5`, Location
FROM `readings`
WHERE YEAR(`Date Time`)=2019 
AND (TIME(`Date Time`) = '08:00:00' OR (TIME(`Date Time`) > '06:00:00' AND TIME(`Date Time`) < '10:00:00'))
GROUP BY Location