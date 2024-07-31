-- Count the number of Atheletes from each county
--SELECT count(PersonName) as num_of_atheletes,Country from atheletes group by Country;

-- Count Total Medals won  by each country
--select TeamCountry,Total from medals order by total desc;

--Count the average number of entries by gender for eac discipline
select Discipline, avg(Male) as avg_ml, AVG(Female) as fml
from gender  group by Discipline;


-- 


---SELECT TOP (100) [PersonName]
--,[Country]
--,[Discipline]
-- FROM [olympicdb].[dbo].[atheletes]