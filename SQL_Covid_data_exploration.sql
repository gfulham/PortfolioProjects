-- Changing DB
USE Covid_Data

--Show available tables 
SELECT * FROM INFORMATION_SCHEMA.TABLES

--View tables 
SELECT * FROM ['owid-covid-data_deaths$'];
SELECT * FROM ['owid-covid-data_vaccinations$'];

--Starting Template Table
SELECT LOCATION, DATE, TOTAL_CASES, NEW_CASES, TOTAL_DEATHS POPULATION
FROM ['owid-covid-data_deaths$']
ORDER BY LOCATION, DATE

--Total Cases vs total deaths
SELECT LOCATION, DATE, TOTAL_CASES, TOTAL_DEATHS, ROUND((total_deaths/total_cases)*100,2) as Death_percentage
FROM ['owid-covid-data_deaths$']
ORDER BY LOCATION, DATE
 
--United States Total deaths
SELECT LOCATION, DATE, TOTAL_CASES, TOTAL_DEATHS, ROUND((total_deaths/total_cases)*100,2) as Death_percentage
FROM ['owid-covid-data_deaths$']
WHERE LOCATION like '%states%'
ORDER BY total_deaths DESC;

SELECT LOCATION, DATE, TOTAL_CASES, TOTAL_DEATHS, ROUND((total_deaths/total_cases)*100,2) as Death_percentage
FROM ['owid-covid-data_deaths$']
WHERE LOCATION like '%Canada%'
ORDER BY total_deaths DESC;

-- Country Cases Per Capita
SELECT LOCATION, POPULATION, MAX(TOTAL_CASES), ROUND((MAX(total_cases)/population)*100,2) as Cases_per_capita
FROM ['owid-covid-data_deaths$']
GROUP BY Location, population
ORDER BY Cases_per_capita DESC;

-- Country Cases Per Capita For pop > 5mil
SELECT LOCATION, population, MAX(TOTAL_CASES) as Max_Cases, ROUND(MAX((total_cases)/population)*100,1) as percent_population_infected
FROM ['owid-covid-data_deaths$']
WHERE population > 5000000
GROUP BY Location, population
ORDER BY percent_population_infected DESC;

-- Death Counts of Countries
SELECT LOCATION, MAX(total_deaths) as Max_Deaths
FROM ['owid-covid-data_deaths$']
WHERE continent is not null
GROUP BY Location, population
ORDER BY Max_Deaths DESC;

-- Australia is in continent Oceania
SELECT * FROM ['owid-covid-data_deaths$'] WHERE location = 'Australia'

-- Deaths by Continent and Total
SELECT LOCATION, MAX(total_deaths) as Max_Deaths
FROM ['owid-covid-data_deaths$']
WHERE continent is NULL 
	AND location <> 'Low income'
	AND location <> 'High income'
	AND location <> 'Middle income'
	AND location <> 'Upper middle income'
	AND location <> 'Lower middle income'
	AND location <> 'European Union'
	AND location <> 'International'
GROUP BY Location, population
ORDER BY Max_Deaths DESC;

-- Death Counts Countries Per Capita 
SELECT LOCATION, population, MAX(total_deaths) as Max_Deaths, ROUND(MAX((total_deaths)/population)*100,2) as percent_population_dead
FROM ['owid-covid-data_deaths$']
GROUP BY Location, population
ORDER BY percent_population_dead DESC;

--GOBAL VIEW Of ALL COUNTIRES CHANGING DAILY CASES AND DAILY DEATHS 
SELECT date
, SUM(new_cases) as total_cases
, sum(new_deaths) as total_deaths 
FROM ['owid-covid-data_deaths$']
GROUP BY date
ORDER BY date DESC;

--GOBAL VIEW OF RUNNING/CUMULATIVE CASES, DEATHS, KILLING RATE 
SELECT date
	, total_cases
	, total_deaths
	, ROUND((total_deaths/(NULLIF(total_cases,0))*100),4) AS Running_death_percent
    --, ROUND((SUM(new_deaths)/SUM(new_cases))*100,2) as Daily_covid_death_percentage
FROM ['owid-covid-data_deaths$']
WHERE location = 'World'
ORDER BY date DESC;

--GOBAL VIEW OF RUNNING CASES, DEATHS, DAILY KILLING RATE
SELECT date
    ,SUM(new_deaths)/(SUM(NULLIF(new_cases,0)))*100 as Daily_covid_death_percentage
FROM ['owid-covid-data_deaths$']
GROUP BY date
ORDER BY date DESC;

--JOINING VACCINATION TABLE TO VIEW WITH GLOBAL DEATHS
SELECT *
FROM ['owid-covid-data_vaccinations$'] vac 
JOIN ['owid-covid-data_deaths$'] dea
	ON dea.location = vac.location
	and dea.date = vac.date


-- Countries Population vs Daily Vaccinations
SELECT 
	dea.continent
	, dea.location
	, dea.date
	, dea.population
	, cast(vac.new_vaccinations as int) as daily_new_vaccinations
FROM ['owid-covid-data_vaccinations$'] vac 
JOIN ['owid-covid-data_deaths$'] dea
	ON dea.location = vac.location
	and dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 5 desc

-- Partition to sum daily new cases over locations
-- Finds daily vaccinations and acculmulation of each country
SELECT 
	dea.continent
	, dea.location
	, dea.date
	, dea.population
	, cast(vac.new_vaccinations as int) as daily_new_vaccinations
	,SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.Date)  as Total_vaccinations
	-- Partition will take the aggregate sum of vac.new_vaccinations and break it down by/over the dea.location
FROM ['owid-covid-data_vaccinations$'] vac 
JOIN ['owid-covid-data_deaths$'] dea
	ON dea.location = vac.location
	and dea.date = vac.date
WHERE dea.continent IS NOT NULL

--FIND VACCINATION RATES WITH CTE
--USING CTE TO PERFORM CALCULATION ON A CALCULATED CREATED COLUMN
--THIS IS A WORKAROUND SINCE ITS NOT POSSIBLE TO MAKE A CALCULATION ON A NEWLY CREATED COLUMN
--THIS INSTNACE ROLLING_VACCINATIONS WAS JUST CREATED
--WE CANNOT USE IT TO FIND COUNTRY VACCINATION RATES UNLESS USE CTE 
WITH PopvsVac (Continent, Location, Date, Population, daily_new_vaccinations, RollingVaccinations)
as
(
SELECT 
	dea.continent
	, dea.location
	, dea.date
	, dea.population
	, cast(vac.new_vaccinations as int) as daily_new_vaccinations
	,SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.Date)  as RollingVaccinations
	-- Partition will take the aggregate sum of vac.new_vaccinations and break it down by/over the dea.location
FROM ['owid-covid-data_vaccinations$'] vac 
JOIN ['owid-covid-data_deaths$'] dea
	ON dea.location = vac.location
	and dea.date = vac.date
WHERE dea.continent IS NOT NULL
)
--Here we can make the Vaccination_rate calculation
SELECT *, (RollingVaccinations/population) *100 as Vaccination_rate
FROM PopvsVac



--FIND VACCINATION RATES BY CREATING NEW TABLE
--TEMP TABLE
DROP Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingVaccinations numeric
)

Insert into #PercentPopulationVaccinated
SELECT 
	dea.continent
	, dea.location
	, dea.date
	, dea.population
	, cast(vac.new_vaccinations as int) as daily_new_vaccinations
	,SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.Date)  as RollingVaccinations
	-- Partition will take the aggregate sum of vac.new_vaccinations and break it down by/over the dea.location
FROM ['owid-covid-data_vaccinations$'] vac 
JOIN ['owid-covid-data_deaths$'] dea
	ON dea.location = vac.location
	and dea.date = vac.date
WHERE dea.continent IS NOT NULL

Select *, (RollingVaccinations/Population)*100
From #PercentPopulationVaccinated

--CREATE VIEWS FOR VISUALIZATIONS LATER
CREATE VIEW Percent_Population_Vaccinated as
SELECT 
	dea.continent
	, dea.location
	, dea.date
	, dea.population
	, cast(vac.new_vaccinations as int) as daily_new_vaccinations
	,SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.Date)  as Total_vaccinations
	-- Partition will take the aggregate sum of vac.new_vaccinations and break it down by/over the dea.location
FROM ['owid-covid-data_vaccinations$'] vac 
JOIN ['owid-covid-data_deaths$'] dea
	ON dea.location = vac.location
	and dea.date = vac.date
WHERE dea.continent IS NOT NULL