/* Getting trends from all states */
SELECT DateRecorded, State, Positive, Negative, Total, Death 
FROM USCASEBYSTATE
ORDER BY State;

/* Looking at tests vs med aid; we can compare state totals, but it will not be correlation it can show the trends though! */
SELECT u.DateRecorded, u.State, u.Positive, u.Negative, u.Total AS test_total, u.Death, ma.Deliveries, ma.Cost, ma.Weight
FROM USCASEBYSTATE u, USMEDAIDAGGREGATE ma
WHERE u.State = ma.State AND u.DateRecorded = ma.DateRecorded 
ORDER BY u.State, u.DateRecorded;

/* Getting Population Data joined with Testing Data */
SELECT * FROM (
SELECT FAC.State, FAC.DateRecorded, sum(FAC.Population20Plus) AS over20Pop, sum(FAC.Population65Plus) AS over65Pop, sum(Population) as total_pop, US.Positive, US.Negative, US.Total, US.Death
FROM USFACILITIES FAC, USCASEBYSTATE US
WHERE FAC.State = US.State AND FAC.DateRecorded = US.DateRecorded
GROUP BY FAC.State, FAC.DateRecorded, US.Positive, US.Negative, US.Total, US.Death
ORDER BY FAC.State, FAC.DateRecorded
) where daterecorded IN(select max(daterecorded) from USCASebystate)
ORDER BY state;

/* getting ICU occupancy rate, population and death rates*/
select f.state, f.daterecorded, total_pop, icu_avg, j.death_tot from(select f.state, f.daterecorded, sum(population) as total_pop from USFACILITIES f where  daterecorded=to_date('05/09/2020','MM/DD/YYYY')  group by state, daterecorded) f , (select o.state, o.daterecorded, round(avg(icubedoccupancyrate),3) as icu_avg from usicubedoccupancy o where o.daterecorded=to_date('05/09/2020','MM/DD/YYYY')  group by o.state, daterecorded) o, (select sum(deaths) as death_tot, j.state, j.daterecorded from jhudata j where daterecorded=to_date('05/09/2020','MM/DD/YYYY')   group by state, daterecorded) j where f.state=o.state and j.state=o.state;


