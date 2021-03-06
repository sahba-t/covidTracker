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
) where DateRecorded IN(select max(DateRecorded) from USCASEBYSTATE)
ORDER BY state;

/* Getting ICU occupancy rate, population and death rates */
SELECT f.State, f.DateRecorded, total_pop, icu_avg, j.death_tot 
FROM(
    SELECT f.State, f.DateRecorded, sum(Population) AS total_pop 
    FROM USFACILITIES f 
    WHERE  DateRecorded=to_date('05/13/2020','MM/DD/YYYY')  
    GROUP BY State, DateRecorded
    ) f , 
    (SELECT o.State, o.DateRecorded, round(avg(ICUBedOccupancyRate),3) AS icu_avg 
    FROM USICUBEDOCCUPANCY o 
    WHERE o.DateRecorded=to_date('05/13/2020','MM/DD/YYYY')  
    GROUP BY o.State, DateRecorded
    ) o, 
    (SELECT sum(Deaths) AS death_tot, j.State, j.DateRecorded 
    FROM JHUDATA j 
    WHERE DateRecorded=to_date('05/13/2020','MM/DD/YYYY')   
    GROUP BY State, DateRecorded) j 
WHERE f.State=o.State AND j.State=o.State;


