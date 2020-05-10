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
SELECT FAC.State, FAC.DateRecorded, sum(FAC.Population20Plus) AS over20Pop, sum(FAC.Population65Plus) AS over65Pop, sum(Population), US.Positive, US.Negative, US.Total, US.Death
FROM USFACILITIES FAC, USCASEBYSTATE US
WHERE FAC.State = US.State AND FAC.DateRecorded = US.DateRecorded
GROUP BY FAC.State, FAC.DateRecorded, US.Positive, US.Negative, US.Total, US.Death
ORDER BY FAC.State, FAC.DateRecorded;
