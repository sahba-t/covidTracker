/*some queries to see what is where!*/
/*total is some of +/- so this is where we get our total tests*/
select * from uscasebystate where ROWNUM <= 5;

/*I think this will be pretty much useless :) */
select * from USTESTS where ROWNUM <= 5;

select * from ventilator where ROWNUM <= 5 order by state;

/*For some states data does not change for some data changes a lot*/
select * from usmedaidaggregate order by state;

/*a lot of things do not change so we will not have a trend!*/
select * from USICUBEDOCCUPANCY order by countyname, daterecorded;

select daterecorded, state, ROUND(AVG(icubedoccupancyrate),2) as icu_occ_avg
from USICUBEDOCCUPANCY
group by state, daterecorded order by state, daterecorded;

/*-----------------------------------------------------------------------*/


/*getting trends for some of the sates*/
select daterecorded, state, positive, death from uscasebystate where state IN ('NM', 'CA', 'NY', 'GA') order by state;



/* combining ventilator data with test results a lot of missing data!*/
select s.daterecorded, s.state, positive, death, onventilatorcumulative as vent_cum, onventilatorcurrently as vent_curr
from uscasebystate s, ventilator v
where  s.state=v.state and s.daterecorded=v.daterecorded
order by s.state;


/*getting tests total for states*/
select state, total, daterecorded from uscasebystate order by state;

/*-----------------------------------------------------------------------*/


/*Looking at some of correlations*/

/*IT might be interesting to look at death rate and 65th population*/
/*remember "correlation does not imply causation"*/

/*!!!!!This particular one may not be needed everyday!!!!!? We are not going to gain population 65+ everyday!; how about beds and stuff?*/
select state, daterecorded, sum(population65plus) as over65Pop from USFACILITIES group by state, daterecorded order by state, daterecorded;

/*same with staffed beds and stuff*/
select state, daterecorded, sum(staffedallbeds) as staffed_beds, sum(staffedICUbeds) as staffed_icu, 
sum(licensedallbeds) as licensed_beds from USFACILITIES group by state, daterecorded order by state, daterecorded;


/*!!!! Sahba sees potential here!!!!*/
/*looking at tests vs med aid; we can compare state totals, but it will not be correlation it can show the trends though!*/
select u.daterecorded, u.state, u.total as test_total, ma.deliveries, ma.cost, ma.weight
from uscasebystate u, usmedaidaggregate ma
where u.state = ma.state and u.daterecorded = ma.daterecorded order by u.state, u.daterecorded;