/*
 Generate a list with the previous 365 dates including today
 */
select dateadd(day, -1, dateadd(day, -row_number() over (order by seq4()) + 2, current_date)) as period
from table (generator(rowcount => 365))
order by period desc;

/*
 Generate a list with the last day of the previous 36 months including the current month.
 */
select last_day(dateadd(month, -row_number() over (order by seq4()) + 1, current_date)) as period
from table (generator(rowcount => 36))
order by period desc;

/*
 Generate a list with the last day of the previous 52 weeks including the current week.
 */
select last_day(dateadd(week, -row_number() over (order by seq4()) + 1, current_date), week) as period
from table (generator(rowcount => 52))
order by period desc;
