/*
 PURPOSE
 I need to find a query I did earlier.
 */
select *
from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where USER_NAME = 'BAKKE'                                     -- This one should be obvious. Use "SHOW USERS" if you're not sure.
  and QUERY_TEXT not in ('SELECT ''keep alive''', 'select 1') -- The first one removes the "Keep alive" stuff from DataGrip. And the second one is useless as well.
  and QUERY_TYPE = 'SELECT'                                   -- This is usually what I'm looking for. Not schema refreshes or other such things.
order by END_TIME desc -- You can of course filter on spesific timestamps as well.
