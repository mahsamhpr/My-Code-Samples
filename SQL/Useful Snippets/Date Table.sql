with base_dates as
         /*
          Generates a list of 10000 dates from today and backwards in time.
          */
         (select dateadd(day, -1, dateadd(day, -row_number() over (order by seq4()) + 2, current_date)) as period
          from table (generator(rowcount => 10000))
          order by period desc),
     date_range as
         /*
          Move all dates 3 years into the future.
          */
         (select dateadd(year, 3, period) as nk_date
          from base_dates),
     basic_calculations as
         /*
          Add basic date attributes.
          */
         (SELECT nk_date,
                 cast(replace(nk_date, '-', '') as number(38, 0))                     AS DATE_NUMBER,
                 DATE_PART('DAY', nk_date)                                            AS DAY_MONTH_NUMBER,
                 WEEK(nk_date)                                                        AS WEEK_NUMBER,
                 DATE_PART('MONTH', nk_date)                                          AS MONTH_NUMBER,
                 DATE_PART('YEAR', nk_date)                                           AS YEAR_NUMBER,
                 DATE_PART('QUARTER', nk_date)                                        AS QUARTER_NUMBER,
                 dayofweekiso(nk_date)                                                AS DAY_OF_WEEK,
                 yearofweekiso(nk_date)                                               AS YEAR_OF_WEEK,
                 date_from_parts(year(nk_date), month(nk_date), '01')                 AS FIRST_DAY_OF_MONTH,
                 LAST_DAY(nk_date)                                                    AS LAST_DAY_OF_MONTH,
                 iff(dayofweekiso(nk_date) = 1, nk_date, previous_day(nk_date, 'mo')) AS FIRST_DAY_OF_WEEK,
                 LAST_DAY(nk_date, 'week')                                            AS LAST_DAY_OF_WEEK,
                 WEEKOFYEAR(nk_date)                                                  AS WEEK_OF_YEAR,
                 weekiso(nk_date)                                                     AS WEEK_ISO,
                 datediff('day', FIRST_DAY_OF_MONTH, LAST_DAY_OF_MONTH) + 1           AS DAYS_IN_MONTH,
                 datediff('day', FIRST_DAY_OF_WEEK, LAST_DAY_OF_WEEK) + 1             AS DAYS_IN_WEEK,
                 YEAR_NUMBER * 100 + MONTH_NUMBER                                     AS YEAR_MONTH_NUMBER,
                 cast(concat(YEAR_NUMBER, '-', MONTH_NUMBER) as varchar(7))           AS YEAR_MONTH_NAME,
                 datediff('day', current_date(), nk_date) * -1                        AS relative_day_position,
                 datediff('month', current_date(), nk_date) * -1                      AS relative_month_position,
                 datediff('quarter', current_date(), nk_date) * -1                    AS relative_quarter_position,
                 datediff('year', current_date(), nk_date) * -1                       AS relative_year_position,
                 datediff('week', current_date(), nk_date) * -1                       AS relative_week_position,
                 TO_CHAR(YEAR_NUMBER)                                                 AS YEAR_NAME,
                 'W ' || TO_CHAR(WEEK_ISO)                                            AS WEEK_NAME,
                 YEAR_OF_WEEK || '-' || WEEK_ISO                                      AS YEAR_WEEK_NAME,
                 'Q' || TO_CHAR(QUARTER_NUMBER)                                       AS QUARTER_NAME,
                 ('Q' || TO_CHAR(QUARTER_NUMBER)) || RIGHT((TO_CHAR(YEAR_NUMBER)), 2) AS YEAR_QUARTER_NAME
          FROM date_range),
     holiday_calculations as
         /*
          Add Easter calculations.
          Add other holiday calculations.
          */
         (select nk_date,
                 FLOOR(YEAR_NUMBER / 100)                                                                                                                                                        AS FIRSTDIG,
                 MOD(YEAR_NUMBER, 19)                                                                                                                                                            AS REMAIN19,
                 MOD(FLOOR((FIRSTDIG - 15) / 2) + 202 - 11 * REMAIN19 + IFF(FIRSTDIG in (21, 24, 25, 27, 28, 29, 30, 31, 32, 34, 35, 38), -1, IFF(FIRSTDIG in (33, 36, 37, 39, 40), -2, 0)), 30) AS TEMP1,
                 TEMP1 + 21 + IFF(TEMP1 = 29 OR (TEMP1 = 28 AND REMAIN19 > 10), -1, 0)                                                                                                           AS TA,
                 MOD(TA - 19, 7)                                                                                                                                                                 AS TB,
                 MOD(40 - FIRSTDIG, 4)                                                                                                                                                           AS TCPRE,
                 TCPRE + IFF(TCPRE = 3, 1, 0) + IFF(TCPRE > 1, 1, 0)                                                                                                                             AS TC,
                 MOD(YEAR_NUMBER, 100)                                                                                                                                                           AS TEMP2,
                 MOD(TEMP2 + FLOOR(TEMP2 / 4), 7)                                                                                                                                                AS TD,
                 MOD(20 - TB - TC - TD, 7) + 1                                                                                                                                                   AS TE,
                 TA + TE                                                                                                                                                                         AS D,
                 IFF(D > 31, D - 31, D)                                                                                                                                                          AS EASTERDAY,
                 IFF(D > 31, 4, 3)                                                                                                                                                               AS EASTERMONTH,
                 DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY)                                                                                                                            AS EASTER_SUNDAY,
                 case
                     when nk_date = (DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY)) then 'Easter Sunday'
                     when nk_date = dateadd('day', 1, (DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY))) then 'Easter Monday'
                     when nk_date = dateadd('day', -7, (DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY))) then 'Palm Sunday'
                     when nk_date = dateadd('day', -3, (DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY))) then 'Maundy Thursday'
                     when nk_date = dateadd('day', -2, (DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY))) then 'Good Friday'
                     when nk_date = dateadd('day', 39, (DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY))) then 'Ascension Day'
                     when nk_date = dateadd('day', 49, (DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY))) then 'Whit Sunday'
                     when nk_date = dateadd('day', 50, (DATE_FROM_PARTS(YEAR_NUMBER, EASTERMONTH, EASTERDAY))) then 'Whit Monday'
                     when MONTH_NUMBER = 1 and DAY_MONTH_NUMBER = 1 then 'New Year\'s day'
                     when MONTH_NUMBER = 12 and DAY_MONTH_NUMBER = 31 then 'New Year\'s eve'
                     when MONTH_NUMBER = 5 and DAY_MONTH_NUMBER = 1 then 'Labour day'
                     when MONTH_NUMBER = 5 and DAY_MONTH_NUMBER = 17 then 'Constitution day'
                     when MONTH_NUMBER = 12 and DAY_MONTH_NUMBER = 24 then 'Christmas eve'
                     when MONTH_NUMBER = 12 and DAY_MONTH_NUMBER = 25 then 'Christmas day'
                     when MONTH_NUMBER = 12 and DAY_MONTH_NUMBER = 26 then 'Boxing day'
                     end                                                                                                                                                                         AS HOLIDAY,
                 case
                     when HOLIDAY is not null then 0
                     when DAY_OF_WEEK <= 5 then 1
                     else 0
                     end                                                                                                                                                                         as is_working_day,
                 SUM(is_working_day) OVER (PARTITION BY YEAR_MONTH_NUMBER ORDER BY nk_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                                                     AS working_day_number
          from basic_calculations),
     holidays as (select nk_date, HOLIDAY, is_working_day, working_day_number
                  from holiday_calculations)
select basic_calculations.*,
       holidays.HOLIDAY,
       holidays.is_working_day,
       holidays.working_day_number,
       case
           when date_trunc(month, basic_calculations.nk_date) = date_trunc(month, current_date) then 'Current month'
           when date_trunc(month, basic_calculations.nk_date) = date_trunc(month, dateadd(month, -1, current_date)) then 'Last month'
           else YEAR_MONTH_NAME
           end as reporting_month_name,
       case
           when basic_calculations.nk_date = current_date then 'Today'
           when basic_calculations.nk_date = current_date - 1 then 'Yesterday'
           else to_varchar(basic_calculations.nk_date)
           end as reporting_date
from basic_calculations
         left join holidays on basic_calculations.nk_date = holidays.nk_date
order by nk_date
;
