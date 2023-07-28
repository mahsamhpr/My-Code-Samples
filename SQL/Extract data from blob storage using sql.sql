with indata as (
    SELECT $1 as payload
    from
        @DWH.STG."raw_data_zone"/Ad-hoc/financial_dataset_dev.json
            (file_format => 'DEV_DWH.STG.DEFAULT_JSON_FORMAT')
)

select $1:"model"
from indata;

with indata as (
    SELECT $1 as payload
    from
        @DWH.STG."raw_data_zone"/Ad-hoc/ActivityEvent.json
            (file_format => 'DEV_DWH.STG.DEFAULT_JSON_FORMAT')
)
-- select * from table(flatten(input => parse_json(indata.payload))) f;

SELECT t.VALUE
from @DWH.STG."raw_data_zone"/Ad-hoc/ActivityEvent.json
         (file_format => 'DEV_DWH.STG.DEFAULT_JSON_FORMAT') as s,
     table ( flatten(s.$1) ) as t
;

-- CSV example. It struggles A LOT with special characters like ÆØÅ. So it can be improved upon.
SELECT concat($1, 'p', $2, 'rpt', $3) as agreement_code, $4 as member_code, $6 as person_status, $7 as end_date, $8 as price
from @DWH.STG."raw_data_zone"/Ad-hoc/20220401_CORPORATE.csv (file_format => DWH.STG.CSV_SEMICOLON_SKIP_HEADER) as T
;
