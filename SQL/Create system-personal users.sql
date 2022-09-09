use role useradmin;
CREATE USER SYS_SATS_GX_LOAD_FACTOR
  PASSWORD = 'V68TIGYR'
  DISPLAY_NAME = 'GX Load FactorInitiative'
  DEFAULT_WAREHOUSE = 'ANALYST_SMALL'
;

create role SATS_GX_LOAD_FACTOR_ROLE

drop role SATS_GX_LOAD_FACTOR

use role SECURITYADMIN;
grant role SATS_GX_LOAD_FACTOR_ROLE to user damon_chan;
alter user SYS_SATS_GX_LOAD_FACTOR set DEFAULT_ROLE=SATS_GX_LOAD_FACTOR_ROLE;

grant select on table DM.SANDBOX.DIM_MEMBER_DETAILS to role SATS_GX_LOAD_FACTOR_ROLE


use role SECURITYADMIN;
grant usage on database DM to role SATS_DQ_ROLE
grant usage on schema DATA_QUALITY to role SATS_DQ_ROLE

grant select on table DM.DATA_QUALITY.DIM_CENTER to role SATS_DQ_ROLE

--grant operate to a warehouse to start and stop it
use role SECURITYADMIN;
grant operate on warehouse ANALYST_SMALL to role SATS_DQ_ROLE;
grant usage on warehouse ANALYST_SMALL to role SATS_DQ_ROLE;
---------------------------------Personal user --------------------------------------------------------
use role useradmin;
CREATE USER larsen_benjamin
  PASSWORD = 'MP6Jhs'
  LOGIN_NAME = 'ext.benjamin.larsen@sats.no'
  DISPLAY_NAME = 'larsen benjamin'
  DEFAULT_WAREHOUSE = 'ANALYST_SMALL'
;
use role USERADMIN
create role dbt_tester
grant role SATS_DQ_ROLE to user mangmisi_marcus;
--drop user mangmisi_marcus

use role useradmin;
USE DATABASE DM;
 create schema DATA_QUALITY;

create or replace view DM.DATA_QUALITY.DIM_CENTER as (select "center_id","person_id", "check_in_datetime", "check_out_datetime","result","card_checked_in" from DWH.PSA.EXERP_SA_VISIT);

grant select on table DM.DATA_QUALITY.DIM_CENTER to role SATS_DQ_ROLE
----------------------
