COPY INTO ${ve_DB_DWH}.PSA.SHP_FORECASTS FROM (
	select
		try_parse_json($1):"center_id"::varchar as CENTER_ID,
  		try_parse_json($1):"center_name"::varchar as CENTER_NAME,
  		try_parse_json($1):"country_code"::varchar as COUNTRY_CODE,
  		try_parse_json($1):"business_unit"::varchar as BUSINESS_UNIT,
  		try_parse_json($1):"unique_id"::varchar as UNIQUE_ID,
  		try_parse_json($1):"currency"::varchar as CURRENCY,
  		try_parse_json($1):"currency_rate"::varchar as CURRENCY_RATE,
  		try_parse_json($1):"account"::varchar as ACCOUNT,
  		try_parse_json($1):"club_oh"::varchar as CLUB_OH,
  		try_parse_json($1):"forecast_file"::varchar as FORECAST_FILE,
  		try_parse_json($1):"current_fc"::varchar as CURRENT_FC,
  		try_parse_json($1):"period"::varchar as PERIOD,
  		try_parse_json($1):"metric_name"::varchar as METRIC_NAME,
  		try_cast(try_parse_json($1):"value"::varchar as numeric(15,5)) as VALUE,
  		try_parse_json($1):"extract_date"::VARCHAR as EXTRACT_DATE,
  		try_parse_json($1)::variant as PAYLOAD,
     	metadata$filename as ETL_METADATA_FILENAME,        -- which filename we read from
     	metadata$file_row_number AS ETL_METADATA_FILE_ROW_NUMBER, -- row number in file
     	current_timestamp() as ETL_CREATEDATETIME

  from @${ve_DB_DWH}.stg."raw_data_zone"/${vj_dlPathPrefix}/ ------------------------------=====-vj_dlPathPrefix = SharePoint/Forecasts
  (file_format => '${ve_DB_DWH}.STG.default_json_format', pattern => '.*\.json')
);

