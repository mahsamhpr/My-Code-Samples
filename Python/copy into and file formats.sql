-------Avro-----------------------------------------------------------------------------

copy into ${ve_DB_DWH}.PSA.SP_APP_MEMBER_EVENTS from (
    select 
  -- The iff condition is due to a change in date format from ServicePlatform team side.
  iff(
                try_parse_json(hex_decode_string($1:"Body")):"UtcTime"::string like '%/%',
                 try_to_timestamp_ntz(
           try_parse_json(hex_decode_string($1:"Body")):"UtcTime"::string,'mm/dd/yyyy HH12:mi:ss AM'),

           try_parse_json(hex_decode_string($1:"Body")):"UtcTime"::string)
                     AS EVENT_TIMESTAMP_UTC,
           
           try_parse_json(hex_decode_string($1:"Body")):"Member"::string as MEMBER_ID,  		  
           try_parse_json(hex_decode_string($1:"Body"))                  as EVENT_PAYLOAD,
           metadata$filename,        -- which filename we read from
           metadata$file_row_number, -- row number in file
           current_timestamp()                                             as ETL_CREATED_TIMESTAMP_UTC,
           try_parse_json(hex_decode_string($1:"Body")):"Source"::string as SOURCE,
  		   split_part(try_parse_json(hex_decode_string($1:"Body")):"Path"::string,'/',-1) as DEVICEID	
    from @${ve_DB_DWH}.stg."raw_data_zone"/event-hub-capture/sats-prd-mdp-event-streams-ns/sats-prd-mobile-app-requests/0/${vj_fileDatePath_current}
    (file_format => '${ve_DB_DWH}.STG.default_avro_format' , pattern => '.*\.avro')
);

---- Another example of Avro -------------------------------------------------------------------------

copy into ${ve_DB_DWH}.PSA.SP_WEB_SALE from (
    
  select	   
           try_parse_json(hex_decode_string($1:"Body"))                  as EVENT_PAYLOAD,
	       try_parse_json(hex_decode_string($1:"Body")):"salesPerson":"exerpExternalId" as SalesPerson_externalID,
	       try_parse_json(hex_decode_string($1:"Body")): "saleType" as Sale_Type,
	       try_parse_json(hex_decode_string($1:"Body")):"member"."exerpPersonId" as Contact_Code,
	       concat(try_parse_json(hex_decode_string($1:"Body")):"member"."exerpMemberId"."clubId",'p', try_parse_json(hex_decode_string($1:"Body")):"member"."exerpMemberId"."id") as Member_Code,
	       try_parse_json(hex_decode_string($1:"Body")): "recruitmentCode" as RecruitmentCode,
	       concat(try_parse_json(hex_decode_string($1:"Body")): "membership"."exerpSubscriptionId"."clubId",'ss',try_parse_json(hex_decode_string($1:"Body")): "membership"."exerpSubscriptionId"."id") as Subscription_Id,
           concat(try_parse_json(hex_decode_string($1:"Body")): "selections"."exerpProductId"."clubId",'prod',try_parse_json(hex_decode_string($1:"Body")): "selections"."exerpProductId"."id") as Product_Id,
           try_parse_json(hex_decode_string($1:"Body")): "campaignCode" as Campaign_Code,
	       try_parse_json(hex_decode_string($1:"Body")): "brand" as Brand,
	       try_parse_json(hex_decode_string($1:"Body")): "club"."country" as Country,
           try_parse_json(hex_decode_string($1:"Body")): "source" as Source,
	       try_parse_json(hex_decode_string($1:"Body")):"createdAt" as CreatedDate,
           try_parse_json(hex_decode_string($1:"Body")):"exerpGlobalId" as Exerp_Global_Id,
           try_parse_json(hex_decode_string($1:"Body")): "membership"."periodPriceAmountInLocalCurrency" as Period_Price_Amount_In_Current_Membership,
           try_parse_json(hex_decode_string($1:"Body")): "payment"."paymentReference" as Payment_Reference,
           try_parse_json(hex_decode_string($1:"Body")): "voucher" as Voucher,
           metadata$filename,
           metadata$file_row_number,
           current_timestamp()    as ETL_CREATED_TIMESTAMP_UTC,
  		   try_parse_json(hex_decode_string($1:"Body")):"clubSalesClubId" as clubSalesClubId	
    from @${ve_DB_DWH}.stg."raw_data_zone"/event-hub-capture/sats-prd-mdp-event-streams-ns/sats-prd-mdp-web-sale-messages-eh/
    (file_format => '${ve_DB_DWH}.STG.default_avro_format' , pattern => '.*\.avro')
);

----------------Parquet ------------------------------------------------------------------------------------

-- Unload data to azure blob storage

COPY INTO @${ve_DB_DM}.ML.CURATED_DATA_ZONE_ML_STAGE/churn/scoring/input/${vj_TABLE_TO_UNLOAD}/SCORING_DATE=${vj_SCORING_DATE_STR}/
FROM (
    select *
    from ${ve_DB_DM}.ml.${vj_TABLE_TO_UNLOAD}
  	where SCORING_DATE = '${vj_SCORING_DATE_STR}'
    )
FILE_FORMAT = (TYPE = PARQUET, COMPRESSION = SNAPPY)
HEADER = TRUE
DETAILED_OUTPUT = TRUE
OVERWRITE = TRUE
;
--------------------------------

-- copy blob data to a table 
COPY INTO ${ve_DB_DM}.ml.SCORING_RESULTS_CHURN_12_WEEK (
    SCORING_DATE,
    PK,
    PREDICTION_PROBABILITY,
    MODEL_NAME,
    MODEL_VERSION,
    SCORING_DATETIME_UTC,
    ETL_METADATA_FILENAME,
    ETL_METADATA_ROW_NUMBER,
    ETL_CREATEDATETIME
    )
    from (
        select
               to_timestamp($1:SCORING_DATE::int/1000000)::DATE,
               $1:PK::STRING,
               $1:PREDICTION_PROBABILITY::NUMERIC(6,5),
               $1:MODEL_NAME::STRING,
               $1:MODEL_VERSION::STRING,
      		   $1:SCORING_DATETIME_UTC::TIMESTAMP,
               metadata$filename,
               metadata$file_row_number,
               current_timestamp
        from @${ve_DB_DM}.ml.CURATED_DATA_ZONE_ML_STAGE/churn/scoring/output/churn-12wk/
            ( file_format  => 'dm.ml.PARQUET_FILE_FORMAT', pattern=>'.*\\.parquet') t
        )
;
-------------------------------------------------------------------------------------------------------------------------------------------
