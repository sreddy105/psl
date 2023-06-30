TRUNCATE TABLE `{{ params.PIPELINE_PROJECT_ID }}.ads_pcc_imprsns_stg.wads_insertion_order_campaign`;

INSERT INTO `{{ params.PIPELINE_PROJECT_ID }}.ads_pcc_imprsns_stg.wads_insertion_order_campaign` (
io_insertion_order_name,
io_owning_advertiser_cd,
io_reseller_name,
c_campaign_id,
c_campaign_name
) 
SELECT	DISTINCT 
	CASE 
		WHEN INSORD.insertion_order_name IS NULL THEN '' 
		ELSE INSORD.insertion_order_name 
	end AS io_insertion_order_name, 
	INSORD.owning_advertiser_cd, 
	INSORD.reseller_name,
	CMPG.campaign_id,
	CMPG.campaign_name 
FROM `{{ params.LAKEHOUSE_PROJECT_ID }}.ads_reference.insertion_order` INSORD, 
	`{{ params.LAKEHOUSE_PROJECT_ID }}.ads_reference.campaign` CMPG 
WHERE
	(INSORD.insertion_order_cd = CMPG.insertion_order_cd) 
	AND (CAST(CMPG.discontinue_ts AS DATE) = '9999-12-31') 
	AND (INSORD.reseller_name = '{{ params.RESELLER_NAME }}' 
	AND CAST(INSORD.discontinue_ts AS DATE) = '9999-12-31') ;