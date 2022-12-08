CREATE TABLE IF NOT EXISTS hive.odyn_css.fact_ads_moderation_funnel_final
(
  site_code             varchar,
  ad_category_l1        varchar,
  private_business      varchar,
  source_of_moderation  varchar,
  decision_auto         varchar,
  decision_manual       varchar,
  total_ads             integer,
  median_timediff       integer,
  dt                    date
)

WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY ['DT']
);

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

DELETE
FROM   hive.odyn_css.fact_ads_moderation_funnel_final
WHERE  dt = Date('${vars:date}') - interval '1' day;

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

INSERT INTO hive.odyn_css.fact_ads_moderation_funnel_final


WITH countries_category AS
            (
                      SELECT    id                      AS ad_id,
                                livesync_dbname         AS site_code,
                                category_id,
                                private_business
                      FROM      hive.livesync.cee_olxkz_ads x
                      WHERE date(created_at_first) between date('${vars:date}') - interval '1' day and date('${vars:date}')

                      UNION ALL

                      SELECT    id                      AS ad_id,
                                livesync_dbname         AS site_code,
                                category_id,
                                private_business
                      FROM      hive.livesync.cee_olxbg_ads x
                      WHERE date(created_at_first) between date('${vars:date}') - interval '1' day and date('${vars:date}')

                      UNION ALL

                      SELECT    id                      AS ad_id,
                                livesync_dbname         AS site_code,
                                category_id,
                                private_business
                      FROM      hive.livesync.cee_olxro_ads x
                      WHERE date(created_at_first) between date('${vars:date}') - interval '1' day and date('${vars:date}')

                      UNION ALL

                      SELECT    id                      AS ad_id,
                                livesync_dbname         AS site_code,
                                category_id,
                                private_business
                      FROM      hive.livesync.cee_olxuz_ads x
                      WHERE date(created_at_first) between date('${vars:date}') - interval '1' day and date('${vars:date}')

                      UNION ALL

                      SELECT    id                      AS ad_id,
                                livesync_dbname         AS site_code,
                                category_id,
                                private_business
                      FROM      hive.livesync.cee_olxpl_ads x
                      WHERE date(created_at_first) between date('${vars:date}') - interval '1' day and date('${vars:date}')

                      UNION ALL

                      SELECT    id                      AS ad_id,
                                livesync_dbname         AS site_code,
                                category_id,
                                private_business
                      FROM      hive.livesync.cee_olxua_ads x
                      WHERE date(created_at_first) between date('${vars:date}') - interval '1' day and date('${vars:date}')

                      UNION ALL

                      SELECT    id                      AS ad_id,
                                livesync_dbname         AS site_code,
                                category_id,
                                private_business
                      FROM      hive.livesync.cee_olxpt_ads x
                      WHERE date(created_at_first) between date('${vars:date}') - interval '1' day and date('${vars:date}')


),

category as (
                      SELECT cast(ad_id as varchar) ad_id,
                             site_code,
                             category_id,
                             private_business, category_l1_name_en AS category_l1
                      FROM countries_category x
                      LEFT JOIN hive.olxgroup_reservoir_ares.reservoirs_olxgroup_reservoir_eu_bi_eu_bi_dim_categories y
                      ON     cast(x.category_id AS varchar) = y.category_nk

                          and x.site_code = 'olx'||lower(country_nk)
                          and region_sk = 'olx|eu'
),

total_verdicts AS ( 
       
WITH verdicts_ade AS
(
       SELECT cast(params_content_event_adid AS varchar)                                ad_id,
              params_content_event_sitecode                                             site_code,
              params_content_event_source                                               source_of_moderation,
              params_content_meta_type                                                  decision_auto,
              params_content_event_extra_parentadid                                     parent_adid,
              cast(from_iso8601_timestamp(params_content_event_createdat) AS timestamp) auto_moderation_dt,
              year,
              month,
              day
       FROM   hive.hydra.global_css_verdicts
       WHERE  cast(concat(year,'-',month,'-',day) AS date) BETWEEN date('${vars:date}') - interval '1' day AND    date('${vars:date}')
       AND    params_content_meta_sender = 'ads-decision-engine'
       AND    params_content_event_source <> 'hermes'
), 

verdicts_moderators AS
(
       SELECT cast(params_content_event_adid AS varchar)                                ad_id,
              params_content_event_sitecode                                             site_code,
              params_content_meta_type                                                  decision_manual,
              params_content_event_moderationcategory                                   moderation_category,
              cast(from_iso8601_timestamp(params_content_event_createdat) AS timestamp) verdict_moderator_dt
       FROM   hive.hydra.global_css_verdicts
       WHERE  cast(concat(year,'-',month,'-',day) AS date) BETWEEN date('${vars:date}') - interval '1' day AND    date('${vars:date}')
       AND    params_content_meta_sender = 'ad-moderation-queue'
       AND    params_content_event_moderationcategory IN ('default', 'duplicate_ads') -- potentially add post-moderation here
), 

final_default_queue AS
(
          SELECT    distinct 
                    x.*,
                    decision_manual,
                    y.verdict_moderator_dt,
                    date_diff('minute', auto_moderation_dt,verdict_moderator_dt) datediff,
rank() OVER (partition BY x.ad_id,x.site_code, verdict_moderator_dt ORDER BY auto_moderation_dt desc) rank,
                    cast(concat(x.year,'-',x.month,'-',x.day) AS date) dt
          FROM      verdicts_ade x
          LEFT JOIN verdicts_moderators y
          ON        x.ad_id=y.ad_id
          AND       x.site_code=y.site_code
          AND       auto_moderation_dt < verdict_moderator_dt
          WHERE x.source_of_moderation <> 'umbrella' 
          AND y.moderation_category = 'default'
),

final_duplicate_queue AS
(
          SELECT    distinct 
                    x.*,
                    decision_manual,
                    y.verdict_moderator_dt,
                    date_diff('minute', auto_moderation_dt,verdict_moderator_dt) datediff,
rank() OVER (partition BY x.ad_id,x.site_code, verdict_moderator_dt ORDER BY auto_moderation_dt desc) rank,
                    cast(concat(x.year,'-',x.month,'-',x.day) AS date) dt
          FROM      verdicts_ade x
          LEFT JOIN verdicts_moderators y
          ON
          (
                    x.ad_id = y.ad_id
          OR        x.parent_adid = y.ad_id 
          )
          AND       x.site_code=y.site_code
          AND       auto_moderation_dt < verdict_moderator_dt
          WHERE x.source_of_moderation = 'umbrella' 
          AND y.moderation_category = 'duplicate_ads'
),

final as (
select * from final_default_queue
UNION ALL
select * from final_duplicate_queue
)
          

SELECT ad_id,
       site_code,
       source_of_moderation,
       decision_auto,
       auto_moderation_dt,
       CASE
              WHEN decision_manual IS NULL AND decision_auto = 'ad.verdict.premoderate' THEN 'No verdict yet'
              WHEN decision_manual IS NULL AND decision_auto <> 'ad.verdict.premoderate' THEN 'auto-moderated'
              ELSE decision_manual
       END decision_manual,
       CASE
              WHEN verdict_moderator_dt IS NULL THEN auto_moderation_dt
              ELSE verdict_moderator_dt
       END AS verdict_moderator_dt,
       datediff,
       dt
FROM   final
WHERE  rank = 1
), 

final_table AS
(
          SELECT    x.ad_id,
                    x.site_code,
                    category_l1,
                    y.private_business,
                    source_of_moderation,
                    decision_auto,
                    decision_manual,
                    auto_moderation_dt,
                    verdict_moderator_dt, --has all the timings
                    datediff,
                    dt
          FROM      total_verdicts x
          LEFT JOIN category y
          ON        x.ad_id = y.ad_id
          AND       x.site_code = y.site_code
)
          

SELECT   site_code,
         category_l1,
         Cast(private_business AS VARCHAR),
         source_of_moderation,
         decision_auto,
         decision_manual,
         Count(ad_id) AS total_ads,
         datediff,
         --approx_percentile(datediff,0.5) as median_timediff,
         dt
FROM     final_table
WHERE    dt = Date('${vars:date}') - interval '1' day
GROUP BY 1,2,3,4,5,6,8,9;
