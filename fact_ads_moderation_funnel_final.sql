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
WITH total_verdicts AS ( 
       
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
       with verdicts as (
SELECT cast(params_content_event_adid AS varchar)                                ad_id,
              params_content_event_sitecode                                             site_code,
              params_content_meta_type                                                  decision_manual,
              params_content_event_moderationcategory                                   moderation_category,
              cast(from_iso8601_timestamp(params_content_event_createdat) AS timestamp) verdict_moderator_dt,
              params_content_event_extra_moderatoremail                                  mod_email
       FROM   hive.hydra.global_css_verdicts
       WHERE  cast(concat(year,'-',month,'-',day) AS date) BETWEEN date('${vars:date}') - interval '1' day AND    date('${vars:date}')
       AND    params_content_meta_sender = 'ad-moderation-queue'
       AND    params_content_event_moderationcategory IN ('default', 'duplicate_ads')
       --and params_content_event_adid = '826690639'
),
    decisions as (
SELECT
    distinct
    cast (params_content_event_adid AS varchar)                                  ad_id,
    params_content_event_type                                                    manual_action,
     params_content_event_sitecode                                               site_code,
    params_content_event_moderatordetails_email                                  mod_email
FROM hive.hydra.global_css_decisions
WHERE cast (concat(year, '-', month, '-', day) AS date) BETWEEN date ('${vars:date}') - interval '1' day AND date ('${vars:date}')
  AND params_content_meta_sender = 'ad-moderation-queue'
  AND params_content_meta_type = 'ad.decision.moderator'
  and params_content_event_moderationcategory in ('default', 'duplicate_ads')
  and params_content_event_type = 'edit'
  --and params_content_event_adid = '826690639'
    )
    SELECT x.ad_id,
           x.site_code,
           CASE
               WHEN x.decision_manual = 'ad.verdict.accept' and y.manual_action = 'edit' THEN 'ad.verdict.accept+edit'
                   else x.decision_manual
                       end as decision_manual,
           x.moderation_category,
           x.verdict_moderator_dt
    from verdicts x left join decisions y
on x.ad_id=y.ad_id and x.site_code=y.site_code and x.mod_email=y.mod_email
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
          and x.source_of_moderation <> 'ads-decision-engine'
          AND y.moderation_category = 'default'
),
   final_auto AS
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
          ON x.ad_id = y.ad_id
          AND       x.site_code=y.site_code
          AND       auto_moderation_dt < verdict_moderator_dt
          WHERE
          x.source_of_moderation <> 'umbrella'
          AND y.moderation_category IS NULL
          AND y.verdict_moderator_dt IS NULL
)
,
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
where rank = 1
UNION ALL
select * from final_auto
where rank = 1
UNION ALL
select * from final_duplicate_queue
where rank = 1
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
),
categories as 
(
       select *,
       rank() OVER (partition BY ad_id,site_code, category_id ORDER BY dt desc) rank
       from hive.odyn_css.fact_ads_categories_and_type
       where dt between Date('${vars:date}') - interval '60' day AND  Date('${vars:date}') -- potentially could be changed
),
categories_final as
(
       select * from categories
       where rank = 1
),
final_table AS 
(
          SELECT    DISTINCT
                    x.ad_id,
                    x.site_code,
                    category_l1,
                    private_business,
                    source_of_moderation,
                    decision_auto,
                    decision_manual,
                    auto_moderation_dt,
                    verdict_moderator_dt, --has all the timings
                    datediff,
                    x.dt
          FROM      total_verdicts x
          LEFT JOIN categories_final y
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
         dt
FROM     final_table
WHERE    dt = Date('${vars:date}') - interval '1' day
GROUP BY 1,2,3,4,5,6,8,9;
