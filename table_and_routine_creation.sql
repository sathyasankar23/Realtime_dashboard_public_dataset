--create a table for the raw layer

create table `endless-upgrade-475014-b1.g4a_events.event_raw`
( event_date string,
  user_pseudo_id STRING	,
  event_name STRING	,
  traffic_source_name STRING	,
  traffic_medium STRING	,
  traffic_campaign STRING	,
  event_time timestamp default current_timestamp()
);

---

create table `endless-upgrade-475014-b1.g4a_events.event_stg`
( event_date string,
  user_pseudo_id STRING	,
  event_name STRING	,
  traffic_source_name STRING	,
  traffic_medium STRING	,
  traffic_campaign STRING	,
  event_time timestamp,
  db_prcsdttm timestamp default current_timestamp()

);
--creating a table for user_attribution(final table)

create table `endless-upgrade-475014-b1.g4a_events.user_attribution`
( event_date string,
  user_pseudo_id STRING	,
  event_name STRING	,
  traffic_source_name STRING	,
  traffic_medium STRING	,
  traffic_campaign STRING	,
  event_time timestamp,
  is_first_click bool,
  is_last_click bool
);


---inserting the history record from the ga4 public dataset

insert into `endless-upgrade-475014-b1.g4a_events.event_raw`
  SELECT
  event_date,
  user_pseudo_id,
  event_name,
  traffic_source.source AS traffic_source_name,
  traffic_source.medium AS traffic_medium,
  traffic_source.name AS traffic_campaign,
  TIMESTAMP_MICROS(event_timestamp) AS event_time
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20201201' AND '20210131'
  
  
---inserting the data from RAW TO STG
CREATE OR REPLACE PROCEDURE `endless-upgrade-475014-b1.g4a_events.user_attribution_routine`()
begin
MERGE INTO `endless-upgrade-475014-b1.g4a_events.event_stg` AS stg
USING (
  SELECT
    event_date,
    user_pseudo_id,
    event_name,
    traffic_source_name,
    traffic_medium,
    traffic_campaign,
    event_time
  FROM `endless-upgrade-475014-b1.g4a_events.event_raw`
) AS raw
ON
  stg.event_date = raw.event_date AND
  stg.user_pseudo_id = raw.user_pseudo_id AND
  stg.event_name = raw.event_name AND
  stg.event_time = raw.event_time AND
  stg.traffic_source_name= raw.traffic_source_name AND
  stg.traffic_medium = raw.traffic_medium AND
  stg.traffic_campaign = raw.traffic_campaign
WHEN NOT MATCHED THEN
  INSERT (
    event_date,
    user_pseudo_id,
    event_name,
    traffic_source_name,
    traffic_medium,
    traffic_campaign,
    event_time
  )
  VALUES (
    raw.event_date,
    raw.user_pseudo_id,
    raw.event_name,
    raw.traffic_source_name,
    raw.traffic_medium,
    raw.traffic_campaign,
    raw.event_time
  );


---inserting the data from stg to final table

INSERT INTO `endless-upgrade-475014-b1.g4a_events.user_attribution` (
  event_date,
  user_pseudo_id,
  event_name,
  traffic_source_name,
  traffic_medium,
  traffic_campaign,
  event_time,
  is_first_click,
  is_last_click
)
SELECT *
FROM (
  -- CTEs for preprocessing
  WITH events AS (
    SELECT
      event_date,
      user_pseudo_id,
      event_name,
      traffic_source_name,
      traffic_medium,
      traffic_campaign,
      event_time
    FROM `endless-upgrade-475014-b1.g4a_events.event_stg`
  ),
  ---there are multiple records for id. Since this a transactional data. So,finding the event_name which has first_visit & timestamp
  first_click AS (
   SELECT
    user_pseudo_id,
    event_time AS first_click_time
  FROM events
  WHERE event_name = 'first_visit'
  ),
  ---finding the which has last_visit based on the recent timestamp
  last_click AS (
   SELECT
    user_pseudo_id,
    MAX(event_time) AS last_click_time
  FROM events
  GROUP BY user_pseudo_id
  )
  ----setting the flags
  SELECT
    e.event_date,
    e.user_pseudo_id,
    e.event_name,
    e.traffic_source_name,
    e.traffic_medium,
    e.traffic_campaign,
    e.event_time,
    CASE WHEN e.event_time = f.first_click_time THEN TRUE ELSE FALSE END AS is_first_click,
    CASE WHEN e.event_time = l.last_click_time THEN TRUE ELSE FALSE END AS is_last_click
  FROM events e
  LEFT JOIN first_click f
    ON e.user_pseudo_id = f.user_pseudo_id
    and e.event_name = 'first_visit'
  LEFT JOIN last_click l
    ON e.user_pseudo_id = l.user_pseudo_id
)
ORDER BY user_pseudo_id, event_time;
end


