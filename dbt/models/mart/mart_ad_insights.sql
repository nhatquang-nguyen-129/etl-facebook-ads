{{ 
  config(
    alias = var('company') ~ '_table_facebook_all_all_creative_performance',
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["account_id", "ad_id"],
    tags = ['mart', 'facebook', 'ad']
  ) 
}}

select
    date,
    month,
    year,

    department,
    account,

    account_id,
    campaign_id,
    adset_id,
    ad_id,

    ad_name,
    ad_status,
    thumbnail_url,

    impressions,
    clicks,
    spend,

    result,
    result_type,
    messaging_conversations_started,
    purchase,

    platform,
    objective,
    budget_group,
    region,
    category_level_1,
    track,
    pillar,
    `group`,

    location,
    gender,
    age,
    audience,
    `format`,
    strategy,
    angle,
    content,
    `type`

from {{ ref('int_ad_insights') }}