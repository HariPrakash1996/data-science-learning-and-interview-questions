/*create table rocai_analysis2.prof_acs_defaulter_list_new
(
msisdn_new bigint,
denomination int,
currency string,
txn_amt double,
txn_cnt int,
flag string,
default_flag int,
bundle_flag int,
segment string
)
PARTITIONED BY (part_date STRING)
STORED AS ORC;

alter table rocai_analysis2.prof_acs_defaulter_list_new drop if exists partition(part_date='${hiveconf:cur_date}');
insert into rocai_analysis2.prof_acs_defaulter_list_new partition(part_date) 
select msisdn_new,denomination,'zwl' as currency,txn_amt,txn_cnt,a.flag,default_flag,bundle_flag,segment,part_date 
from rocai_analysis2.prof_acs_defaulter_list where part_date<='20230331';*/



/*select msisdn_new,denomination,currency,txn_amt,txn_cnt,a.flag,default_flag,bundle_flag,segment,part_date 
from rocai_analysis2. where part_date='hiveconf curr date';

select msisdn_new,radio_rev,...
0 as guessnwin_rev
0 as guessnwn_cnt
,part_date
from org table where part_Date<='10nov';*/
--------------------------------------------------------------------------
----new logics for alter insert tables
--------------------------------------------------------------------------
drop table rocai_analysis2.prof_new_business_msisdn_daily_comp_bundle_inclusive_temp_new;
create table rocai_analysis2.prof_new_business_msisdn_daily_comp_bundle_inclusive_temp_new
(
msisdn varchar(64),
comp_bundle_cnt bigint,
comp_bundle_rev double,
allocations_platform_cnt bigint,
allocations_platform_rev double,
vassdp_307_cnt bigint,
vassdp_307_rev double,
vassdp_6dcontent_cnt bigint,
vassdp_6dcontent_rev double,
vassdp_6dcp_1_cnt bigint,
vassdp_6dcp_1_rev double,
vassdp_6dcp_2_cnt bigint,
vassdp_6dcp_2_rev double,
vassdp_africalotto_cnt bigint,
vassdp_africalotto_rev double,
vassdp_amh_mns_cnt bigint,
vassdp_amh_mns_rev double,
vassdp_amhmns_cnt bigint,
vassdp_amhmns_rev double,
vassdp_beauty_tips_cnt bigint,
vassdp_beauty_tips_rev double,
vassdp_car_tips_cnt bigint,
vassdp_car_tips_rev double,
vassdp_comedy_box_cnt bigint,
vassdp_comedy_box_rev double,
vassdp_comedybox_cnt bigint,
vassdp_comedybox_rev double,
vassdp_connected_life_cnt bigint,
vassdp_connected_life_rev double,
vassdp_connected_life_tiips_cnt bigint,
vassdp_connected_life_tiips_rev double,
vassdp_connected_life_tips_cnt bigint,
vassdp_connected_life_tips_rev double,
vassdp_crbt_cnt bigint,
vassdp_crbt_rev double,
vassdp_dailynews_1_cnt bigint,
vassdp_dailynews_1_rev double,
vassdp_dailynews_2_cnt bigint,
vassdp_dailynews_2_rev double,
vassdp_ecofarmer_1_cnt bigint,
vassdp_ecofarmer_1_rev double,
vassdp_ecofarmer_2_cnt bigint,
vassdp_ecofarmer_2_rev double,
vassdp_ecofarmer_3_cnt bigint,
vassdp_ecofarmer_3_rev double,
vassdp_ecohealth_1_cnt bigint,
vassdp_ecohealth_1_rev double,
vassdp_ecohealth_2_cnt bigint,
vassdp_ecohealth_2_rev double,
vassdp_farmingnews_cnt bigint,
vassdp_farmingnews_rev double,
vassdp_financialgaz_cnt bigint,
vassdp_financialgaz_rev double,
vassdp_fingaz_cnt bigint,
vassdp_fingaz_rev double,
vassdp_fingaz_news_cnt bigint,
vassdp_fingaz_news_rev double,
vassdp_huawei_gaming_cnt bigint,
vassdp_huawei_gaming_rev double,
vassdp_hyvemobile_cnt bigint,
vassdp_hyvemobile_rev double,
vassdp_maisha_tips_cnt bigint,
vassdp_maisha_tips_rev double,
vassdp_mobile_radio_cnt bigint,
vassdp_mobile_radio_rev double,
vassdp_music_app_cnt bigint,
vassdp_music_app_rev double,
guessnwin_cnt bigint,
guessnwin_rev double,
music_wo_guess_cnt bigint,
music_wo_guess_rev double,
vassdp_musicapp_cnt bigint,
vassdp_musicapp_rev double,
vassdp_mussicapp_cnt bigint,
vassdp_mussicapp_rev double,
vassdp_religious_portal_cnt bigint,
vassdp_religious_portal_rev double,
vassdp_religiousportal_cnt bigint,
vassdp_religiousportal_rev double,
vassdp_social_xpress_cnt bigint,
vassdp_social_xpress_rev double,
vassdp_upstream2018_cnt bigint,
vassdp_upstream2018_rev double,
vassdp_zimpapers_cnt bigint,
vassdp_zimpapers_rev double,
others_cnt bigint,
others_rev double,
homing_bsc varchar(64),
homing_province string,
homing_district varchar(64),
homing_clusters varchar(64),
homing_site_code varchar(64),
homing_site_name varchar(64),
homing_longitude double,
homing_latitude double,
max_technology string,
subs_max_tech string,
overall_cust_segment string
)
PARTITIONED BY (part_date STRING)
STORED AS ORC;


----------------

drop table rocai_analysis2.prof_new_business_msisdn_daily_agg_split_temp_new;
create table rocai_analysis2.prof_new_business_msisdn_daily_agg_split_temp_new
(
msisdn varchar(64),
music_rev double,
music_cnt bigint,
guessnwin_cnt bigint,
guessnwin_rev double,
music_wo_guess_cnt bigint,
music_wo_guess_rev double,
crbt_rev double,
crbt_cnt bigint,
radio_rev double,
radio_cnt bigint,
infotainment_rev double,
infotainment_cnt bigint,
mobile_news_rev double,
mobile_news_cnt bigint,
religious_rev double,
religious_cnt bigint,
homing_bsc varchar(64),
homing_province string,
homing_district varchar(64),
homing_clusters varchar(64),
homing_site_code varchar(64),
homing_site_name varchar(64),
homing_longitude double,
homing_latitude double,
max_technology string,
subs_max_tech string,
overall_cust_segment string,
week_of_year string,
week_of_month string,
day_of_week string

)
PARTITIONED BY (part_date STRING)
STORED AS ORC;


----------------3---------
drop table rocai_analysis2.prof_dcta_new_business_backend_v1_split_temp_new;
create table rocai_analysis2.prof_dcta_new_business_backend_v1_split_temp_new
(
dist_subscriber_count bigint,
music_subs_cnt bigint,
guessnwin_subs_cnt bigint,
music_wo_guess_subs_cnt bigint,
crbt_subs_cnt bigint,
radio_subs_cnt bigint,
religious_subs_cnt bigint,
infotainment_subs_cnt bigint,
mobile_news_subs_cnt bigint,
music_rev double,
guessnwin_rev double,
music_wo_guess_rev double,
music_cnt bigint,
guessnwin_cnt bigint,
music_wo_guess_cnt bigint,
crbt_rev double,
crbt_cnt bigint,
radio_rev double,
radio_cnt bigint,
infotainment_rev double,
infotainment_cnt bigint,
mobile_news_rev double,
mobile_news_cnt bigint,
religious_rev double,
religious_cnt bigint,
homing_bsc varchar(64),
homing_province string,
homing_district varchar(64),
homing_clusters varchar(64),
homing_site_code varchar(64),
homing_site_name varchar(64),
homing_longitude double,
homing_latitude double,
max_technology string,
subs_max_tech string,
overall_cust_segment string,
week_of_year string,
week_of_month string,
day_of_week string

)
PARTITIONED BY (part_date STRING)
STORED AS ORC;

---------------------------------4--------------

drop table rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_bckp_20230602;
create table rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_bckp_20230602
(
calling_nbr varchar(64),
party_code string,
party_type string,
subscription_count bigint,
revenue double,
homing_bsc varchar(64),
homing_province string,
homing_district varchar(64),
homing_clusters varchar(64),
homing_site_code varchar(64),
homing_site_name varchar(64),
homing_longitude double,
homing_latitude double,
max_technology string,
subs_max_tech string,
overall_cust_segment string
)
PARTITIONED BY (part_date STRING)
STORED AS ORC;


---------------64 prof -----------------


------------------------------------------------------------------------

/*Alter Table rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_temp_new Drop If Exists Partition (part_date='${hiveconf:cur_date}');
Insert Into rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_temp_new  partition (part_date)
Select a.msisdn, a.party_code, a.party_type, a.subscription_count, a.revenue, 
b.homing_bsc, b.homing_province, b.homing_district, b.homing_clusters, b.homing_site_code, b.homing_site_name,
b.homing_longitude, b.homing_latitude, b.max_technology, b.subs_max_tech, b.overall_cust_segment, a.part_date
From (
(Select * From rocai_analysis2.tmp_new_business_long_format_msisdn_stats Where part_date='${hiveconf:cur_date}')a 
Left Join 
(Select calling_nbr, part_date, homing_bsc, homing_province, homing_district, homing_clusters, homing_site_code, homing_site_name, homing_latitude, homing_longitude, max_technology, subs_max_tech, overall_cust_segment
From rocai_analysis3.prof_all_info_msisdn_daily_final_cco
Where part_date='${hiveconf:cur_date}'
And plan_code not like '%Staff%' And plan_code not like '%Spouse%' And plan_code not like '%Postpaid%' And plan_code not like '%Hybrid%' And plan_code like '%Prepaid%')b 
on a.msisdn=b.calling_nbr And a.part_date=b.part_date);*/


Alter Table rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_bckp_20230602 Drop If Exists Partition (part_date='20220401');
Insert Into rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_bckp_20230602  partition (part_date)
Select calling_nbr, party_code, party_type, subscription_count, revenue, 
homing_bsc, homing_province, homing_district, homing_clusters, homing_site_code, homing_site_name,
homing_longitude, homing_latitude, max_technology, subs_max_tech, overall_cust_segment, part_date
from 
rocai_analysis2.20230602_01_H_prof_new_business_msisdn_partycode_daily_split_temp_bckp
where part_date='20220401';

Alter Table rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_bckp_20230602 Drop If Exists Partition (part_date='${hiveconf:cur_date}');
Insert Into rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_bckp_20230602  partition (part_date)
Select calling_nbr, party_code, party_type, subscription_count, revenue, 
homing_bsc, homing_province, homing_district, homing_clusters, homing_site_code, homing_site_name,
homing_longitude, homing_latitude, max_technology, subs_max_tech, overall_cust_segment, part_date
from 
rocai_analysis2.20230602_01_H_prof_new_business_msisdn_partycode_daily_split_temp_bckp
Where part_date='${hiveconf:cur_date}';


-------------59.0.1-----------------------------------------

prof_all_music_entertainment_DoS_daily
DROP TABLE IF EXISTS rocai_analysis2.prof_all_music_entertainment_DoS_daily_bckp_20230703;

CREATE TABLE rocai_analysis2.prof_all_music_entertainment_DoS_daily_bckp_20230703 (
  dos INT,
  overall_customer_count BIGINT,
  music_customer_count BIGINT,
  crbt_customer_count BIGINT,
  radio_customer_count BIGINT,
  infotain_customer_count BIGINT,
  mobile_news_customer_count BIGINT,
  religious_customer_count BIGINT
)
PARTITIONED BY (part_date STRING)
STORED AS ORC;

rocai_analysis2.prof_new_business_msisdn_partycode_daily_split_bckp_20230612


---------------------chat gpt prof schema------------
DROP TABLE IF EXISTS rocai_analysis2.prof_mne_VASSDP_ChatGPT_campaign;

CREATE TABLE rocai_analysis2.prof_mne_VASSDP_ChatGPT_campaign (
  msisdn varchar(64),
  rev double,
  subscription_cnt bigint,
  region string,
  cluster string,
  homing_province string,
  homing_district string,
  homing_site_code string,
  homing_site_name string,
  homing_longitude string,
  homing_latitude string,
  max_technology string,
  subs_max_tech string,
  overall_cust_segment string,
  week_of_year string,
  week_of_month string,
  day_of_week string
) PARTITIONED BY (part_date STRING) STORED AS ORC;

select part_date,count(distinct msisdn) as cust from rocai_analysis.u_z_cust_inst_msisdn_acct_res_id_orc where party_code = 'VASSDP_ESPORT'
and part_date>='20230314' and part_date>='20230330'
group by 
part_date;



-----------------big cash--------------

Select a.DoS,a.flag,
case when a.flag='overall_customer_count' then b.overall_customer_count
when a.flag='africa_customer_count' then b.africa_customer_count
when a.flag='huawei_customer_count' then b.huawei_customer_count
when a.flag='upsteam_customer_count' then b.upsteam_customer_count
when a.flag='whalecloud_customer_count' then b.whalecloud_customer_count
when a.flag='fantasy_customer_count' then b.fantasy_customer_count
Else 0 End as customer_count,
a.part_date
from rocai_analysis2.temp_all_gaming_DoS_daily_v1 as a
Left Join rocai_analysis2.prof_all_gaming_DoS_daily b
ON a.DoS=b.DoS and a.part_date=b.part_date;


-- Drop table if it exists
DROP TABLE IF EXISTS rocai_analysis2.prof_all_gaming_DoS_daily_v1;

-- Create table
CREATE TABLE rocai_analysis2.prof_all_gaming_DoS_daily_v1 (
    DoS STRING,
    flag STRING,
    customer_count BIGINT
) PARTITIONED BY (part_date STRING) STORED AS ORC;




ALTER TABLE rocai_analysis2.prof_all_gaming_DoS_daily_v1 DROP IF EXISTS PARTITION (part_date = '${hiveconf:cur_date}');


INSERT INTO rocai_analysis2.prof_all_gaming_DoS_daily_v1 PARTITION(part_date)
Select a.DoS,a.flag,
case when a.flag='overall_customer_count' then b.overall_customer_count
when a.flag='africa_customer_count' then b.africa_customer_count
when a.flag='huawei_customer_count' then b.huawei_customer_count
when a.flag='upsteam_customer_count' then b.upsteam_customer_count
when a.flag='whalecloud_customer_count' then b.whalecloud_customer_count
when a.flag='fantasy_customer_count' then b.fantasy_customer_count
Else 0 End as customer_count,
a.part_date
from rocai_analysis2.202403045_01_H_temp_all_gaming_DoS_daily_v1 as a
Left Join rocai_analysis2.prof_all_gaming_DoS_daily b
ON a.DoS=b.DoS and a.part_date=b.part_date;

-- rename: prof_all_gaming_DoS_daily as prof_all_gaming_DoS_daily_old
--rename: prof_all_gaming_DoS_daily_v1 as prof_all_gaming_DoS_daily


Alter table rocai_analysis2.prof_all_gaming_DoS_daily rename to rocai_analysis2.prof_all_gaming_DoS_daily_old;
Alter table rocai_analysis2.prof_all_gaming_DoS_daily_v1 rename to rocai_analysis2.prof_all_gaming_DoS_daily;




alter table rocai_analysis2.prof_all_gaming_DoS_daily drop if exists partition(part_date='${hiveconf:cur_date}');
insert into rocai_analysis2.prof_all_gaming_DoS_daily partition(part_date)
select DoS,overall_customer_count,africa_customer_count,huawei_customer_count,
upsteam_customer_count,whalecloud_customer_count,nvl(fantasy_customer_count,0) fantasy_customer_count,
yogamez_customer_count,bigcash_customer_count,
part_date from rocai_analysis2.temp_all_gaming_DoS_daily_temp_stats
where part_date = '${hiveconf:cur_date}'
;




-------------------esport campaign profiler------------------------------------

ALTER TABLE rocai_analysis2.prof_mne_VASSDP_ESPORT_campaign DROP IF EXISTS PARTITION (part_date = '${hiveconf:cur_date}');
INSERT INTO rocai_analysis2.prof_mne_VASSDP_ESPORT_campaign PARTITION(part_date)
select 
a.msisdn,
sum(nvl(a.amount/1000000,0)) as rev,
count(1) as subscription_cnt,
nvl(b.homing_bsc,'Unmapped') as region,
nvl(b.homing_clusters,'Unmapped') as cluster,
nvl(b.homing_province,'Unmapped') as homing_province,
nvl(b.homing_district,'Unmapped') as homing_district,
nvl(b.homing_site_code,'Unmapped') as homing_site_code,
nvl(b.homing_site_name,'Unmapped') as homing_site_name,
nvl(b.homing_longitude,'Unmapped') as homing_longitude,
nvl(b.homing_latitude,'Unmapped') as homing_latitude,
nvl(b.max_technology,'Unmapped') as max_technology,
nvl(b.subs_max_tech,'Unmapped') as subs_max_tech,
nvl(b.overall_cust_segment,'Unmapped') as overall_cust_segment,
date_format(
from_unixtime(unix_timestamp(part_date, 'yyyyMMdd')),
'w'
) week_of_year,
date_format(
from_unixtime(unix_timestamp(part_date, 'yyyyMMdd')),
'W'
) week_of_month,
date_format(
from_unixtime(unix_timestamp(part_date, 'yyyyMMdd')),
'E'
) day_of_week,
a.part_date
from (select * from
rocai_analysis.u_z_cust_inst_msisdn_acct_res_id_orc where party_code = 'VASSDP_ESPORT' and part_date = '${hiveconf:cur_date}'
)a
left join 
(SELECT calling_nbr, homing_bsc, homing_clusters, 
homing_province,
homing_district,
homing_site_code,
homing_site_name,
homing_longitude,
homing_latitude,
max_technology,
subs_max_tech,
overall_cust_segment,
part_date FROM rocai_analysis3.prof_all_info_msisdn_daily_final_cco
where part_date = '${hiveconf:cur_date}')  b on a.msisdn = b.calling_nbr and a.part_date = b.part_date
group by 
a.msisdn,
nvl(b.homing_bsc,'Unmapped'),
nvl(b.homing_clusters,'Unmapped'),
nvl(b.homing_province,'Unmapped'),
nvl(b.homing_district,'Unmapped'),
nvl(b.homing_site_code,'Unmapped'),
nvl(b.homing_site_name,'Unmapped'),
nvl(b.homing_longitude,'Unmapped'),
nvl(b.homing_latitude,'Unmapped'),
nvl(b.max_technology,'Unmapped'),
nvl(b.subs_max_tech,'Unmapped'),
nvl(b.overall_cust_segment,'Unmapped'),
a.part_date;


----------------esport snapshot codes---------------------------------------------
-----------------------------------DOD summary----------------
drop table if exists rocai_analysis2.temp_mne_VASSDP_ESPORT_campaign_dates_cal_actual ;
create table rocai_analysis2.temp_mne_VASSDP_ESPORT_campaign_dates_cal_actual stored as orc AS
Select 
part_date,
region,
cluster,
homing_province,
homing_district,
homing_site_code,
homing_site_name,
homing_longitude,
homing_latitude,
max_technology,
subs_max_tech,
overall_cust_segment,
rtgs_interbank_exchange,
Count(Distinct msisdn) as cnt_dist_msisdn,
tax_rate,
sum(nvl(rev,0)) as rev,
week_of_year,
week_of_month,
day_of_week,
(case when day_of_week = 'Sun' then 1 
when day_of_week = 'Mon' then 2
when day_of_week = 'Tue' then 3
when day_of_week = 'Wed' then 4
when day_of_week = 'Thu' then 5
when day_of_week = 'Fri' then 6
when day_of_week = 'Sat' then 7 else 0 end) day_of_week_num,
(case when substr(part_date, 7, 2) <= 7 then 1
when substr(part_date, 7, 2) <= 14 then 2
when substr(part_date, 7, 2) <= 21 then 3
when substr(part_date, 7, 2) <= 28 then 4 else 5 
end) as week_of_month_1,
sum(nvl(subscription_cnt,0)) as subscription_cnt from
(select a.*,b.rtgs_interbank_exchange,b.tax_rate from rocai_analysis2.prof_mne_VASSDP_ESPORT_campaign  a
left join rocai_analysis2.rtgs_usd_conversion_rate b on a.part_date = b.part_date)c
group by 
part_date,region,cluster,
homing_province,
homing_district,
homing_site_code,
homing_site_name,
homing_longitude,
homing_latitude,
max_technology,
subs_max_tech,
overall_cust_segment,rtgs_interbank_exchange,tax_rate,
(case when substr(part_date, 7, 2) <= 7 then 1
when substr(part_date, 7, 2) <= 14 then 2
when substr(part_date, 7, 2) <= 21 then 3
when substr(part_date, 7, 2) <= 28 then 4 else 5 
end),
week_of_year,
week_of_month,
day_of_week,
(case when day_of_week = 'Sun' then 1 
when day_of_week = 'Mon' then 2
when day_of_week = 'Tue' then 3
when day_of_week = 'Wed' then 4
when day_of_week = 'Thu' then 5
when day_of_week = 'Fri' then 6
when day_of_week = 'Sat' then 7 else 0 end);

-------------------------preparing actual vs prior----------------------------------------
drop table if exists rocai_analysis2.temp_mne_VASSDP_ESPORT_campaign_dates_cal_actual_vs_prior_v1 ;
create table rocai_analysis2.temp_mne_VASSDP_ESPORT_campaign_dates_cal_actual_vs_prior_v1 stored as orc AS
select *,
'Actual' as period,
'${hiveconf:cur_month}' as month_1
where substr(part_date,1,6) = '${hiveconf:cur_month}'
from rocai_analysis2.temp_mne_VASSDP_ESPORT_campaign_dates_cal_actual
union ALL
select *,
'Prior' as period,
'${hiveconf:prev_month}'as month_1
from rocai_analysis2.temp_mne_VASSDP_ESPORT_campaign_dates_cal_actual
where substr(part_date,1,6) = '${hiveconf:prev_month}' 
;

--------------------------------------prep for mtd level--------------
Drop table if exists rocai_analysis2.temp_mne_VASSDP_esport_campaign_mtd;
Create table rocai_analysis2.temp_mne_VASSDP_esport_campaign_mtd stored as orc as
select region,
cluster,
'Actual' as period,
'${hiveconf:cur_month}' as month_1,
part_date,
msisdn,
sum(rev) as revenues_zwl,
sum(subscription_cnt) as subscription_cnt
from rocai_analysis2.prof_mne_VASSDP_ESPORT_campaign
where substr(part_date,1,6) = '${hiveconf:cur_month}'
group by 
region,
cluster,
part_date,
msisdn
union all
select region,
cluster,
'Prior Month' as period,
'${hiveconf:prev_month}' as month_1,
part_date,
msisdn,
sum(rev) as revenues_zwl,
sum(subscription_cnt) as subscription_cnt
from rocai_analysis2.prof_mne_VASSDP_ESPORT_campaign
where substr(part_date,1,6) = '${hiveconf:prev_month}'
and substr(part_date,7,8) <= substr('${hiveconf:cur_date}',7,8)
group by 
region,cluster,part_date,
msisdn;


--------------------------snap for mtd with summary---------------------------------
Drop table if exists rocai_analysis2.temp_esport_campaign_mtd_snapshot;
Create table rocai_analysis2.temp_esport_campaign_mtd_snapshot stored as orc as
select
region,
cluster,
period,
month_1,
count(distinct msisdn) as customers,
sum(revenues_zwl) as revenues_zwl,
sum(subscription_cnt) as subscription_cnt,
avg(b.tax_rate) as tax_rate,
avg(b.rtgs_interbank_exchange) as rtgs_interbank_exchange
from
rocai_analysis2.temp_mne_VASSDP_esport_campaign_mtd a
left join rocai_analysis2.rtgs_usd_conversion_rate b on a.part_date = b.part_date
group by
region,
cluster,
period,
month_1;




-----------------------------esport----------------------------------------
DROP TABLE IF EXISTS rocai_analysis2.prof_mne_VASSDP_ESPORT_campaign;

CREATE TABLE rocai_analysis2.prof_mne_VASSDP_ESPORT_campaign (
  msisdn varchar(64),
  rev double,
  subscription_cnt bigint,
  region string,
  cluster string,
  homing_province string,
  homing_district string,
  homing_site_code string,
  homing_site_name string,
  homing_longitude string,
  homing_latitude string,
  max_technology string,
  subs_max_tech string,
  overall_cust_segment string,
  week_of_year string,
  week_of_month string,
  day_of_week string
) PARTITIONED BY (part_date STRING) STORED AS ORC;





prev_month              string
msisdn                  string
bundle_code             varchar(64)
aug_chk_flag            string


DROP TABLE IF EXISTS rocai_analysis2.20231004_01_H_regiontag_base;

CREATE TABLE rocai_analysis2.20231004_01_H_regiontag_base (
msisdn string,
bundle_code varchar(64),
aug_chk_flag string
)
PARTITIONED BY (prev_month STRING)
STORED AS ORC;






DROP TABLE IF EXISTS rocai_analysis2.prof_all_dod_homing_bsc_tagging;

CREATE TABLE rocai_analysis2.prof_all_dod_homing_bsc_tagging (
calling_nbr varchar(64),
txn_cnt bigint,
list_of_site_code varchar(255), -- Adjusted data type to varchar(255)
homing_site_code string,
homing_bsc string
)
PARTITIONED BY (part_date STRING)
STORED AS ORC;


-----------------64 prof schrm a------------------

ALTER TABLE rocai_analysis2.prof_yomix_er_tariff_stats_usd DROP IF EXISTS PARTITION (part_date = '20240217');
INSERT INTO rocai_analysis2.prof_yomix_er_tariff_stats_usd PARTITION(part_date)

Select validity,offer_id, service_type, units_purchased, revenue, units_consumed, 
revenue/units_purchased as tariff_offered,
revenue/units_consumed as effective_rate,
part_date
From (
Select validity, offer_id,service_type,
voice_mou_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases_usd
Where part_date='20240217'
And service_type='voice'
UNION ALL 
Select validity,offer_id, service_type,
data_mb_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases_usd
Where part_date='20240217'
And service_type='data'
UNION ALL 
Select validity,offer_id, service_type,
sms_usage_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases_usd
Where part_date='20240217'
And service_type='sms')x;



Drop table if exists rocai_analysis2.prof_yomix_er_tariff_stats_dashboard_snapshot_usd;
Create table rocai_analysis2.prof_yomix_er_tariff_stats_dashboard_snapshot_usd stored as orc AS
Select *,
case when offer_id ='13570' then 'USD_YoMix_DiY_daily'
when offer_id ='13571' then 'USD_DIY_Daily'
when offer_id ='13572' then 'USD_YoMix_DIY_3_Day'
when offer_id ='13573' then 'USD_DIY_Weekly'
when offer_id ='13574' then 'USD_YoMix_DIY_Bi_Weekly'
when offer_id ='13575' then 'USD_DIY_Monthly'
else 'Other' end as Bundle_type
 from rocai_analysis2.prof_yomix_er_tariff_stats_usd
where part_date>= '20231120' and part_date<= '20240217';
















DROP TABLE IF EXISTS rocai_analysis2.prof_yomix_er_tariff_stats_usd;

CREATE TABLE rocai_analysis2.prof_yomix_er_tariff_stats_usd (
validity STRING,
offer_id STRING,
service_type STRING,
units_purchased DOUBLE,
revenue DOUBLE,
units_consumed DOUBLE,
tariff_offered FLOAT,
effective_rate FLOAT
)
PARTITIONED BY (part_date STRING)
STORED AS ORC;




drop table if exists rocai_analysis2.temp_call_cnt_cust_rolling_7_dod;

CREATE TABLE rocai_analysis2.temp_call_cnt_cust_rolling_7_dod (
  msisdn string,
  d0 int,
  d1 int,
  d2 int,
  d3 int,
  d4 int,
  d5 int,
  d6 int
) PARTITIONED BY (part_date STRING) STORED AS ORC;





Alter Table rocai_analysis2.prof_all_dod_homing_bsc_tagging Drop If Exists Partition (part_date='${hiveconf:cur_date}');
Insert Into rocai_analysis2.prof_all_dod_homing_bsc_tagging  partition (part_date)
Select * from 
(SELECT
calling_nbr,
Txn_cnt,
list_of_site_code,
homing_site_code,
homing_bsc,
part_Date
FROM
rocai_analysis2.tmp_homing_bsc_updating_vds_base_v1_rank_r1_r5_regions
UNION
Select
calling_nbr,
txn_cnt,
'first_location' as list_of_site_code,
first_location as homing_site_code ,
homing_bsc,
part_Date
from rocai_analysis2.tmp_homing_bsc_updating_vds_base_v1_rank_r1_r5_f1_loc
UNION
Select
calling_nbr,
txn_cnt,
'last_location' as list_of_site_code,
last_location as homing_site_code ,
homing_bsc,
part_Date
from rocai_analysis2.tmp_homing_bsc_updating_vds_base_v1_rank_r1_r5_l1_loc) a
Order by part_date,calling_nbr;


















------------64 prof end to end with both currency splits------------

Drop table if exists rocai_analysis2.tmp_cust_inst_yomix_subset;
Create table rocai_analysis2.tmp_cust_inst_yomix_subset stored as orc as 
Select msisdn, part_date, offer_id, party_code,  
round(amount/1000000,2) as cdr_chg, ref_attr,
cast(split(split(split(ref_attr,'DIY_VOICE_AMOUNT')[1], ':')[1],',')[0] as int) as voice_mou_purchased,
cast(split(split(split(ref_attr,'DIY_DATA_AMOUNT')[1], ':')[1],',')[0] as int) as data_mb_purchased,
cast(split(split(split(ref_attr,'DIY_SMS_AMOUNT')[1], ':')[1],',')[0] as int) as sms_usage_purchased,
case 
when offer_id='8870' then 'Daily'
when offer_id='8871' then '3Day'
when offer_id='8872' then 'Weekly'
when offer_id='8873' then 'BiWeekly'
when offer_id='8874' then 'Monthly' else 'NA' end as validity
From rocai_analysis.u_z_cust_inst_msisdn_acct_res_id_orc
Where part_date='${hiveconf:cur_date}'
And offer_id in (8870,8871,8872,8873,8874)
and  party_code in ('MI','YXD','YXW','YX3D','YXM','YXBW')
and ref_attr like '%DIY_CHARGE_AMOUNT%';


Drop table if exists rocai_analysis2.tmp_cust_inst_yomix_subset_im1;
Create table rocai_analysis2.tmp_cust_inst_yomix_subset_im1 stored as orc as 
Select *,
case 
when voice_mou_purchased<=0 And data_mb_purchased<=0 And sms_usage_purchased<=0 then 'Non_VDS'
when voice_mou_purchased<=0 And data_mb_purchased<=0 And sms_usage_purchased>0 then 'S'
when voice_mou_purchased<=0 And data_mb_purchased>0 And sms_usage_purchased<=0 then 'D'
when voice_mou_purchased<=0 And data_mb_purchased>0 And sms_usage_purchased>0 then 'D_S'
when voice_mou_purchased>0 And data_mb_purchased<=0 And sms_usage_purchased<=0 then 'V'
when voice_mou_purchased>0 And data_mb_purchased<=0 And sms_usage_purchased>0 then 'V_S'
when voice_mou_purchased>0 And data_mb_purchased>0 And sms_usage_purchased<=0 then 'V_D'
when voice_mou_purchased>0 And data_mb_purchased>0 And sms_usage_purchased>0 then 'V_D_S'
else 'NA' end as subs_flag 
From rocai_analysis2.tmp_cust_inst_yomix_subset
Where part_date='${hiveconf:cur_date}';


Drop table if exists rocai_analysis2.tmp_cust_inst_yomix_subset_agg;
Create table rocai_analysis2.tmp_cust_inst_yomix_subset_agg stored as orc as 
Select part_date, 
case 
when subs_flag='V' then 'voice'
when subs_flag='D' then 'data'
when subs_flag='S' then 'sms'
else 'NA' end as service_type,
offer_id, validity,
count(distinct msisdn) as msisdn_cnt,
sum(nvl(cdr_chg,0)) as cdr_chg,
sum(nvl(voice_mou_purchased,0)) as voice_mou_purchased,
sum(nvl(data_mb_purchased,0)) as data_mb_purchased,
sum(nvl(sms_usage_purchased,0)) as sms_usage_purchased
From rocai_analysis2.tmp_cust_inst_yomix_subset_im1
Where part_date='${hiveconf:cur_date}'
And subs_flag in ('V','D','S')
Group By part_date, subs_flag, offer_id, validity;


Drop table if exists rocai_analysis2.tmp_daily_yomix_vds_usages;
Create table rocai_analysis2.tmp_daily_yomix_vds_usages stored as orc as 
Select 'voice' as service, part_date, acct_res_id,
case 
when acct_res_id='302' then 'Daily'
when acct_res_id='305' then '3Day'
when acct_res_id='308' then 'Weekly'
when acct_res_id='311' then 'BiWeekly'
when acct_res_id='314' then 'Monthly' else 'NA' end as validity,
sum(nvl(total_units_used,0)) as total_units_used
From rocai_analysis2.prof_msisdn_daily_voice_da_usage
Where part_date='${hiveconf:cur_date}'
And acct_res_id in ('302','305','308','311','314')
Group By part_date, acct_res_id, 
case 
when acct_res_id='302' then 'Daily'
when acct_res_id='305' then '3Day'
when acct_res_id='308' then 'Weekly'
when acct_res_id='311' then 'BiWeekly'
when acct_res_id='314' then 'Monthly' else 'NA' end
UNION ALL 
Select 'data' as service, part_date, acct_res_id,
case 
when acct_res_id='300' then 'Daily'
when acct_res_id='303' then '3Day'
when acct_res_id='306' then 'Weekly'
when acct_res_id='309' then 'BiWeekly'
when acct_res_id='312' then 'Monthly' else 'NA' end as validity,
sum(nvl(total_units_used,0)) as total_units_used
From rocai_analysis2.prof_msisdn_daily_da_usage
Where part_date='${hiveconf:cur_date}'
And acct_res_id in ('300','303','306','309','312')
Group By part_date, acct_res_id,
case 
when acct_res_id='300' then 'Daily'
when acct_res_id='303' then '3Day'
when acct_res_id='306' then 'Weekly'
when acct_res_id='309' then 'BiWeekly'
when acct_res_id='312' then 'Monthly' else 'NA' end
UNION ALL 
Select 'sms' as service, part_date, acct_res_id,
case 
when acct_res_id='301' then 'Daily'
when acct_res_id='304' then '3Day'
when acct_res_id='307' then 'Weekly'
when acct_res_id='310' then 'BiWeekly'
when acct_res_id='313' then 'Monthly' else 'NA' end as validity,
sum(nvl(total_units_used,0)) as total_units_used
From rocai_analysis2.prof_msisdn_daily_sms_da_usage
Where part_date='${hiveconf:cur_date}'
And acct_res_id in ('301','304','307','310','313')
Group By part_date, acct_res_id,
case 
when acct_res_id='301' then 'Daily'
when acct_res_id='304' then '3Day'
when acct_res_id='307' then 'Weekly'
when acct_res_id='310' then 'BiWeekly'
when acct_res_id='313' then 'Monthly' else 'NA' end;



Drop table if exists rocai_analysis2.tmp_daily_yomix_vds_usages_purchases;
Create table rocai_analysis2.tmp_daily_yomix_vds_usages_purchases stored as orc as 
Select a.part_date, a.validity, 
a.offer_id,
a.service_type, a.voice_mou_purchased, a.data_mb_purchased, a.sms_usage_purchased, a.cdr_chg,
case 
when service_type='voice' then nvl(total_units_used,0)/60
when service_type='data' then nvl(total_units_used,0)/1024
when service_type='sms' then nvl(total_units_used,0) 
else '0' end as total_units_used
From (
(Select * From rocai_analysis2.tmp_cust_inst_yomix_subset_agg Where part_date='${hiveconf:cur_date}')a 
Left Join 
(Select * From rocai_analysis2.tmp_daily_yomix_vds_usages Where part_date='${hiveconf:cur_date}')b 
on a.part_date=b.part_date And a.validity=b.validity And a.service_type=b.service);


/*Alter table rocai_analysis2.prof_yomix_er_tariff_stats drop if exists partition(part_date='${hiveconf:cur_date}');
Insert into rocai_analysis2.prof_yomix_er_tariff_stats partition(part_date)
Select validity, service_type, units_purchased, revenue, units_consumed, 
revenue/units_purchased as tariff_offered,
revenue/units_consumed as effective_rate,
part_date
From (
Select validity, service_type,
voice_mou_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases
Where part_date='${hiveconf:cur_date}'
And service_type='voice'
UNION ALL 
Select validity, service_type,
data_mb_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases
Where part_date='${hiveconf:cur_date}'
And service_type='data'
UNION ALL 
Select validity, service_type,
sms_usage_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases
Where part_date='${hiveconf:cur_date}'
And service_type='sms')x;*/


Drop table if exists rocai_analysis2.prof_yomix_er_tariff_stats_dashboard_snapshot;
Create table rocai_analysis2.prof_yomix_er_tariff_stats_dashboard_snapshot stored as orc AS
Select * from rocai_analysis2.prof_yomix_er_tariff_stats
where part_date>= '${hiveconf:prev_90_days}' and part_date<= '${hiveconf:cur_date}';



----------------bundle wise prof and snaps for zwl stats-----


ALTER TABLE rocai_analysis2.prof_yomix_er_tariff_stats_zwl DROP IF EXISTS PARTITION (part_date = '${hiveconf:cur_date}');
INSERT INTO rocai_analysis2.prof_yomix_er_tariff_stats_zwl PARTITION(part_date)

Select validity,offer_id, service_type, units_purchased, revenue, units_consumed, 
revenue/units_purchased as tariff_offered,
revenue/units_consumed as effective_rate,
part_date
From (
Select validity, offer_id,service_type,
voice_mou_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases
Where part_date='${hiveconf:cur_date}'
And service_type='voice'
UNION ALL 
Select validity,offer_id, service_type,
data_mb_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases
Where part_date='${hiveconf:cur_date}'
And service_type='data'
UNION ALL 
Select validity,offer_id, service_type,
sms_usage_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases
Where part_date='${hiveconf:cur_date}'
And service_type='sms')x;



Drop table if exists rocai_analysis2.prof_yomix_er_tariff_stats_dashboard_snapshot_new;
Create table rocai_analysis2.prof_yomix_er_tariff_stats_dashboard_snapshot_new stored as orc AS
Select *,
case when offer_id ='8870' then 'YoMix Daily'
when  offer_id ='8871' then 'YoMix 3 Day'
when  offer_id ='8872' then 'YoMix Weekly'
when  offer_id ='8873' then 'YoMix Bi-Weekly'
when  offer_id ='8874' then 'YoMix Monthly'
else 'Other' end as Bundle_type
 from rocai_analysis2.prof_yomix_er_tariff_stats_zwl
where part_date>= '${hiveconf:prev_90_days}' and part_date<= '${hiveconf:cur_date}';



-------------------------------------replicating above for usd stats----------------------



Drop table if exists rocai_analysis2.tmp_cust_inst_yomix_subset_usd;
Create table rocai_analysis2.tmp_cust_inst_yomix_subset_usd stored as orc as 
Select msisdn, part_date, offer_id, party_code,  
round(amount/1000000,2) as cdr_chg, ref_attr,
cast(split(split(split(ref_attr,'DIY_VOICE_AMOUNT')[1], ':')[1],',')[0] as int) as voice_mou_purchased,
cast(split(split(split(ref_attr,'DIY_DATA_AMOUNT')[1], ':')[1],',')[0] as int) as data_mb_purchased,
cast(split(split(split(ref_attr,'DIY_SMS_AMOUNT')[1], ':')[1],',')[0] as int) as sms_usage_purchased,
case 
when offer_id in ('13570','13571') then 'Daily'
when offer_id='13572' then '3Day'
when offer_id='13573' then 'Weekly'
when offer_id='13574' then 'BiWeekly'
when offer_id='13575' then 'Monthly' else 'NA' end as validity
From rocai_analysis.u_z_cust_inst_msisdn_acct_res_id_orc
Where part_date='${hiveconf:cur_date}'
And offer_id in (13570,13571,13572,13573,13574,13575)
--and  party_code in ('MI','YXD','YXW','YX3D','YXM','YXBW')
and ref_attr like '%DIY_CHARGE_AMOUNT%';

Drop table if exists rocai_analysis2.tmp_cust_inst_yomix_subset_im1_usd;
Create table rocai_analysis2.tmp_cust_inst_yomix_subset_im1_usd stored as orc as 
Select *,
case 
when voice_mou_purchased<=0 And data_mb_purchased<=0 And sms_usage_purchased<=0 then 'Non_VDS'
when voice_mou_purchased<=0 And data_mb_purchased<=0 And sms_usage_purchased>0 then 'S'
when voice_mou_purchased<=0 And data_mb_purchased>0 And sms_usage_purchased<=0 then 'D'
when voice_mou_purchased<=0 And data_mb_purchased>0 And sms_usage_purchased>0 then 'D_S'
when voice_mou_purchased>0 And data_mb_purchased<=0 And sms_usage_purchased<=0 then 'V'
when voice_mou_purchased>0 And data_mb_purchased<=0 And sms_usage_purchased>0 then 'V_S'
when voice_mou_purchased>0 And data_mb_purchased>0 And sms_usage_purchased<=0 then 'V_D'
when voice_mou_purchased>0 And data_mb_purchased>0 And sms_usage_purchased>0 then 'V_D_S'
else 'NA' end as subs_flag 
From rocai_analysis2.tmp_cust_inst_yomix_subset_usd
Where part_date='${hiveconf:cur_date}';


Drop table if exists rocai_analysis2.tmp_cust_inst_yomix_subset_agg_usd;
Create table rocai_analysis2.tmp_cust_inst_yomix_subset_agg_usd stored as orc as 
Select part_date, 
case 
when subs_flag='V' then 'voice'
when subs_flag='D' then 'data'
when subs_flag='S' then 'sms'
else 'NA' end as service_type,
offer_id, validity,
count(distinct msisdn) as msisdn_cnt,
sum(nvl(cdr_chg,0)) as cdr_chg,
sum(nvl(voice_mou_purchased,0)) as voice_mou_purchased,
sum(nvl(data_mb_purchased,0)) as data_mb_purchased,
sum(nvl(sms_usage_purchased,0)) as sms_usage_purchased
From rocai_analysis2.tmp_cust_inst_yomix_subset_im1_usd
Where part_date='${hiveconf:cur_date}'
And subs_flag in ('V','D','S')
Group By part_date, subs_flag, offer_id, validity;





Drop table if exists rocai_analysis2.tmp_daily_yomix_vds_usages_usd;
Create table rocai_analysis2.tmp_daily_yomix_vds_usages_usd stored as orc as 
Select 'voice' as service, part_date, acct_res_id,
case 
----daily NA
when acct_res_id='512' then '3Day'
when acct_res_id='515' then 'Weekly'
when acct_res_id='518' then 'BiWeekly'
when acct_res_id='521' then 'Monthly' else 'NA' end as validity,
sum(nvl(total_units_used,0)) as total_units_used
From rocai_analysis2.prof_msisdn_daily_voice_da_usage
Where part_date='${hiveconf:cur_date}'
And acct_res_id in ('512','515','518','521')
Group By part_date, acct_res_id,
case
when acct_res_id='512' then '3Day'
when acct_res_id='515' then 'Weekly'
when acct_res_id='518' then 'BiWeekly'
when acct_res_id='521' then 'Monthly' else 'NA' end
UNION ALL 
Select 'data' as service, part_date, acct_res_id,
case 
when acct_res_id='511' then 'Daily'
when acct_res_id='514' then '3Day'
when acct_res_id='517' then 'Weekly'
when acct_res_id='520' then 'BiWeekly'
when acct_res_id='523' then 'Monthly' else 'NA' end as validity,
sum(nvl(total_units_used,0)) as total_units_used
From rocai_analysis2.prof_msisdn_daily_da_usage
Where part_date='${hiveconf:cur_date}'
And acct_res_id in ('511','514','517','520','523')
Group By part_date, acct_res_id,
case
when acct_res_id='511' then 'Daily'
when acct_res_id='514' then '3Day'
when acct_res_id='517' then 'Weekly'
when acct_res_id='520' then 'BiWeekly'
when acct_res_id='523' then 'Monthly' else 'NA' end
UNION ALL 
Select 'sms' as service, part_date, acct_res_id,
case 
when acct_res_id='510' then 'Daily'
when acct_res_id='513' then '3Day'
when acct_res_id='516' then 'Weekly'
when acct_res_id='519' then 'BiWeekly'
when acct_res_id='522' then 'Monthly' else 'NA' end as validity,
sum(nvl(total_units_used,0)) as total_units_used
From rocai_analysis2.prof_msisdn_daily_sms_da_usage
Where part_date='${hiveconf:cur_date}'
And acct_res_id in ('510','513','516','519','522')
Group By part_date, acct_res_id,
case
when acct_res_id='510' then 'Daily'
when acct_res_id='513' then '3Day'
when acct_res_id='516' then 'Weekly'
when acct_res_id='519' then 'BiWeekly'
when acct_res_id='522' then 'Monthly' else 'NA' end;




Drop table if exists rocai_analysis2.tmp_daily_yomix_vds_usages_purchases_usd;
Create table rocai_analysis2.tmp_daily_yomix_vds_usages_purchases_usd stored as orc as 
Select a.part_date, a.validity, 
a.offer_id,
a.service_type, a.voice_mou_purchased, a.data_mb_purchased, a.sms_usage_purchased, a.cdr_chg,
case 
when service_type='voice' then nvl(total_units_used,0)/60
when service_type='data' then nvl(total_units_used,0)/1024
when service_type='sms' then nvl(total_units_used,0) 
else '0' end as total_units_used
From (
(Select * From rocai_analysis2.tmp_cust_inst_yomix_subset_agg_usd Where part_date='${hiveconf:cur_date}')a 
Left Join 
(Select * From rocai_analysis2.tmp_daily_yomix_vds_usages_usd Where part_date='${hiveconf:cur_date}')b 
on a.part_date=b.part_date And a.validity=b.validity And a.service_type=b.service);





ALTER TABLE rocai_analysis2.prof_yomix_er_tariff_stats_usd DROP IF EXISTS PARTITION (part_date = '${hiveconf:cur_date}');
INSERT INTO rocai_analysis2.prof_yomix_er_tariff_stats_usd PARTITION(part_date)

Select validity,offer_id, service_type, units_purchased, revenue, units_consumed, 
revenue/units_purchased as tariff_offered,
revenue/units_consumed as effective_rate,
part_date
From (
Select validity, offer_id,service_type,
voice_mou_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases_usd
Where part_date='${hiveconf:cur_date}'
And service_type='voice'
UNION ALL 
Select validity,offer_id, service_type,
data_mb_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases_usd
Where part_date='${hiveconf:cur_date}'
And service_type='data'
UNION ALL 
Select validity,offer_id, service_type,
sms_usage_purchased as units_purchased, 
cdr_chg as revenue,
total_units_used as units_consumed,
part_date
From rocai_analysis2.tmp_daily_yomix_vds_usages_purchases_usd
Where part_date='${hiveconf:cur_date}'
And service_type='sms')x;



Drop table if exists rocai_analysis2.prof_yomix_er_tariff_stats_dashboard_snapshot_usd;
Create table rocai_analysis2.prof_yomix_er_tariff_stats_dashboard_snapshot_usd stored as orc AS
Select *,
case when offer_id ='13570' then 'USD_YoMix_DiY_daily'
when offer_id ='13571' then 'USD_DIY_Daily'
when offer_id ='13572' then 'USD_YoMix_DIY_3_Day'
when offer_id ='13573' then 'USD_DIY_Weekly'
when offer_id ='13574' then 'USD_YoMix_DIY_Bi_Weekly'
when offer_id ='13575' then 'USD_DIY_Monthly'
else 'Other' end as Bundle_type
 from rocai_analysis2.prof_yomix_er_tariff_stats_usd
where part_date>= '${hiveconf:prev_90_days}' and part_date<= '${hiveconf:cur_date}';




col_name        data_type       comment
msisdn                  varchar(64)
acct_res_id             int
party_code              string
currency                string
subscription_cnt        bigint
revenue_zig             double
region                  string
cluster                 string
homing_province         string
homing_district         string
homing_site_code        string
homing_site_name        string
homing_longitude        string
homing_latitude         string
max_technology          string
subs_max_tech           string
overall_cust_segment    string
week_of_year            string
week_of_month           string
day_of_week             string
part_date               string





DROP TABLE IF EXISTS rocai_analysis2.prof_music_gaming_dod_zig_usd_stats_info;

CREATE TABLE rocai_analysis2.prof_music_gaming_dod_zig_usd_stats_info (
  msisdn varchar(64),
  acct_res_id string,
  party_code string,
  currency string,
  subscription_cnt bigint,
  revenue_zig double,
  region string,
  cluster string,
  homing_province string,
  homing_district string,
  homing_site_code string,
  homing_site_name string,
  homing_longitude string,
  homing_latitude string,
  max_technology string,
  subs_max_tech string,
  overall_cust_segment string,
  week_of_year string,
  week_of_month string,
  day_of_week string,
  dummy_1 string,
  dummy_2 string,
  dummy_3 string,
  dummy_4 string,
  dummy_5 string,
  dummy_6 string,
  dummy_7 string,
  dummy_8 string,
  dummy_9 string,
  dummy_10 string,
  dummy_11 string,
  dummy_12 string,
  dummy_13 string,
  dummy_14 string,
  dummy_15 string

)
PARTITIONED BY (part_date STRING)
STORED AS ORC;

ALTER TABLE rocai_analysis2.prof_music_gaming_dod_zig_usd_stats_info DROP IF EXISTS PARTITION (part_date = '${hiveconf:cur_date}');
INSERT INTO rocai_analysis2.prof_music_gaming_dod_zig_usd_stats_info PARTITION(part_date)


ALTER TABLE rocai_analysis2.prof_music_gaming_dod_zig_usd_stats_info DROP IF EXISTS PARTITION (part_date = '20240801');
INSERT INTO rocai_analysis2.prof_music_gaming_dod_zig_usd_stats_info PARTITION(part_date)
select
    a.msisdn,
    a.acct_res_id,
    a.party_code,
    a.currency,
    sum(a.subscription_cnt) as subscription_cnt,
    sum(a.revenue_zig) as revenue_zig,
    nvl(b.homing_bsc, 'Unmapped') as region,
    nvl(b.homing_clusters, 'Unmapped') as cluster,
    nvl(b.homing_province, 'Unmapped') as homing_province,
    nvl(b.homing_district, 'Unmapped') as homing_district,
    nvl(b.homing_site_code, 'Unmapped') as homing_site_code,
    nvl(b.homing_site_name, 'Unmapped') as homing_site_name,
    nvl(b.homing_longitude, 'Unmapped') as homing_longitude,
    nvl(b.homing_latitude, 'Unmapped') as homing_latitude,
    nvl(b.max_technology, 'Unmapped') as max_technology,
    nvl(b.subs_max_tech, 'Unmapped') as subs_max_tech,
    nvl(b.overall_cust_segment, 'Unmapped') as overall_cust_segment,
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'w') as week_of_year,
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'W') as week_of_month,
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'E') as day_of_week,
    '0' as dummy_1,
    '0' as dummy_2,
    '0' as dummy_3,
    '0' as dummy_4,
    '0' as dummy_5,
    '0' as dummy_6,
    '0' as dummy_7,
    '0' as dummy_8,
    '0' as dummy_9,
    '0' as dummy_10,
    '0' as dummy_11,
    '0' as dummy_12,
    '0' as dummy_13,
    '0' as dummy_14,
    '0' as dummy_15
    a.part_date
from
    (select * from rocai_analysis2.temp_music_gaming_usd_stats_pre_sanity_checks_rev_converted 
     where part_date = '20240801') a
left join 
    (select calling_nbr, homing_bsc, homing_clusters, homing_province, homing_district, homing_site_code,
            homing_site_name, homing_longitude, homing_latitude, max_technology, subs_max_tech,
            overall_cust_segment, part_date
     from rocai_analysis3.prof_all_info_msisdn_daily_final_cco
     where part_date = '20240801') b
on a.msisdn = b.calling_nbr and a.part_date = b.part_date
group by
    a.msisdn,
    a.acct_res_id,
    a.party_code,
    a.currency,
    nvl(b.homing_bsc, 'Unmapped'),
    nvl(b.homing_clusters, 'Unmapped'),
    nvl(b.homing_province, 'Unmapped'),
    nvl(b.homing_district, 'Unmapped'),
    nvl(b.homing_site_code, 'Unmapped'),
    nvl(b.homing_site_name, 'Unmapped'),
    nvl(b.homing_longitude, 'Unmapped'),
    nvl(b.homing_latitude, 'Unmapped'),
    nvl(b.max_technology, 'Unmapped'),
    nvl(b.subs_max_tech, 'Unmapped'),
    nvl(b.overall_cust_segment, 'Unmapped'),
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'w'),
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'W'),
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'E'),
    a.part_date;





---music new profiler 




DROP TABLE IF EXISTS rocai_analysis2.prof_music_new_prof_dod_zig_usd_stats_info;

CREATE TABLE rocai_analysis2.prof_music_new_prof_dod_zig_usd_stats_info (
  msisdn varchar(64),
  acct_res_id string,
  party_code string,
  currency string,
  subscription_cnt bigint,
  revenue_zig double,
  region string,
  cluster string,
  homing_province string,
  homing_district string,
  homing_site_code string,
  homing_site_name string,
  homing_longitude string,
  homing_latitude string,
  max_technology string,
  subs_max_tech string,
  overall_cust_segment string,
  week_of_year string,
  week_of_month string,
  day_of_week string,
  dummy_1 string,
  dummy_2 string,
  dummy_3 string,
  dummy_4 string,
  dummy_5 string,
  dummy_6 string,
  dummy_7 string,
  dummy_8 string,
  dummy_9 string,
  dummy_10 string,
  dummy_11 string,
  dummy_12 string,
  dummy_13 string,
  dummy_14 string,
  dummy_15 string

)
PARTITIONED BY (part_date STRING)
STORED AS ORC;

ALTER TABLE rocai_analysis2.prof_music_new_prof_dod_zig_usd_stats_info DROP IF EXISTS PARTITION (part_date = '${hiveconf:cur_date}');
INSERT INTO rocai_analysis2.prof_music_new_prof_dod_zig_usd_stats_info PARTITION(part_date)


ALTER TABLE rocai_analysis2.prof_music_new_prof_dod_zig_usd_stats_info DROP IF EXISTS PARTITION (part_date = '20240801');
INSERT INTO rocai_analysis2.prof_music_new_prof_dod_zig_usd_stats_info PARTITION(part_date)
select
    a.msisdn,
    a.acct_res_id,
    a.party_code,
    a.currency,
    sum(a.subscription_cnt) as subscription_cnt,
    sum(a.revenue_zig) as revenue_zig,
    nvl(b.homing_bsc, 'Unmapped') as region,
    nvl(b.homing_clusters, 'Unmapped') as cluster,
    nvl(b.homing_province, 'Unmapped') as homing_province,
    nvl(b.homing_district, 'Unmapped') as homing_district,
    nvl(b.homing_site_code, 'Unmapped') as homing_site_code,
    nvl(b.homing_site_name, 'Unmapped') as homing_site_name,
    nvl(b.homing_longitude, 'Unmapped') as homing_longitude,
    nvl(b.homing_latitude, 'Unmapped') as homing_latitude,
    nvl(b.max_technology, 'Unmapped') as max_technology,
    nvl(b.subs_max_tech, 'Unmapped') as subs_max_tech,
    nvl(b.overall_cust_segment, 'Unmapped') as overall_cust_segment,
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'w') as week_of_year,
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'W') as week_of_month,
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'E') as day_of_week,
    '0' as dummy_1,
    '0' as dummy_2,
    '0' as dummy_3,
    '0' as dummy_4,
    '0' as dummy_5,
    '0' as dummy_6,
    '0' as dummy_7,
    '0' as dummy_8,
    '0' as dummy_9,
    '0' as dummy_10,
    '0' as dummy_11,
    '0' as dummy_12,
    '0' as dummy_13,
    '0' as dummy_14,
    '0' as dummy_15
    a.part_date
from
    (select * from rocai_analysis2.temp_music_gaming_usd_stats_pre_sanity_checks_rev_converted 
     where part_date = '20240801') a
left join 
    (select calling_nbr, homing_bsc, homing_clusters, homing_province, homing_district, homing_site_code,
            homing_site_name, homing_longitude, homing_latitude, max_technology, subs_max_tech,
            overall_cust_segment, part_date
     from rocai_analysis3.prof_all_info_msisdn_daily_final_cco
     where part_date = '20240801') b
on a.msisdn = b.calling_nbr and a.part_date = b.part_date
group by
    a.msisdn,
    a.acct_res_id,
    a.party_code,
    a.currency,
    nvl(b.homing_bsc, 'Unmapped'),
    nvl(b.homing_clusters, 'Unmapped'),
    nvl(b.homing_province, 'Unmapped'),
    nvl(b.homing_district, 'Unmapped'),
    nvl(b.homing_site_code, 'Unmapped'),
    nvl(b.homing_site_name, 'Unmapped'),
    nvl(b.homing_longitude, 'Unmapped'),
    nvl(b.homing_latitude, 'Unmapped'),
    nvl(b.max_technology, 'Unmapped'),
    nvl(b.subs_max_tech, 'Unmapped'),
    nvl(b.overall_cust_segment, 'Unmapped'),
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'w'),
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'W'),
    date_format(from_unixtime(unix_timestamp(a.part_date, 'yyyyMMdd')), 'E'),
    a.part_date;








































col_name        data_type       comment
flag                    string
overall_customer_cnt    bigint
overall_subscription_cnt        bigint
overall_revenue_zig     double
vassdp_trivia_customer_cnt      bigint
vassdp_trivia_subscription_cnt  bigint
vassdp_trivia_revenue_zig       double
loyalty_gaming_customer_cnt     bigint
loyalty_gaming_subscription_cnt bigint
loyalty_gaming_revenue_zig      double
playnwin_customer_cnt   bigint
playnwin_subscription_cnt       bigint
playnwin_revenue_zig    double
big_cash_customer_cnt   bigint
big_cash_subscription_cnt       bigint
big_cash_revenue_zig    double
yogamez_customer_cnt    bigint
yogamez_subscription_cnt        bigint
yogamez_revenue_zig     double
yoplay_customer_cnt     bigint
yoplay_subscription_cnt bigint
yoplay_revenue_zig      double
esport_customer_cnt     bigint
esport_subscription_cnt bigint
esport_revenue_zig      double
mykidzhub_customer_cnt  bigint
mykidzhub_subscription_cnt      bigint
mykidzhub_revenue_zig   double
gamification_customer_cnt       bigint
gamification_subscription_cnt   bigint
gamification_revenue_zig        double
fantasy_football_customer_cnt   bigint
fantasy_football_subscription_cnt       bigint
fantasy_football_revenue_zig    double
bibleapp_customer_cnt   bigint
bibleapp_subscription_cnt       bigint
bibleapp_revenue_zig    double
part_month              string



drop table if exists rocai_analysis2.prof_gaming_monthly_inc_usd_stats_info;

CREATE TABLE rocai_analysis2.prof_gaming_monthly_inc_usd_stats_info (
  tax_rate double,
  rtgs_interbank_exchange double,
  flag string,
  overall_customer_cnt bigint,
  overall_subscription_cnt bigint,
  overall_revenue_zig double,
  vassdp_trivia_customer_cnt bigint,
  vassdp_trivia_subscription_cnt bigint,
  vassdp_trivia_revenue_zig double,
  loyalty_gaming_customer_cnt bigint,
  loyalty_gaming_subscription_cnt bigint,
  loyalty_gaming_revenue_zig double,
  playnwin_customer_cnt bigint,
  playnwin_subscription_cnt bigint,
  playnwin_revenue_zig double,
  big_cash_customer_cnt bigint,
  big_cash_subscription_cnt bigint,
  big_cash_revenue_zig double,
  yogamez_customer_cnt bigint,
  yogamez_subscription_cnt bigint,
  yogamez_revenue_zig double,
  yoplay_customer_cnt bigint,
  yoplay_subscription_cnt bigint,
  yoplay_revenue_zig double,
  esport_customer_cnt bigint,
  esport_subscription_cnt bigint,
  esport_revenue_zig double,
  mykidzhub_customer_cnt bigint,
  mykidzhub_subscription_cnt bigint,
  mykidzhub_revenue_zig double,
  gamification_customer_cnt bigint,
  gamification_subscription_cnt bigint,
  gamification_revenue_zig double,
  fantasy_football_customer_cnt bigint,
  fantasy_football_subscription_cnt bigint,
  fantasy_football_revenue_zig double,
  bibleapp_customer_cnt bigint,
  bibleapp_subscription_cnt bigint,
  bibleapp_revenue_zig double,
  dummy_1 bigint,
  dummy_2 bigint,
  dummy_3 double,
  dummy_4 bigint,
  dummy_5 bigint,
  dummy_6 double,
  dummy_7 bigint,
  dummy_8 bigint,
  dummy_9 double,
  dummy_10 bigint,
  dummy_11 bigint,
  dummy_12 double
)
PARTITIONED BY (part_month STRING)
STORED AS ORC;




ALTER TABLE rocai_analysis2.prof_gaming_monthly_inc_usd_stats_info DROP IF EXISTS PARTITION (part_month = '${hiveconf:cur_month}');

INSERT INTO rocai_analysis2.prof_gaming_monthly_inc_usd_stats_info PARTITION(part_month)
select
tax_rate,
rtgs_interbank_exchange,
flag,
count(distinct msisdn) as overall_customer_cnt,
sum(subscription_cnt) as overall_subscription_cnt,
sum(revenue_zig) as overall_revenue_zig,
count(distinct case when party_code = 'VASSDP_Trivia' then msisdn end) as VASSDP_Trivia_customer_cnt,
sum(case when party_code = 'VASSDP_Trivia' then subscription_cnt end) as VASSDP_Trivia_subscription_cnt,
sum(case when party_code = 'VASSDP_Trivia' then revenue_zig end) as VASSDP_Trivia_revenue_zig,
count(distinct case when party_code = 'loyalty gaming' then msisdn end) as loyalty_gaming_customer_cnt,
sum(case when party_code = 'loyalty gaming' then subscription_cnt end) as loyalty_gaming_subscription_cnt,
sum(case when party_code = 'loyalty gaming' then revenue_zig end) as loyalty_gaming_revenue_zig,
count(distinct case when party_code = 'playnwin' then msisdn end) as playnwin_customer_cnt,
sum(case when party_code = 'playnwin' then subscription_cnt end) as playnwin_subscription_cnt,
sum(case when party_code = 'playnwin' then revenue_zig end) as playnwin_revenue_zig,
count(distinct case when party_code = 'Big cash' then msisdn end) as Big_cash_customer_cnt,
sum(case when party_code = 'Big cash' then subscription_cnt end) as Big_cash_subscription_cnt,
sum(case when party_code = 'Big cash' then revenue_zig end) as Big_cash_revenue_zig,
count(distinct case when party_code = 'Yogamez' then msisdn end) as Yogamez_customer_cnt,
sum(case when party_code = 'Yogamez' then subscription_cnt end) as Yogamez_subscription_cnt,
sum(case when party_code = 'Yogamez' then revenue_zig end) as Yogamez_revenue_zig,
count(distinct case when party_code = 'Yoplay' then msisdn end) as Yoplay_customer_cnt,
sum(case when party_code = 'Yoplay' then subscription_cnt end) as Yoplay_subscription_cnt,
sum(case when party_code = 'Yoplay' then revenue_zig end) as Yoplay_revenue_zig,
count(distinct case when party_code = 'ESPORT' then msisdn end) as ESPORT_customer_cnt,
sum(case when party_code = 'ESPORT' then subscription_cnt end) as ESPORT_subscription_cnt,
sum(case when party_code = 'ESPORT' then revenue_zig end) as ESPORT_revenue_zig,
count(distinct case when party_code = 'MyKidzHub' then msisdn end) as MyKidzHub_customer_cnt,
sum(case when party_code = 'MyKidzHub' then subscription_cnt end) as MyKidzHub_subscription_cnt,
sum(case when party_code = 'MyKidzHub' then revenue_zig end) as MyKidzHub_revenue_zig,
count(distinct case when party_code = 'Gamification' then msisdn end) as Gamification_customer_cnt,
sum(case when party_code = 'Gamification' then subscription_cnt end) as Gamification_subscription_cnt,
sum(case when party_code = 'Gamification' then revenue_zig end) as Gamification_revenue_zig,
count(distinct case when party_code = 'fantasy football' then msisdn end) as fantasy_football_customer_cnt,
sum(case when party_code = 'fantasy football' then subscription_cnt end) as fantasy_football_subscription_cnt,
sum(case when party_code = 'fantasy football' then revenue_zig end) as fantasy_football_revenue_zig,
count(distinct case when party_code = 'BibleApp' then msisdn end) as BibleApp_customer_cnt,
sum(case when party_code = 'BibleApp' then subscription_cnt end) as BibleApp_subscription_cnt,
sum(case when party_code = 'BibleApp' then revenue_zig end) as BibleApp_revenue_zig,
'0' as dummy_1,
'0' as dummy_2,
'0' as dummy_3,
'0' as dummy_4,
'0' as dummy_5,
'0' as dummy_6,
'0' as dummy_7,
'0' as dummy_8,
'0' as dummy_9,
'0' as dummy_10,
part_month
from rocai_analysis2.temp_gaming_inc_usd_monthly_infov3
group by flag,part_month,tax_rate,rtgs_interbank_exchange;





---dod :rocai_analysis2.prof_gaming_daily_inc_usd_stats_info


drop table if exists rocai_analysis2.prof_music_daily_inc_usd_stats_info;

create table rocai_analysis2.prof_music_daily_inc_usd_stats_info (
  tax_rate double,
  rtgs_interbank_exchange double,
  flag string,
  overall_customer_cnt bigint,
  overall_subscription_cnt bigint,
  overall_revenue_zig double,
  vassdp_trivia_customer_cnt bigint,
  vassdp_trivia_subscription_cnt bigint,
  vassdp_trivia_revenue_zig double,
  loyalty_gaming_customer_cnt bigint,
  loyalty_gaming_subscription_cnt bigint,
  loyalty_gaming_revenue_zig double,
  playnwin_customer_cnt bigint,
  playnwin_subscription_cnt bigint,
  playnwin_revenue_zig double,
  big_cash_customer_cnt bigint,
  big_cash_subscription_cnt bigint,
  big_cash_revenue_zig double,
  yogamez_customer_cnt bigint,
  yogamez_subscription_cnt bigint,
  yogamez_revenue_zig double,
  yoplay_customer_cnt bigint,
  yoplay_subscription_cnt bigint,
  yoplay_revenue_zig double,
  esport_customer_cnt bigint,
  esport_subscription_cnt bigint,
  esport_revenue_zig double,
  mykidzhub_customer_cnt bigint,
  mykidzhub_subscription_cnt bigint,
  mykidzhub_revenue_zig double,
  gamification_customer_cnt bigint,
  gamification_subscription_cnt bigint,
  gamification_revenue_zig double,
  fantasy_football_customer_cnt bigint,
  fantasy_football_subscription_cnt bigint,
  fantasy_football_revenue_zig double,
  bibleapp_customer_cnt bigint,
  bibleapp_subscription_cnt bigint,
  bibleapp_revenue_zig double,
  dummy_1 bigint,
  dummy_2 bigint,
  dummy_3 bigint,
  dummy_4 bigint,
  dummy_5 bigint,
  dummy_6 bigint,
  dummy_7 bigint,
  dummy_8 bigint,
  dummy_9 bigint,
  dummy_10 bigint
)
partitioned by (part_date string)
stored as orc;


ALTER TABLE rocai_analysis2.prof_gaming_daily_inc_usd_stats_info DROP IF EXISTS PARTITION (part_date = '${hiveconf:cur_date}');

INSERT INTO rocai_analysis2.prof_gaming_daily_inc_usd_stats_info PARTITION(part_date)

select
tax_rate,
rtgs_interbank_exchange,
flag,
count(distinct msisdn) as overall_customer_cnt,
sum(subscription_cnt) as overall_subscription_cnt,
sum(revenue_zig) as overall_revenue_zig,
count(distinct case when party_code = 'VASSDP_Trivia' then msisdn end) as VASSDP_Trivia_customer_cnt,
sum(case when party_code = 'VASSDP_Trivia' then subscription_cnt end) as VASSDP_Trivia_subscription_cnt,
sum(case when party_code = 'VASSDP_Trivia' then revenue_zig end) as VASSDP_Trivia_revenue_zig,
count(distinct case when party_code = 'loyalty gaming' then msisdn end) as loyalty_gaming_customer_cnt,
sum(case when party_code = 'loyalty gaming' then subscription_cnt end) as loyalty_gaming_subscription_cnt,
sum(case when party_code = 'loyalty gaming' then revenue_zig end) as loyalty_gaming_revenue_zig,
count(distinct case when party_code = 'playnwin' then msisdn end) as playnwin_customer_cnt,
sum(case when party_code = 'playnwin' then subscription_cnt end) as playnwin_subscription_cnt,
sum(case when party_code = 'playnwin' then revenue_zig end) as playnwin_revenue_zig,
count(distinct case when party_code = 'Big cash' then msisdn end) as Big_cash_customer_cnt,
sum(case when party_code = 'Big cash' then subscription_cnt end) as Big_cash_subscription_cnt,
sum(case when party_code = 'Big cash' then revenue_zig end) as Big_cash_revenue_zig,
count(distinct case when party_code = 'Yogamez' then msisdn end) as Yogamez_customer_cnt,
sum(case when party_code = 'Yogamez' then subscription_cnt end) as Yogamez_subscription_cnt,
sum(case when party_code = 'Yogamez' then revenue_zig end) as Yogamez_revenue_zig,
count(distinct case when party_code = 'Yoplay' then msisdn end) as Yoplay_customer_cnt,
sum(case when party_code = 'Yoplay' then subscription_cnt end) as Yoplay_subscription_cnt,
sum(case when party_code = 'Yoplay' then revenue_zig end) as Yoplay_revenue_zig,
count(distinct case when party_code = 'ESPORT' then msisdn end) as ESPORT_customer_cnt,
sum(case when party_code = 'ESPORT' then subscription_cnt end) as ESPORT_subscription_cnt,
sum(case when party_code = 'ESPORT' then revenue_zig end) as ESPORT_revenue_zig,
count(distinct case when party_code = 'MyKidzHub' then msisdn end) as MyKidzHub_customer_cnt,
sum(case when party_code = 'MyKidzHub' then subscription_cnt end) as MyKidzHub_subscription_cnt,
sum(case when party_code = 'MyKidzHub' then revenue_zig end) as MyKidzHub_revenue_zig,
count(distinct case when party_code = 'Gamification' then msisdn end) as Gamification_customer_cnt,
sum(case when party_code = 'Gamification' then subscription_cnt end) as Gamification_subscription_cnt,
sum(case when party_code = 'Gamification' then revenue_zig end) as Gamification_revenue_zig,
count(distinct case when party_code = 'fantasy football' then msisdn end) as fantasy_football_customer_cnt,
sum(case when party_code = 'fantasy football' then subscription_cnt end) as fantasy_football_subscription_cnt,
sum(case when party_code = 'fantasy football' then revenue_zig end) as fantasy_football_revenue_zig,
count(distinct case when party_code = 'BibleApp' then msisdn end) as BibleApp_customer_cnt,
sum(case when party_code = 'BibleApp' then subscription_cnt end) as BibleApp_subscription_cnt,
sum(case when party_code = 'BibleApp' then revenue_zig end) as BibleApp_revenue_zig,
'0' as dummy_1,
'0' as dummy_2,
'0' as dummy_3,
'0' as dummy_4,
'0' as dummy_5,
'0' as dummy_6,
'0' as dummy_7,
'0' as dummy_8,
'0' as dummy_9,
'0' as dummy_10,
part_date
from rocai_analysis2.temp_gaming_inc_usd_daily_infov3
where part_date = '${hiveconf:cur_date}'
group by flag,part_date;







--------------for music 

prof_music_monthly_inc_usd_stats_info


drop table if exists rocai_analysis2.prof_music_monthly_inc_usd_stats_info;

CREATE TABLE rocai_analysis2.prof_music_monthly_inc_usd_stats_info (
  tax_rate double,
  rtgs_interbank_exchange double,
  flag string,
  overall_customer_cnt bigint,
  overall_subscription_cnt bigint,
  overall_revenue_zig double,
  crbt_customer_cnt bigint,
  crbt_subscription_cnt bigint,
  crbt_revenue_zig double,
  buddiebeatz_customer_cnt bigint,
  buddiebeatz_subscription_cnt bigint,
  buddiebeatz_revenue_zig double,
  dummy_1 bigint,
  dummy_2 bigint,
  dummy_3 bigint,
  dummy_4 bigint,
  dummy_5 bigint,
  dummy_6 bigint,
  dummy_7 bigint,
  dummy_8 bigint,
  dummy_9 bigint,
  dummy_10 bigint
)
PARTITIONED BY (part_month STRING)
STORED AS ORC;


ALTER TABLE rocai_analysis2.prof_music_monthly_inc_usd_stats_info DROP IF EXISTS PARTITION (part_month = '${hiveconf:cur_month}');
INSERT INTO rocai_analysis2.prof_music_monthly_inc_usd_stats_info PARTITION(part_month)
select 
tax_rate,
rtgs_interbank_exchange,
flag,
count(distinct msisdn) as overall_customer_cnt,
sum(subscription_cnt) as overall_subscription_cnt,
sum(revenue_zig) as overall_revenue_zig,
count(distinct case when party_code = 'CRBT' then msisdn end) as crbt_customer_cnt,
sum(case when party_code = 'CRBT' then subscription_cnt end) as crbt_subscription_cnt,
sum(case when party_code = 'CRBT' then revenue_zig end) as crbt_revenue_zig,
count(distinct case when party_code = 'BuddieBeatz' then msisdn end) as buddiebeatz_customer_cnt,
sum(case when party_code = 'BuddieBeatz' then subscription_cnt end) as buddiebeatz_subscription_cnt,
sum(case when party_code = 'BuddieBeatz' then revenue_zig end) as buddiebeatz_revenue_zig,
'0' as dummy_1,
'0' as dummy_2,
'0' as dummy_3,
'0' as dummy_4,
'0' as dummy_5,
'0' as dummy_6,
'0' as dummy_7,
'0' as dummy_8,
'0' as dummy_9,
'0' as dummy_10,
part_month
from rocai_analysis2.temp_music_inc_usd_monthly_infov3
where part_month = '${hiveconf:cur_month}'
group by flag,part_month,tax_rate,rtgs_interbank_exchange;




---same for daily 
rocai_analysis2.prof_music_daily_inc_usd_stats_info

drop table if exists rocai_analysis2.prof_music_daily_inc_usd_stats_info;

create table rocai_analysis2.prof_music_daily_inc_usd_stats_info (
  tax_rate double,
  rtgs_interbank_exchange double,
  flag string,
  overall_customer_cnt bigint,
  overall_subscription_cnt bigint,
  overall_revenue_zig double,
  crbt_customer_cnt bigint,
  crbt_subscription_cnt bigint,
  crbt_revenue_zig double,
  buddiebeatz_customer_cnt bigint,
  buddiebeatz_subscription_cnt bigint,
  buddiebeatz_revenue_zig double,
  dummy_1 bigint,
  dummy_2 bigint,
  dummy_3 bigint,
  dummy_4 bigint,
  dummy_5 bigint,
  dummy_6 bigint,
  dummy_7 bigint,
  dummy_8 bigint,
  dummy_9 bigint,
  dummy_10 bigint
)
partitioned by (part_date string)
stored as orc;



ALTER TABLE rocai_analysis2.prof_music_daily_inc_usd_stats_info DROP IF EXISTS PARTITION (part_date = '${hiveconf:cur_date}');
INSERT INTO rocai_analysis2.prof_music_daily_inc_usd_stats_info PARTITION(part_date)

select
tax_rate,
rtgs_interbank_exchange,
flag,
count(distinct msisdn) as overall_customer_cnt,
sum(subscription_cnt) as overall_subscription_cnt,
sum(revenue_zig) as overall_revenue_zig,
count(distinct case when party_code = 'CRBT' then msisdn end) as crbt_customer_cnt,
sum(case when party_code = 'CRBT' then subscription_cnt end) as crbt_subscription_cnt,
sum(case when party_code = 'CRBT' then revenue_zig end) as crbt_revenue_zig,
count(distinct case when party_code = 'BuddieBeatz' then msisdn end) as buddiebeatz_customer_cnt,
sum(case when party_code = 'BuddieBeatz' then subscription_cnt end) as buddiebeatz_subscription_cnt,
sum(case when party_code = 'BuddieBeatz' then revenue_zig end) as buddiebeatz_revenue_zig,
'0' as dummy_1,
'0' as dummy_2,
'0' as dummy_3,
'0' as dummy_4,
'0' as dummy_5,
'0' as dummy_6,
'0' as dummy_7,
'0' as dummy_8,
'0' as dummy_9,
'0' as dummy_10,
part_date
from rocai_analysis2.temp_music_inc_usd_daily_infov3
where part_date = '${hiveconf:cur_date}'
group by flag,part_date;






drop table if exists rocai_analysis2.prof_music_monthly_inc_usd_stats_info;

CREATE TABLE rocai_analysis2.prof_music_monthly_inc_usd_stats_info (
tax_rate double,
rtgs_interbank_exchange double,
flag string,
overall_customer_cnt bigint,
overall_subscription_cnt bigint,
overall_revenue_zig double,
Chatgpt_customer_cnt bigint,
Chatgpt_subscription_cnt bigint,
Chatgpt_revenue_zig double,
Radio_customer_cnt bigint,
Radio_subscription_cnt bigint,
Radio_revenue_zig double,
CRBT_customer_cnt bigint,
CRBT_subscription_cnt bigint,
CRBT_revenue_zig double,
Music_only_customer_cnt bigint,
Music_only_subscription_cnt bigint,
Music_only_revenue_zig double,
Religious_customer_cnt bigint,
Religious_subscription_cnt bigint,
Religious_revenue_zig double,
Guessnwin_customer_cnt bigint,
Guessnwin_subscription_cnt bigint,
Guessnwin_revenue_zig double,
Infotainment_customer_cnt bigint,
Infotainment_subscription_cnt bigint,
Infotainment_revenue_zig double,
Mobile_new_customer_cnt bigint,
Mobile_new_subscription_cnt bigint,
Mobile_new_revenue_zig double,
dummy_1 bigint,
dummy_2 bigint,
dummy_3 double,
dummy_4 bigint,
dummy_5 bigint,
dummy_6 double,
dummy_7 bigint,
dummy_8 bigint,
dummy_9 double,
dummy_10 bigint,
dummy_11 bigint,
dummy_12 double
)
PARTITIONED BY (part_month STRING)
STORED AS ORC;






drop table if exists rocai_analysis2.prof_music_daily_inc_usd_stats_info;

create table rocai_analysis2.prof_music_daily_inc_usd_stats_info (
  tax_rate double,
  rtgs_interbank_exchange double,
  flag string,
  overall_customer_cnt bigint,
  overall_subscription_cnt bigint,
  overall_revenue_zig double,
    Chatgpt_customer_cnt bigint,
    Chatgpt_subscription_cnt bigint,
    Chatgpt_revenue_zig double,
    Radio_customer_cnt bigint,
    Radio_subscription_cnt bigint,
    Radio_revenue_zig double,
    CRBT_customer_cnt bigint,
    CRBT_subscription_cnt bigint,
    CRBT_revenue_zig double,
    Music_only_customer_cnt bigint,
    Music_only_subscription_cnt bigint,
    Music_only_revenue_zig double,
    Religious_customer_cnt bigint,
    Religious_subscription_cnt bigint,
    Religious_revenue_zig double,
    Guessnwin_customer_cnt bigint,
    Guessnwin_subscription_cnt bigint,
    Guessnwin_revenue_zig double,
    Infotainment_customer_cnt bigint,
    Infotainment_subscription_cnt bigint,
    Infotainment_revenue_zig double,
    Mobile_new_customer_cnt bigint,
    Mobile_new_subscription_cnt bigint,
    Mobile_new_revenue_zig double,
  dummy_1 bigint,
  dummy_2 bigint,
  dummy_3 double,
  dummy_4 bigint,
  dummy_5 bigint,
  dummy_6 double,
  dummy_7 bigint,
  dummy_8 bigint,
  dummy_9 double,
  dummy_10 bigint,
    dummy_11 bigint,
    dummy_12 double
)
partitioned by (part_date string)
stored as orc;
