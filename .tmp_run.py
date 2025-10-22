from src.jobs import job_ads_summary, job_ads_daily
import os
adv   = os.environ['ADVERTISER_ID']
site  = os.environ['SITE_ID']
start = '2025-07-24'
end   = '2025-10-21'
print('SUMMARY →', job_ads_summary(adv, site))
print('DAILY   →', job_ads_daily(adv, site, start, end))
