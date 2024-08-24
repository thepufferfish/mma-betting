from dagster import job, define_asset_job, AssetSelection, AssetKey, ScheduleDefinition
from mma_betting.ops.extract.ufcstats_ops import detect_ufcstats_events
from mma_betting.assets.extract.fetch_tapology import crawl_tapology

@job
def crawler_job():
    crawl_tapology()

crawler_schedule = ScheduleDefinition(job=crawler_job, cron_schedule='5 8 * * 0')

@job
def detect_ufcstats_events_job():
    detect_ufcstats_events()

detect_ufcstats_events_schedule = ScheduleDefinition(job=detect_ufcstats_events_job, cron_schedule='5 8 * * 0')

fetch_fightodds_events_list_job = define_asset_job(name='fetch_fightodds_events_list_job', selection=AssetSelection.assets(AssetKey(['fightodds', 'fetch_events_list_fightodds'])))

fetch_fightodds_events_list_schedule = ScheduleDefinition(job=fetch_fightodds_events_list_job, cron_schedule='5 8 * * 0')