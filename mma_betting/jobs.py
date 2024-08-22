from dagster import job, ScheduleDefinition
from mma_betting.ops.extract.ufcstats_ops import detect_ufcstats_events
from mma_betting.assets.extract.fetch_tapology import crawl_tapology
from mma_betting.resources.api_resources import UFCStatsAPIResource

@job
def crawler_job():
    crawl_tapology()

crawler_schedule = ScheduleDefinition(job=crawler_job, cron_schedule='5 8 * * 0')

@job
def detect_ufcstats_events_job():
    detect_ufcstats_events()

detect_ufcstats_events_schedule = ScheduleDefinition(job=detect_ufcstats_events_job, cron_schedule='5 8 * * 0')