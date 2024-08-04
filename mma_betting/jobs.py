from dagster import job, ScheduleDefinition
from mma_betting.assets.extract.fetch_tapology import crawl_tapology

@job
def crawler_job():
    crawl_tapology()

crawler_schedule = ScheduleDefinition(job=crawler_job, cron_schedule='5 8 * * 0')