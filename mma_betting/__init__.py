from dagster import Definitions, load_assets_from_modules

from . import assets
from .jobs import crawler_schedule, detect_ufcstats_events_schedule

all_assets = load_assets_from_modules([assets])
all_schedules = [crawler_schedule, detect_ufcstats_events_schedule]

defs = Definitions(
    assets=all_assets,
    schedules=all_schedules
)
