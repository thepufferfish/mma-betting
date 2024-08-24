import os
from dagster import Definitions, load_assets_from_modules

from . import assets
from .jobs import crawler_schedule, detect_ufcstats_events_schedule, fetch_fightodds_events_list_schedule
from .io_managers.mongo_io_manager import MongoDBIOManager

all_assets = load_assets_from_modules([assets])
all_schedules = [
    crawler_schedule, detect_ufcstats_events_schedule, fetch_fightodds_events_list_schedule
]

defs = Definitions(
    assets=all_assets,
    schedules=all_schedules,
    resources={
        'mongo_io_manager': MongoDBIOManager(
            connection_string=os.getenv('MONGO_URI'),
            database_name=os.getenv('MONGO_DATABASE')
        )
    }
)
