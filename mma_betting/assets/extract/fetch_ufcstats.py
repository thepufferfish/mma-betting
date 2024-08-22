from dagster import asset, AssetExecutionContext, DynamicOutput
from mma_betting.resources.api_resources import UFCStatsAPIResource
from mma_betting.partitions import ufc_events_partitions_def, ufc_fights_partitions_def

api = UFCStatsAPIResource()

@asset(
    key_prefix='ufc_stats',
    partitions_def=ufc_events_partitions_def
)
def fetch_ufc_event(context: AssetExecutionContext):
    event_id = context.partition_key
    context.log.debug(f'Fetching data for event {event_id}')
    event_data = api.fetch_event(event_id)
    fights = event_data['LiveEventDetail']['FightCard']
    for fight in fights:
        fight_id = fight['FightId']
        context.log.debug(f'Found fight {fight_id}. Adding to UFC fights partition')
        context.instance.add_dynamic_partitions(ufc_fights_partitions_def.name, [str(fight_id)])
    return event_data



@asset(
    key_prefix='ufc_stats',
    partitions_def=ufc_fights_partitions_def,
    deps=[fetch_ufc_event]
)
def fetch_ufc_fight(context: AssetExecutionContext):
    fight_id = context.partition_key
    context.log.debug(f'Fetching data for fight {fight_id}')
    fight_data = api.fetch_fight(fight_id)
    return fight_data