from dagster import asset, AssetExecutionContext, AssetDep, AllPartitionMapping
from mma_betting.resources.api_resources import UFCStatsAPIResource
from mma_betting.partitions import ufc_events_partitions_def, ufc_fights_partitions_def

api = UFCStatsAPIResource()

@asset(
    key_prefix='ufc_stats',
    partitions_def=ufc_events_partitions_def,
    io_manager_key='mongo_io_manager'
)
def fetch_ufc_event(context: AssetExecutionContext):
    existing_fight_partitions = ufc_fights_partitions_def.get_partition_keys(dynamic_partitions_store=context.instance)
    event_id = context.partition_key
    context.log.debug(f'Fetching data for event {event_id}')
    event_data = api.fetch_event(event_id)
    fights = event_data['LiveEventDetail']['FightCard']
    for fight in fights:
        fight_id = fight['FightId']
        existing_fight = fight_id in existing_fight_partitions
        if not existing_fight:
            context.log.debug(f'Found fight {fight_id}. Adding to UFC fights partition')
            context.instance.add_dynamic_partitions(ufc_fights_partitions_def.name, [str(fight_id)])
        else:
            context.log.debug(f'Partition for {fight_id} already exists')
    return event_data['LiveEventDetail']

@asset(
    key_prefix='ufc_stats',
    partitions_def=ufc_fights_partitions_def,
    deps=[
        AssetDep(fetch_ufc_event, partition_mapping=AllPartitionMapping())
    ],
    io_manager_key='mongo_io_manager'
)
def fetch_ufc_fight(context: AssetExecutionContext):
    fight_id = context.partition_key
    context.log.debug(f'Fetching data for fight {fight_id}')
    fight_data = api.fetch_fight(fight_id)
    return fight_data['LiveFightDetail']