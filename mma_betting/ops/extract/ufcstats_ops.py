from dagster import op, OpExecutionContext
from mma_betting.partitions import ufc_events_partitions_def
from mma_betting.resources.api_resources import UFCStatsAPIResource

@op
def detect_ufcstats_events(context: OpExecutionContext):
    api = UFCStatsAPIResource()
    existing_partitions = ufc_events_partitions_def.get_partition_keys(dynamic_partitions_store=context.instance)
    event_id = 1
    consecutive_failures = 0
    while consecutive_failures <= 5:
        if not str(event_id) in existing_partitions:
            context.log.debug(f'Checking UFC API for event {event_id}')
            event_exists = api.check_event_id(event_id)
            if event_exists:
                context.log.debug(f'Found event {event_id}, adding to UFC API events partition')
                context.instance.add_dynamic_partitions(ufc_events_partitions_def.name, [str(event_id)])
                consecutive_failures = 0
            else:
                context.log.debug(f'Event {event_id} does not exist')
                consecutive_failures += 1
        event_id += 1
