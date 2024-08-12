from dagster import asset, AssetExecutionContext
from mma_betting.resources.api_resource import UFCStatsAPIResource

api = UFCStatsAPIResource()

@asset(key_prefix='ufc_stats')
def fetch_ufc_events(context: AssetExecutionContext):
    all_events = []
    event_id = 1
    num_failed = 0
    while num_failed < 10:
        context.log.info(f'Fetching event {event_id}')
        event = api.fetch_event(event_id)
        if not event.get('LiveEventDetail'):
            num_failed += 1
        else:
            all_events.append(event)
        event_id += 1
    return all_events


@asset(key_prefix='ufc_stats')
def fetch_ufc_fights(context: AssetExecutionContext, fetch_ufc_events):
    all_fights = []
    for event in fetch_ufc_events:
        fights = event['LiveEventDetail']['FightCard']
        for fight in fights:
            fight_id = fight['FightId']
            context.log.info(f'Fetching fight {fight_id}')
            fight_data = api.fetch_fight(fight_id)
            all_fights.append(fight_data)
    return all_fights