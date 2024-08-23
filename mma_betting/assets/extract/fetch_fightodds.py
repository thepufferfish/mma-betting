from dagster import asset, AssetExecutionContext
from mma_betting.resources.api_resources import FightOddsAPIResource
from mma_betting.partitions import fightodds_events_partitions_def, fightodds_fights_partitions_def

api = FightOddsAPIResource()

@asset(key_prefix='fightodds')
def fetch_events_list_fightodds(context: AssetExecutionContext):
    context.log.debug('Fetching events list from fightodds.io API')
    events = api.fetch_events()
    for event in events:
        event_pk = str(event['node']['pk'])
        context.log.debug(f'Adding event {event_pk} to fightodds events partitions')
        context.instance.add_dynamic_partitions(fightodds_events_partitions_def.name, [event_pk])
    return events

@asset(
    key_prefix='fightodds',
    partitions_def=fightodds_events_partitions_def,
    deps=[fetch_events_list_fightodds]
)
def fetch_event_fights_fightodds(context: AssetExecutionContext):
    event_pk = context.partition_key
    context.log.debug(f'Fetching data for event {event_pk} from fightodds.io API')
    event_fights = api.fetch_event_fights(event_pk)
    fights = event_fights['data']['event']['fights']['edges']
    if fights:
        for fight in fights:
            fight_slug = str(fight['node']['slug'])
            context.log.debug(f'Adding fight {fight_slug} to fightodds fights partitions')
            context.instance.add_dynamic_partitions(fightodds_fights_partitions_def.name, [fight_slug])
        return event_fights
    else:
        context.log.info(f'No fights found for event {event_pk}')

@asset(
    key_prefix='fightodds',
    partitions_def=fightodds_fights_partitions_def,
    deps=[fetch_event_fights_fightodds]
)
def fetch_fight_odds(context: AssetExecutionContext):
    fight_slug = context.partition_key
    context.log.debug(f'Fetching data for fight {fight_slug} from fightodds.io API')
    odds = api.fetch_fight_odds(fight_slug)
    return odds

@asset(
    key_prefix='fightodds',
    partitions_def=fightodds_fights_partitions_def,
    deps=[fetch_fight_odds]
)
def fetch_odds_history(context: AssetExecutionContext, fetch_fight_odds):
    all_history = []
    i = 0
    for fight in fetch_fight_odds:
        i += 1
        context.log.debug(f'Fetching odds history for fight {i} of {len(fetch_fight_odds)}')
        fight_props = fight['data']['fightPropOfferTable']['propOffers']['edges']
        for prop in fight_props:
            offers = prop['node']['offers']['edges']
            for offer in offers:
                outcomes = [offer['node'].get('outcome1'), offer['node'].get('outcome2')]
                for outcome in outcomes:
                    if outcome:
                        history = api.fetch_odds_history(outcome['id'])
                        all_history.append(history)
    return all_history