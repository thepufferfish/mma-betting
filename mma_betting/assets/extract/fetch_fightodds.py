from dagster import asset, AssetExecutionContext
from mma_betting.resources.api_resource import FightOddsAPIResource

api = FightOddsAPIResource()

@asset(key_prefix='fightodds')
def fetch_events_fightodds():
    events = api.fetch_events()
    return events

@asset(key_prefix='fightodds')
def fetch_fights_fightodds(context: AssetExecutionContext, fetch_events_fightodds):
    all_fights = []
    i = 0
    for event in fetch_events_fightodds:
        i += 1
        context.log.debug(f'Fetching fights for event {i} of {len(fetch_events_fightodds)}')
        fights = api.fetch_event_fights(event['node']['pk'])
        all_fights.append(fights['data'])
    return all_fights

@asset(key_prefix='fightodds')
def fetch_fight_odds(context: AssetExecutionContext, fetch_fights_fightodds):
    all_odds = []
    i = 0
    for event in fetch_fights_fightodds:
        i += 1
        context.log.debug(f'Fetching odds for event {i} of {len(fetch_fights_fightodds)}')
        fights = event['event']['fights']['edges']
        for fight in fights:
            odds = api.fetch_fight_odds(fight['node']['slug'])
            all_odds.append(odds)
    return all_odds

@asset(key_prefix='fightodds')
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