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
        context.log.info(f'Fetching fights for event {i} of {len(fetch_events_fightodds)}')
        fights = api.fetch_event_fights(event['node']['pk'])
        all_fights.append(fights['data'])
    return all_fights

@asset(key_prefix='fightodds')
def fetch_fight_odds(context: AssetExecutionContext, fetch_fights_fightodds):
    all_odds = []
    i = 0
    for event in fetch_fights_fightodds:
        i += 1
        context.log.info(f'Fetching odds for event {i} of {len(fetch_fights_fightodds)}')
        fights = event['event']['fights']['edges']
        for fight in fights:
            odds = api.fetch_fight_odds(fight['node']['slug'])
            all_odds.append(odds)
    print(all_odds)
    return all_odds

@asset(key_prefix='fightodds')
def fetch_odds_history(fetch_fight_odds):
    pass