import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from dagster import ConfigurableResource

class FightOddsAPIResource(ConfigurableResource):

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
    def fetch_data(self, query, variables) -> dict:
        url = 'https://api.fightinsider.io/gql'
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, json={'query': query, 'variables': variables})
        response.raise_for_status()
        return response.json()
    
    def fetch_event_page(self, after: None) -> dict:
        query = '''
        query AllEventsQuery($after: String) {
            allEvents(first: 100, after: $after) {
                edges {
                node {
                    id
                    name
                    slug
                    date
                    venue
                    city
                    pk
                    promotion {
                    slug
                    shortName
                    id
                    }
                }
                cursor
                }
                pageInfo {
                hasNextPage
                endCursor
                }
            }
        }
        '''
        variables = {'after': after}
        return self.fetch_data(query, variables)

    def fetch_events(self) -> list:
        all_events = []
        after = None
        while True:
            data = self.fetch_event_page(after)
            events = data['data']['allEvents']['edges']
            all_events.extend(events)
            pageInfo = data['data']['allEvents']['pageInfo']
            
            if not pageInfo['hasNextPage']:
                break
            after = pageInfo['endCursor']
        return all_events
    
    def fetch_event_fights(self, event_pk) -> dict:
        query = '''
        query EventFightsQuery(
        $eventPk: Int
        ) {
        event: eventByPk(pk: $eventPk) {
            pk
            slug
            fights {
            ...EventTabPanelFights_fights
            }
            id
        }
        }

        fragment EventTabPanelFights_fights on FightNodeConnection {
        edges {
            node {
            id
            }
        }
        ...FightTable_fights
        ...FightCardList_fights
        }

        fragment FightCardList_fights on FightNodeConnection {
        edges {
            node {
            ...FightCard_fight
            id
            }
        }
        }

        fragment FightCard_fight on FightNode {
        id
        fighter1 {
            id
            firstName
            lastName
            slug
        }
        fighter2 {
            id
            firstName
            lastName
            slug
        }
        order
        weightClass {
            weightClass
            weight
            id
        }
        fightType
        slug
        }

        fragment FightTable_fights on FightNodeConnection {
        edges {
            node {
            id
            fighter1 {
                id
                firstName
                lastName
                slug
            }
            fighter2 {
                id
                firstName
                lastName
                slug
            }
            isCancelled
            fightType
            slug
            }
        }
        }
        '''
        variables = {'eventPk': event_pk}
        return self.fetch_data(query, variables)
    
    def fetch_fight_odds(self, fight_slug) -> dict:
        query = '''
        query FightPropOfferTableQrQuery(
        $fightSlug: String!
        ) {
            sportsbooks: allSportsbooks(hasOdds: true) {
                ...FightPropOfferTable_sportsbooks
            }
            fightPropOfferTable(slug: $fightSlug) {
                ...FightPropOfferTable_fightPropOfferTable
                id
            }
            offerTypes: allOfferTypes {
                ...FightPropOfferTable_offerTypes
            }
        }

        fragment FightPropOfferTable_fightPropOfferTable on FightPropOfferTableNode {
        propOffers {
            edges {
            node {
                propName1
                propName2
                offerType {
                id
                }
                offers {
                edges {
                    node {
                    sportsbook {
                        id
                    }
                    outcome1 {
                        id
                    }
                    outcome2 {
                        id
                    }
                    id
                    }
                }
                }
            }
            }
        }
        }

        fragment FightPropOfferTable_offerTypes on OfferTypeNodeConnection {
        edges {
            node {
                id
            }
        }
        }

        fragment FightPropOfferTable_sportsbooks on SportsbookNodeConnection {
        edges {
            node {
                id
                shortName
                slug
            }
        }
        }
        '''
        variables = {'fightSlug': fight_slug}
        return self.fetch_data(query, variables)
    
    def fetch_odds_history(self, outcome_id) -> dict:
        query = '''
        query PopoverOddsChartQuery(
        $outcomeId: ID!
        ) {
        odds: allOdds(outcome: $outcomeId, orderBy: "timestamp") {
            ...OddsChart_odds
            edges {
            node {
                timestamp
                id
            }
            }
        }
        outcome(id: $outcomeId) {
            ...OutcomeName_outcome
            ...AddBetForm_outcome
            ...DialogAddBet_outcome
            ...ButtonAddParlay_outcome
            offer {
            sportsbook {
                fullName
                slug
                shortName
                websiteUrl
                id
            }
            timestamp
            id
            }
            id
        }
        }

        fragment AddBetForm_outcome on OutcomeNode {
        ...OutcomeName_outcome
        id
        fighter {
            id
        }
        offer {
            fight {
            fighter1 {
                id
            }
            fighter2 {
                id
            }
            id
            }
            id
        }
        }

        fragment ButtonAddParlay_outcome on OutcomeNode {
        ...OutcomeName_outcome
        id
        odds
        isNot
        fighter {
            firstName
            lastName
            id
        }
        offer {
            offerType {
            offerTypeId
            description
            value
            id
            }
            fight {
            id
            fighter1 {
                id
            }
            fighter2 {
                id
            }
            }
            sportsbook {
            id
            }
            id
        }
        }

        fragment DialogAddBet_outcome on OutcomeNode {
        ...OutcomeName_outcome
        id
        isNot
        fighter {
            firstName
            lastName
            id
        }
        offer {
            offerType {
            offerTypeId
            description
            value
            id
            }
            sportsbook {
            fullName
            id
            }
            id
        }
        }

        fragment OddsChart_odds on OddsNodeConnection {
        edges {
            node {
            odds
            timestamp
            id
            }
        }
        }

        fragment OutcomeName_outcome on OutcomeNode {
        isNot
        fighter {
            firstName
            lastName
            id
        }
        offer {
            offerType {
            notDescription
            offerTypeId
            description
            value
            id
            }
            id
        }
        }
        '''
        variables = {'outcomeId': outcome_id}
        return self.fetch_data(query, variables)
    
class UFCStatsAPIResource(ConfigurableResource):

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
    def fetch_data(self, url) -> dict:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def fetch_event(self, event_id) -> dict:
        url = f'https://d29dxerjsp82wz.cloudfront.net/api/v3/event/live/{event_id}.json'
        return self.fetch_data(url)
    
    def fetch_fight(self, fight_id) -> dict:
        url = f'https://d29dxerjsp82wz.cloudfront.net/api/v3/fight/live/{fight_id}.json'
        return self.fetch_data(url)
    
    def check_event_id(self, event_id):
        event_data = self.fetch_event(event_id)
        if event_data.get('LiveEventDetail'):
            return True
