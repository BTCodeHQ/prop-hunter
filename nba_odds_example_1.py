import requests
import re
import time
import pandas as pd
from datetime import date, datetime, timezone, timedelta
import psycopg2
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert

today = date.today()

def utc_to_local(utc_dt):
    # Converts UTC to Local timezone
    date_object = datetime.strptime(utc_dt, "%Y-%m-%dT%H:%M:%SZ")
    return date_object.replace(tzinfo=timezone.utc).astimezone(tz=None)

def pkey(row):
    # Creates a unique ID for each selection
    return (row['date'] + row['event'] + row['market'] + row['selection']).replace(' ','')

def get_ladbrokesNBA_odds():
    # Scrapes all relevant NBA markets from https://www.ladbrokes.com.au/
    print('Scraping Ladbrokes')
    url = 'https://api.ladbrokes.com.au/v2/sport/event-request?category_ids=%5B%223c34d075-dc14-436d-bfc4-9272a49c2b39%22%5D&competition_id=2d20a25b-6b96-4651-a523-442834136e2d'
    IDs = []

    my_markets = ['Head To Head','Margin 1-10','First Player to Score','Both Teams to Score 100+ Points',
                'Both Teams to Score 110+ Points','Both Teams to Score 120+ Points','Team With Highest Scoring Quarter',
                'Player to Have a Triple Double','Player to Have a Double Double','Leading Point Scorer','To Score 10+ Points',
                'To Score 15+ Points','To Score 20+ Points','To Score 25+ Points','To Score 30+ Points','To Score 35+ Points',
                'To Score 40+ Points','To Score 45+ Points','To Score 50+ Points','To Have 4+ Rebounds','To Have 6+ Rebounds',
                'To Have 8+ Rebounds','To Have 10+ Rebounds','To Have 12+ Rebounds','To Have 14+ Rebounds','To Have 16+ Rebounds',
                'To Have 18+ Rebounds','To Have 20+ Rebounds','To Have 4+ Assists','To Have 6+ Assists','To Have 8+ Assists',
                'To Have 10+ Assists','To Have 12+ Assists','To Have 14+ Assists','To Make 1+ Three Point FG','To Make 2+ Three Point FG',
                'To Make 3+ Three Point FG','To Make 4+ Three Point FG','To Make 5+ Three Point FG','To Have 1+ Blocks','To Have 2+ Blocks',
                'To Have 3+ Blocks','To Have 4+ Blocks','To Have 5+ Blocks','To Have 1+ Steals','To Have 2+ Steals','To Have 3+ Steals',
                'To Have 4+ Steals','To Have 5+ Steals','Race to 10','Race to 15','Race to 20','Race to 25','Race to 30','1st Quarter Head To Head',
                '1st Half Head To Head','2nd Quarter Head To Head','3rd Quarter Head To Head','4th Quarter Head To Head']

    market_cleanup = {'Margin 1-10':'Winning Team & Margin',
                    '1st Half Head To Head':'1st Half Winner',
                    '1st Quarter Head To Head':'1st Quarter Winner',
                    '2nd Quarter Head To Head':'2nd Quarter Winner',
                    '3rd Quarter Head To Head':'3rd Quarter Winner',
                    '4th Quarter Head To Head':'4th Quarter Winner',
                    'First Player to Score':'First Basket',
                    'Leading Point Scorer':'Top Points Scorer',
                    'Race to 10':'Race To 10 Points',
                    'Race to 15':'Race To 15 Points',
                    'Race to 20':'Race To 20 Points',
                    'Race to 25':'Race To 25 Points',
                    'Race to 30':'Race To 30 Points',
                    'Wire to Wire':'Wire To Wire',
                    'Player to Have a Triple Double':'To Record A Triple Double',
                    'Player to Have a Double Double':'To Record A Double Double',
                    'To Have 4+ Rebounds':'To Record 4+ Rebounds',
                    'To Have 6+ Rebounds':'To Record 6+ Rebounds',
                    'To Have 8+ Rebounds':'To Record 8+ Rebounds',
                    'To Have 10+ Rebounds':'To Record 10+ Rebounds',
                    'To Have 12+ Rebounds':'To Record 12+ Rebounds',
                    'To Have 14+ Rebounds':'To Record 14+ Rebounds',
                    'To Have 16+ Rebounds':'To Record 16+ Rebounds',
                    'To Have 18+ Rebounds':'To Record 18+ Rebounds',
                    'To Have 20+ Rebounds':'To Record 20+ Rebounds',
                    'To Have 4+ Assists':'To Record 4+ Assists',
                    'To Have 6+ Assists':'To Record 6+ Assists',
                    'To Have 8+ Assists':'To Record 8+ Assists',
                    'To Have 10+ Assists':'To Record 10+ Assists',
                    'To Have 12+ Assists':'To Record 12+ Assists',
                    'To Have 14+ Assists':'To Record 14+ Assists',
                    'To Have 1+ Blocks':'To Record 1+ Blocks',
                    'To Have 2+ Blocks':'To Record 2+ Blocks',
                    'To Have 3+ Blocks':'To Record 3+ Blocks',
                    'To Have 4+ Blocks':'To Record 4+ Blocks',
                    'To Have 5+ Blocks':'To Record 5+ Blocks',
                    'To Have 1+ Steals':'To Record 1+ Steals',
                    'To Have 2+ Steals':'To Record 2+ Steals',
                    'To Have 3+ Steals':'To Record 3+ Steals',
                    'To Have 4+ Steals':'To Record 4+ Steals',
                    'To Have 5+ Steals':'To Record 5+ Steals',

                    }

    r = requests.get(url)
    if 'events' in r.json():
        events = r.json()['events']
        for e in events:
            event = events.get(e)
            start_time = utc_to_local(event['actual_start'])
            if (event['name'][:3] != 'NBA') and str(start_time.date()) == str(today):
                IDs.append(e)

        scrape = []
        for i in IDs:
            url = f'https://api.ladbrokes.com.au/v2/sport/event-card?id={i}'
            r = requests.get(url)
            try:
                event = r.json()
            except:
                continue
            try:
                event_name = event['events'].get(i).get('name').replace('vs','v')
            except KeyError:
                continue

            event_date = str(today)
            for m in event['markets']:
                market_name = event['markets'].get(m).get('name')
                if market_name in my_markets:
                    market_name = market_cleanup.get(market_name,market_name)
                    entrantIDs = event['markets'].get(m).get('entrant_ids')
                    for sID in entrantIDs:
                        odds_code = f'{sID}:940b8704-e497-4a76-b390-00918ff7d282:'
                        selection_name = event['entrants'].get(sID).get('name')
                        selection_name = re.sub(r'\([^)]*\)','',selection_name).strip().replace('.','').title()
                        odds = round((event['prices'].get(odds_code)['odds']['numerator'] / event['prices'].get(odds_code)['odds']['denominator'])+1,2)
                        scrape.append([event_date,event_name,market_name,selection_name,odds])
        df = pd.DataFrame(scrape,columns=['date','event','market','selection','odds'])
        print('Success: ',len(df),' selections found')
        return df
    else:
        df = pd.DataFrame(columns=['date','event','market','selection','odds'])
        return df
        print('Failed: No selections found')

def get_sportsbetNBA_odds():
    # Scrapes all relevant NBA markets from https://www.sportsbet.com.au/
    print('Scraping Sportsbet')
    my_markets = ['Match Betting','Big Win Little Win','To Score 10+ Points','To Score 15+ Points','To Score 20+ Points',
                'To Score 25+ Points','To Score 30+ Points','To Score 35+ Points','To Score 40+ Points','To Score 45+ Points','To Score 50+ Points','To Record 4+ Rebounds','To Record 6+ Rebounds',
                'To Record 8+ Rebounds','To Record 10+ Rebounds','To Record 12+ Rebounds','To Record 14+ Rebounds','To Record 16+ Rebounds',
                'To Record 18+ Rebounds','To Record 20+ Rebounds','To Record 4+ Assists','To Record 6+ Assists','To Record 8+ Assists',
                'To Record 10+ Assists','To Record 12+ Assists','To Record 14+ Assists','1+ Made Threes','2+ Made Threes',
                '3+ Made Threes','4+ Made Threes','5+ Made Threes','To Record 1+ Blocks','To Record 2+ Blocks','To Record 3+ Blocks','To Record 4+ Blocks',
                'To Record 5+ Blocks','To Record 1+ Steals','To Record 2+ Steals','To Record 3+ Steals','To Record 4+ Steals','To Record 5+ Steals',
                '1st Half Winner','1st Quarter Winner','2nd Quarter Winner','3rd Quarter Winner','4th Quarter Winner',
                'Team to Score First','Team To Score Last','First Basket','First Team Basket Scorer','Top Points Scorer',
                'Race To 8 Points','Race To 10 Points','Race To 15 Points','Race To 20 Points','Will there be OverTime?',
                'Both Score 100+','To Record A Double Double','To Record A Triple Double']

    market_cleanup = {'Match Betting':'Head To Head',
                    'Big Win Little Win':'Winning Team & Margin',
                    'Team to Score First':'First Team To Score',
                    'Team To Score Last':'Last Team To Score',
                    'Will there be OverTime?':'Will There Be Overtime?',
                    '1+ Made Threes':'To Make 1+ Three Point FG',
                    '2+ Made Threes':'To Make 2+ Three Point FG',
                    '3+ Made Threes':'To Make 3+ Three Point FG',
                    '4+ Made Threes':'To Make 4+ Three Point FG',
                    '5+ Made Threes':'To Make 5+ Three Point FG',
                    'Both Score 100+':'Both Teams to Score 100+ Points'}

    url = 'https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Competitions/6927'
    response = requests.get(url)
    payload = response.json()['events']
    df_markets = []

    for event in payload:
        start_time = time.strftime('%Y-%m-%d', time.localtime(event['startTime']))
        if (start_time == str(today)) and (event['hasBIRStarted'] == False):
            eventID = event['id']
            event_name = event['name']
            try:
                away,home = event_name.split(' At ')
            except ValueError:
                break
            event_name = f'{home} v {away}'
            marketsURL = f'https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events/{eventID}/Markets'
            response = requests.get(marketsURL)
            payload2 = response.json()
            for market in payload2:
                if market['name'] in my_markets:
                    market_name = market['name']
                    market_name = market_cleanup.get(market_name,market_name)
                    selections = market['selections']
                    for s in selections:
                        selection_name = s['name'].replace('.','').replace(' - ','-').title()
                        event_date = str(today)
                        odds = s['price']['winPrice']
                        df_markets.append([event_date,event_name,market_name,selection_name,odds])
    df = pd.DataFrame(df_markets,columns=['date','event','market','selection','odds'])
    print('Success: ',len(df),' selections found')
    return df

def get_tabnzNBA_odds():
    # Scrapes all relevant NBA markets from https://www.tab.co.nz/
    print('Scraping TAB NZ')

    my_markets = ['Head To Head','Winning Team & Margin','Both Score 100+ Points',
                'First Half winner','First Quarter Winner',
                'Second Quarter Winner','Third Quarter Winner',
                'Fourth Quarter Winner','First Team To Score',
                'Last Team To Score','First Basket','First Team Basket Scorer',
                'Top Points Scorer','First To Eight Points','First To 10 Points',
                'First To 15 Points','First To 20 Points','Will There Be Overtime?']

    market_cleanup = {'First Half winner':'1st Half Winner',
                    'First Quarter Winner':'1st Quarter Winner',
                    'Second Quarter Winner':'2nd Quarter Winner',
                    'Third Quarter Winner':'3rd Quarter Winner',
                    'Fourth Quarter Winner':'4th Quarter Winner',
                    'First To Eight Points':'Race To 8 Points',
                    'First To 10 Points':'Race To 10 Points',
                    'First To 15 Points':'Race To 15 Points',
                    'First To 20 Points':'Race To 20 Points',
                    'Both Score 100+ Points':'Both Teams to Score 100+ Points'}

    response = requests.get('https://content.tab.co.nz/content-service/api/v1/q/event-list?drilldownTagIds=10804&includePriceHistory=true&includeChildMarkets=true')
    payload = response.json()
    events = payload['data']['events']
    scrape = []
    try:
        for e in events:
            if e['name'][:3] != 'NBA' or e['name'][-7:] != '2020-21':
                event_name = e['name']
                event_id = e['id']
                live_now = e['liveNow']
                start_time = e['startTime']
                start_time = utc_to_local(start_time)
                if str(start_time.date()) == str(today) and live_now == False:
                    markets = e['markets']
                    for m in markets:
                        market_name = m['name']
                        if market_name in my_markets:
                            market_name = market_cleanup.get(market_name,market_name)
                            outcomes = m['outcomes']
                            for selection in outcomes:
                                event_date = str(today)
                                selection_name = selection['name'].replace('.','').replace(' - ','-').title()
                                price = selection['prices'][0]['decimal']
                                scrape.append([event_date,event_name,market_name,selection_name,price])
        df = pd.DataFrame(scrape,columns=['date','event','market','selection','odds'])
        print('Success: ',len(df),' selections found')
    except:
        print('Error during scrape, skipping TAB NZ')
        df = pd.DataFrame(columns=['date','event','market','selection','odds'])
    return df


ladbrokes = get_ladbrokesNBA_odds()
sportsbet = get_sportsbetNBA_odds()
tabNZ = get_tabnzNBA_odds()

print('Merging Scrape')
df = ladbrokes.merge(sportsbet, on=['date','market','event','selection'],how='outer').merge(tabNZ,on=['date','market','event','selection'],how='outer')
df['odds'] = round(df.mean(axis=1),2)
final = ['date','event','market','selection','odds']
df = df[final]
try:
    df['pkey'] = df.apply(lambda row: pkey(row), axis=1)
    print(df)
except ValueError:
    print('No markets found')

if len(df) > 0:
    print('Upserting to PostgreSQL')
    engine = create_engine('POSTGRESQL ADDRESS') ### Removed database info
    conn = engine.connect()
    meta = MetaData(bind=engine)
    meta.reflect(bind=engine)
    nba_markets = meta.tables['POSTGRESQL TABLE NAME'] ### Removed database info

    # upsert data to postgres
    insert_statement = insert(nba_markets).values(df.to_dict(orient='records'))
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=['pkey'],
        set_={c.key: c for c in insert_statement.excluded if c.key != 'pkey'})

    # execute and close
    conn.execute(upsert_statement)
    conn.close()
    print('Success')
else:
    print('No markets found')
