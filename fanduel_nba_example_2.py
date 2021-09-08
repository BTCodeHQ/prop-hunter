import pandas as pd
import requests

# Scrapes all open markets from Fanduel for all open NBA events

url = f'https://sportsbook.fanduel.com/cache/psmg/UK/63747.3.json'
events = requests.get(url).json()['events']
eventIDs = [x['idfoevent'] for x in events]

scrape = []
for eID in eventIDs:
    url = f'https://sportsbook.fanduel.com/cache/psevent/UK/1/false/{eID}.json'
    payload = requests.get(url).json()
    if 'eventmarketgroups' in payload:
        for g in payload['eventmarketgroups']:
            if g['name'] == 'All':
                markets = g['markets']
                for m in markets:
                    event_name = m['eventname']
                    market_name = m['name']
                    selections = m['selections']
                    for s in selections:
                        selection_name = s['name']
                        selection_price = round((s['currentpriceup'] / s['currentpricedown']) + 1,2)
                        scrape.append([event_name,market_name,selection_name,selection_price])
df = pd.DataFrame(scrape,columns=['event_name','market_name','selection_name','selection_price'])
print(df)
# df.to_csv('Fanduel_scrape.csv')
