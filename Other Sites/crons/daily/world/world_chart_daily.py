import requests
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import traceback
from urllib.parse import unquote
import sys
if __name__ != "__main__":
    from crons.daily.world.mapping import world_api_map, world_fund_type_map
else:
    from mapping import world_api_map, world_fund_type_map


mongo_connection_string = "mongodb://localhost:27017/"
threads_count = 75
timeout = 40
retry_count = 5
processed = 0
empty_records = 0
proxy_list = ["http://ip:port", "http://ip:port"]
fund_type = None
country_code_db_name = None


def pickProxy():
    picked = proxy_list.pop(0)
    proxy_list.append(picked)
    return picked


def recordStatsOnDb(key, value):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db['stats']
    # get old stat if exist
    old_stat = collection.find_one()
    if old_stat:
        collection.update_one({}, {'$set': {key: value}})
    else:
        collection.insert_one({
            key: value
        })
    client.close()


headers = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'dnt': '1',
    'cookies': 'cookies=true;',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
}


def addToDB(collection, data):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection]
    if collection.find_one({'SecId': data['SecId']}):
        collection.update_one({'SecId': data['SecId']}, {'$set': data})
    else:
        collection.insert_one(data)
    client.close()


def getChartTimeSeries(id, performanceType):
    link = f'https://lt.morningstar.com/api/rest.svc/timeseries_cumulativereturn/3y3wd9echv'
    params = {
        'currencyId': 'GBP',
        'idtype': 'Morningstar',
        'frequency': 'daily',
        'startDate': '1970-01-01',
        'performanceType': performanceType,
        'outputType': 'COMPACTJSON',
        'id': id,
        'decPlaces': 8,
        'applyTrackRecordExtension': 'true',
        'restructureDateOptions': 'true'
    }
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, params=params, proxies=proxies).json()
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return []
    return resp


def getChartText(SecIds, fund_type):
    global empty_records
    loaded = False
    link = f'https://lt.morningstar.com/3y3wd9echv/snapshot/default.aspx?tab=7&SecurityToken={SecIds}%5D9%5D0%5D{fund_type}&Id={SecIds}&ClientFund=0&CurrencyId=GBP'
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, proxies=proxies).text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return []
    soup = bs(resp, 'html.parser')
    iframes = soup.find_all('iframe')
    universe_id = 'FOGBR$$ALL'
    SecIdList = SecIds
    for iframe in iframes:
        if hasattr(iframe, 'src') and 'SecurityTokenList=' in iframe.get('src'):
            item_split = unquote(iframe.get('src').split(
                'SecurityTokenList=')[1].split('&')[0])
            SecIdList = item_split.split(']')[0]
            universe_id = item_split.split(']')[-1]
            break
    # this works with multiple SecIds
    link = f'https://lt.morningstar.com/api/rest.svc/security_list/3y3wd9echv/'
    params = {
        'ModuleId': 1,
        'languageId': 'en-GB',
        'viewId': 'Snapshot',
        'ColumnList': 'Name,Isin,BaseCurrencyId,ExchangeId,CategoryId,CategoryName,LocalCategoryId,LocalCategoryName,PrimaryBenchmarkId,PrimaryBenchmarkName,CategoryPrimaryIndex,CategoryPrimaryIndexName,CustomBenchmarkId,CustomBenchmarkName,CustomBenchmarkId2,CustomBenchmarkName2,CEFIndexId,CEFIndexName,MSAICsectorCode,MSAICsectorName,IMASectorId,IMASectorName,InceptionDate,CustomTaxOption,CustomRestructureDate,InvestmentTypeId,HoldingTypeId,domicileCountryId',
        'outputtype': 'compactjson',
        'v': '1.1',
        'SecIdList': SecIdList,
        'multipleuniverseids': universe_id
    }
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, params=params, proxies=proxies).json()
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return []
    rows = resp['rows']
    all_charts = []
    records_ok = False
    for row in rows:
        id = row['id']
        final_data = {
            'SecId': id,
            'charts': []
        }
        cell = row['cell']
        names = [
            cell[0],
            cell[11],
            cell[5],
            # cell[7],
            # cell[13]
        ]
        name_ids = [
            ('', id + "]8]0]{}".format(fund_type)),
            ('', cell[10] + "]7]0]IXALL$$ALL"),
            ('weighteddailymarketreturnindex', cell[4] + "]8]0]CAALL$$ALL"),
            # ('weighteddailymarketreturnindex', cell[6] + "]8]0]CAALL$$ALL")
        ]
        names = [name for name in names if name != '']
        name_ids = [
            name_id for name_id in name_ids if not name_id[1].startswith(']')]
        print(names)
        print(name_ids)
        for idx, (param, id) in enumerate(name_ids):
            chart = getChartTimeSeries(id, param)
            if chart:
                records_ok = True
            try:
                final_data['charts'].append({
                    'name': names[idx],
                    'timeseries': chart
                })
            except:
                pass
        all_charts.append(final_data)
    if not records_ok:
        empty_records += 1
    return all_charts


def processInThreadWorld(SecId):
    global processed
    processed += 1
    print(f"Processing {SecId}")
    global fund_type
    global country_code_db_name
    try:
        charts = getChartText(SecId, fund_type)
        for chart in charts:
            addToDB(f'{country_code_db_name}_charts', chart)
    except:
        with open('errors.txt', 'a') as f:
            f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} <country_code>")
        sys.exit(1)
    country_code = sys.argv[1].lower()
    try:
        threads_count = int(sys.argv[2])
    except:
        threads_count = 20
    if country_code not in world_fund_type_map:
        print("Invalid country code")
        sys.exit(1)
    fund_type = world_fund_type_map[country_code]
    country_code_db_name = world_api_map[country_code]
    recordStatsOnDb(f"World {country_code.upper()} Chart Daily Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(f'{country_code_db_name}_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        executor.map(processInThreadWorld, sec_ids)
    recordStatsOnDb(f"World {country_code.upper()} Chart Daily Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb(f"World {country_code.upper()} Chart Daily Processed", processed)
    recordStatsOnDb(f"World {country_code.upper()} Chart Daily Empty", empty_records)
