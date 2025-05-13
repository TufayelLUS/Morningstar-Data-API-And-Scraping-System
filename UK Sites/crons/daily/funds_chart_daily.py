import requests
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import traceback
from etf_chart_daily import getChartText as getChartTextETF


# mongo_connection_string = "mongodb://localhost:27017/"
threads_count = 20
timeout = 40
retry_count = 5
proxy_list = ["http://ip:port", "http://ip:port"]
processed = 0
completed = 0

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


def cleanUpDB():
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db['fund_charts_temp']
    collection.delete_many({})
    collection = db['etf_charts_temp']
    collection.delete_many({})
    client.close()


def addToDB(collection, data):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection]
    if collection.find_one({'SecId': data['SecId']}):
        collection.update_one({'SecId': data['SecId']}, {'$set': data})
    else:
        collection.insert_one(data)
    client.close()


def addToDBDirect(collection, data):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection]
    collection.insert_one(data)
    client.close()


def migrateNewDBToOld():
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection_new_fund = db['fund_charts_temp']
    collection_new_etf = db['etf_charts_temp']
    collection_old_fund = db['fund_charts']
    collection_old_etf = db['etf_charts']
    # Migrate fund data
    for item in collection_new_fund.find():
        secid = item.get("SecId")  # Ensure SecId exists
        if secid:
            item.pop("_id", None)  # Remove _id field before update
            collection_old_fund.update_one({'SecId': secid}, {'$set': item}, upsert=True)
    # Migrate ETF data
    for item in collection_new_etf.find():
        secid = item.get("SecId")
        if secid:
            item.pop("_id", None)
            collection_old_etf.update_one({'SecId': secid}, {'$set': item}, upsert=True)
    client.close()



def getChartTimeSeries(id, performanceType):
    link = f'https://tools.morningstar.co.uk/api/rest.svc/timeseries_cumulativereturn/t92wz0sj7c'
    params = {
        'currencyId': 'GBP',
        'idtype': 'Morningstar',
        'frequency': 'daily',
        'startDate': '1970-01-01',
        'performanceType': performanceType,
        'outputType': 'COMPACTJSON',
        'id': id,
        'decPlaces': 8,
        'applyTrackRecordExtension': 'true'
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


def getChartText(SecIds):
    loaded = False
    link = f'https://www.morningstar.co.uk/uk/funds/snapshot/snapshot.aspx?id={SecIds}&tab=13'
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
            item_split = iframe.get('src').split('SecurityTokenList=')[1].split('&')[0]
            SecIdList = item_split.split(']')[0]
            universe_id = item_split.split(']')[-1]
            break
    # this works with multiple SecIds
    link = f'https://tools.morningstar.co.uk/api/rest.svc/security_list/t92wz0sj7c/'
    params = {
        'ModuleId': 131,
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
            cell[7],
            cell[13]
        ]
        name_ids = [
            ('', id + "]2]0]FOGBR$$ALL"),
            ('', cell[10] + "]7]0]IXALL$$ALL"),
            ('weighteddailymarketreturnindex', cell[4] + "]8]0]CAALL$$ALL"),
            ('weighteddailymarketreturnindex', cell[6] + "]8]0]CAALL$$ALL")
        ]
        names = [name for name in names]
        name_ids = [
            name_id for name_id in name_ids if not name_id[1].startswith(']')]
        print(names)
        print(name_ids)
        for idx, (param, id) in enumerate(name_ids):
            chart = getChartTimeSeries(id, param)
            final_data['charts'].append({
                'name': names[idx],
                'timeseries': chart
            })
        # remove empty timeseries
        final_data['charts'] = [
            chart for chart in final_data['charts'] if chart['timeseries']]
        all_charts.append(final_data)

    return all_charts


def processInThread(SecId):
    global processed
    global completed
    print(f"Processing {SecId}")
    try:
        charts = getChartText(SecId)
        processed += 1
        for chart in charts:
            addToDBDirect('fund_charts_temp', chart)
            completed += 1
    except:
        with open('errors.txt', 'a') as f:
            f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def processInThreadETF(SecId):
    global processed
    global completed
    print(f"Processing {SecId}")
    try:
        charts = getChartTextETF(SecId)
        processed += 1
        for chart in charts:
            addToDBDirect('etf_charts_temp', chart)
            completed += 1
    except:
        with open('errors.txt', 'a') as f:
            f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    cleanUpDB()
    recordStatsOnDb("Funds Chart Daily Started", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open('funds_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("Funds Chart Daily Completed", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb("Funds Chart Daily Processed Count", processed)
    recordStatsOnDb("Funds Chart Daily Completed Count", completed)
    # reset the counter again
    processed = 0
    completed = 0
    recordStatsOnDb("ETF Chart Daily Started", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open('etf_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        executor.map(processInThreadETF, sec_ids)
    recordStatsOnDb("ETF Chart Daily Completed", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb("ETF Chart Daily Processed Count", processed)
    recordStatsOnDb("ETF Chart Daily Completed Count", completed)
    recordStatsOnDb("Funds & ETF Charts Migration Started", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    migrateNewDBToOld()
    recordStatsOnDb("Funds & ETF Charts Migration Completed", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
