import requests
import os
import json
from pymongo import MongoClient
from datetime import datetime
from etf_screener import scrapeFunds as scrapeFundsETF
from time import sleep

# database_file = "funds_data.json"
timeout = 40
retry_count = 5
proxy_list = ["http://ip:port", "http://ip:port"]
mongo_connection_string = "mongodb://localhost:27017/"


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


def addToDb(data):
    print("Adding data to mongodb")
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db['fund_screener_data']
    # drop old data
    collection.delete_many({})
    collection.insert_many(data)
    print("Data added to mongodb")
    client.close()


def getFromDb(filter=None):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db['fund_screener_data']
    if filter:
        return list(collection.find(filter))
    return list(collection.find())


def scrapeFunds():
    link = 'https://tools.morningstar.co.uk/api/rest.svc/klr5zyak8x/security/screener'
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'dnt': '1',
        'referer': 'https://www.morningstar.co.uk/uk/screener/fund.aspx',
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
    page_no = 1
    data = []
    fails = 0
    with open('funds_sec_ids.txt', 'w+', encoding='utf-8') as f:
        while True:
            print("Checking page: {}".format(page_no))
            params = {
                'page': page_no,
                'pageSize': 10000,
                'sortOrder': 'LegalName asc',
                'outputType': 'json',
                'version': 1,
                'languageId': 'en-GB',
                'currencyId': 'GBP',
                'universeIds': 'FOGBR$$ALL|FOCHI$$ONS',
                'securityDataPoints': 'SecId|Name|PriceCurrency|TenforeId|LegalName|ClosePrice|Yield_M12|CategoryName|Medalist_RatingNumber|StarRatingM255|SustainabilityRank|GBRReturnD1|GBRReturnW1|GBRReturnM1|GBRReturnM3|GBRReturnM6|GBRReturnM0|GBRReturnM12|GBRReturnM36|GBRReturnM60|GBRReturnM120|MaxFrontEndLoad|OngoingCostActual|PerformanceFeeActual|TransactionFeeActual|MaximumExitCostAcquired|FeeLevel|ManagerTenure|MaxDeferredLoad|InitialPurchase|FundTNAV|EquityStyleBox|BondStyleBox|AverageMarketCapital|AverageCreditQualityCode|EffectiveDuration|MorningstarRiskM255|AlphaM36|BetaM36|R2M36|StandardDeviationM36|SharpeM36|InvestorTypeRetail|InvestorTypeProfessional|InvestorTypeEligibleCounterparty|ExpertiseBasic|ExpertiseAdvanced|ExpertiseInformed|ReturnProfilePreservation|ReturnProfileGrowth|ReturnProfileIncome|ReturnProfileHedging|ReturnProfileOther|TrackRecordExtension',
                'filters': '',
                'term': '',
                'subUniverseId': ''
            }
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                resp = requests.get(link, headers=headers, params=params, timeout=timeout, proxies=proxies).json()
            except:
                print("Failed to open {}".format(link))
                fails += 1
                if fails == retry_count:
                    break
                continue
            records = resp['rows']
            if len(records) == 0:
                break
            for fund in records:
                f.write(fund['SecId'] + '\n')
                data.append(fund)
                
            # with open(database_file, mode='w', encoding='utf-8') as f:
            #     json.dump(data, f, ensure_ascii=False, indent=4)
            page_no += 1
    addToDb(data)


if __name__ == '__main__':
    # data = json.load(open(database_file, mode='r', encoding='utf-8'))
    # addToDb(data)
    recordStatsOnDb("Fund Screener Started", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    scrapeFunds()
    recordStatsOnDb("Fund Screener Completed", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sleep(10)
    recordStatsOnDb("ETF Screener Started", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    scrapeFundsETF()
    recordStatsOnDb("ETF Screener Completed", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
