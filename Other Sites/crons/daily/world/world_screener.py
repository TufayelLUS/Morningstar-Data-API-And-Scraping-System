import requests
import os
import json
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from time import sleep
import re
from bs4 import BeautifulSoup as bs
import traceback
from threading import Thread
import sys
from mapping import world_api_map, world_fund_type_map

# plan is to call the script using python3 world_screener.py <country_code>

# database_file = "funds_data.json"
threads = 1
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


def addToDb(data, collection, fund_type):
    print("Adding data to mongodb")
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection]
    # drop old data
    collection.delete_many({"FundType": fund_type})
    batch_size = 1000
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        collection.insert_many(batch)
    print("Data added to mongodb")
    client.close()


def updateInDb(data, collection, batch_size=1000):
    # print("Updating data to mongodb")
    # client = MongoClient(mongo_connection_string)
    # db = client['morningstar']
    # collection = db[collection]
    # for doc in data:
    #     try:
    #         collection.update_one({'SecId': doc['SecId']}, {'$set': doc})
    #     except:
    #         pass
    # client.close()
    print("Updating data to mongodb")
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection]

    operations = []
    for i, doc in enumerate(data):
        if 'SecId' in doc:
            cleaned_doc = {k: v for k, v in doc.items() if k.strip() != ''}
            operations.append(UpdateOne({'SecId': doc['SecId']}, {
                              '$set': cleaned_doc}, upsert=False))

        if len(operations) == batch_size:
            try:
                collection.bulk_write(operations, ordered=False)
            except Exception as e:
                print(f"Bulk write error: {e}")
            operations = []

    if operations:
        try:
            collection.bulk_write(operations, ordered=False)
        except Exception as e:
            print(f"Bulk write error: {e}")
    client.close()


def getFromDb(filter=None, collection_name='fund_screener_data'):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection_name]
    if filter:
        return list(collection.find(filter))
    return list(collection.find())


def scrapeFunds(fund_type, collection_name, f=None):
    s = requests.Session()
    link = 'https://lt.morningstar.com/3y3wd9echv/fundquickrank/default.aspx?Universe={}&LanguageId=en-GB'.format(
        fund_type)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        # 'accept-encoding': 'gzip, deflate, br',
        # 'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        # 'cookie': '_ga_8R1W3TJHY4=GS1.1.1744724220.1.1.1744724227.0.0.0; _ga_1E5VHFNL9Z=GS1.1.1744724220.1.1.1744724227.0.0.0; __utmb=192614060.3.9.1744724225521; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Apr+15+2025+19%3A37%3A07+GMT%2B0600+(Bangladesh+Standard+Time)&version=6.27.0&isIABGlobal=false&hosts=&consentId=9a25157e-0168-42df-bcc7-17ff690f197b&interactionCount=1&landingPath=https%3A%2F%2Fwww.morningstar.co.uk%2Fuk%2Flnpquickrank%2Fdefault.aspx&groups=C0001%3A1%2CC0003%3A0%2CC0002%3A0%2CC0004%3A0',
        'dnt': '1',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
    }
    page_no = 1
    ready = False
    data_final = []
    fails = 0
    while True:
        print("Checking page: {}".format(page_no))
        if page_no > 1:
            soup = bs(resp, 'html.parser')
            form = soup.find('form', {'id': 'aspnetForm'})
            __VIEWSTATE = form.find(
                'input', {'id': '__VIEWSTATE'}).get('value')
            __VIEWSTATEGENERATOR = form.find(
                'input', {'id': '__VIEWSTATEGENERATOR'}).get('value')
            __EVENTVALIDATION = form.find(
                'input', {'id': '__EVENTVALIDATION'}).get('value')
            rv_token = re.findall(
                r'id="ctl00___RequestVerificationToken" value="(.*?)"', resp)[0]
            data = {
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlPageSize',
                '__EVENTARGUMENT': page_no,
                'ctl00_ContentPlaceHolder1_aFundQuickrankControl_scrtmgrFundQuickrank_HiddenField': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': __VIEWSTATE,
                '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                '__VIEWSTATEENCRYPTED': '',
                '__EVENTVALIDATION': __EVENTVALIDATION,
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$msTopBar$ddlTitleBarCurrency': 'BAS',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$msTopBar$ddlTitleBarTools': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlUniverse': '{}'.format(fund_type),
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlBrandingName': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlIncOrAcc': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlCategory': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlSustainablePreference': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$txtSearchKey': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnFilterBySelection': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlPageSize': '500',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnSelectedFunds': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnSelectedIndex': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnFirstFundNavDate': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnCustomImageFileIds': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnSelectedSecurityCount': '0',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnTabs': 'Snapshot,Performance,ShortTerm,Portfolio,FeesAndDetails,CalendarYear,Documents,Pricing',
                'ctl00$__RequestVerificationToken': rv_token
            }
            if not ready:
                page_no -= 1
                print("Setting page count to 500")
                data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlPageSize'
                data['__EVENTARGUMENT'] = ''
                ready = True
            else:
                data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$aFundQuickrankControl$AspNetPager'
                data['__EVENTARGUMENT'] = page_no
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-encoding': 'gzip, deflate, br',
                'cache-control': 'max-age=0',
                'content-type': 'application/x-www-form-urlencoded',
                # 'cookie': 'cookies=true; RT_uk_LANG=en-GB; ASP.NET_SessionId=qq3axobjnclaumyea1tqvtji; __RequestVerificationToken=ApqcXIOFsAg5OXnFusC-MdjfpgfgBedZrJuc0F_BYycO7J6DCL6Q-0-u4smycwr5qRf7m6RizhHdfy5J00tFIYwhXX1hYdgqkCDy5h6Aqck1',
                'dnt': '1',
                'origin': 'https://lt.morningstar.com',
                'priority': 'u=0, i',
                'referer': 'https://lt.morningstar.com/3y3wd9echv/fundquickrank/default.aspx?Universe={}&LanguageId=en-GB'.format(fund_type),
                'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
            }
            try:
                # print(json.dumps(data, indent=4))
                resp = s.post(link, headers=headers,
                              data=data, timeout=timeout, proxies=proxies).text
                # open('resp.html', mode='w+', encoding='utf-8').write(resp)
            except:
                print("Failed to open {}".format(link))
                fails += 1
                if fails == retry_count:
                    break
                continue
        else:
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            print("Loading homepage")
            try:
                resp = s.get(link, headers=headers,
                             timeout=timeout, proxies=proxies).text
                page_no += 1
                # open('resp.html', mode='w+', encoding='utf-8').write(resp)
                continue
            except:
                print("Failed to open {}".format(link))
                fails += 1
                if fails == retry_count:
                    break
                continue
        soup = bs(resp, 'html.parser')
        table = soup.find(
            'table', {'id': 'ctl00_ContentPlaceHolder1_aFundQuickrankControl_gridResult'})
        if not table:
            print("No records table found")
            break
        table_rows = table.find_all('tr')
        headers_name = []
        headers_ready = False
        for row in table_rows:
            all_td = row.children
            column_texts = []
            secId = None
            json_mapping = {}
            for idx, td in enumerate(all_td):
                # check if it's a th
                if td.name == 'th':
                    headers_name.append(td.get_text(
                        separator='\n').strip().replace('\n', ' '))
                else:
                    try:
                        if 'snapshot.aspx?id=' in td.find('a').get('href'):
                            secId = td.find('a', href=True).get(
                                'href').split('id=')[1].split('&')[0]
                            f.write(secId + '\n')
                    except:
                        pass
                    try:
                        if 'gridStarRating' in td.get('class', []):
                            try:
                                column_texts.append(td.find('img').get(
                                    'src').split('/')[-1].split('.')[0])
                            except:
                                column_texts.append(td.text.strip())
                        elif 'gridMedalistRatingNumber' in td.get('class', []):
                            try:
                                column_texts.append(
                                    "https://lt.morningstar.com/" + td.find('img').get('src'))
                            except:
                                column_texts.append(td.text.strip())
                        elif 'gridDocument' in td.get('class', []):
                            try:
                                doc_link = td.find(
                                    'a', href=True).get('href')
                                column_texts.append(
                                    'https:' + doc_link if doc_link.startswith('//') else doc_link)
                            except:
                                column_texts.append(td.text.strip())
                        elif td.get('title'):
                            column_texts.append(td.get('title').strip())
                        elif td.find('img'):
                            column_texts.append(
                                td.find('img').get('title'))
                        elif td.find('a'):
                            column_texts.append(td.find('a').get('title'))
                        else:
                            column_texts.append(td.text.strip())
                    except:
                        column_texts.append(td.text.strip())
            if not secId:
                continue
            if not headers_ready:
                headers_name = headers_name[2:]
                headers_ready = True
            column_texts = column_texts[3:-1]
            for header in headers_name:
                json_mapping[header] = ""
            json_mapping['SecId'] = secId
            json_mapping['FundType'] = fund_type
            counter = 0
            for index in range(len(headers_name)):
                if headers_name[counter] == '':
                    headers_name[counter] = 'Currency'
                json_mapping[headers_name[counter]] = column_texts[index]
                counter += 1
            # remove empty key 
            for key in list(json_mapping.keys()):
                if key == '':
                    del json_mapping[key]
            print(json_mapping)
            data_final.append(json_mapping)
        pagination_matches = soup.find(
            'td', {'class': 'ms_page_custom_label'}).text.strip()
        matches = re.findall(
            r'([\d]+)\-([\d]+) of ([\d]+)', pagination_matches)
        if matches:
            start, end, total = map(int, matches[0])
            print(f"Processed {start}-{end} of {total}")
            if end == total:
                break
        page_no += 1
    addToDb(data_final, collection_name, fund_type)


def scrapeOtherTabs(tab, fund_type, collection_name):
    s = requests.Session()
    link = 'https://lt.morningstar.com/3y3wd9echv/fundquickrank/default.aspx?Universe={}&LanguageId=en-GB'.format(
        fund_type)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        # 'accept-encoding': 'gzip, deflate, br',
        # 'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        # 'cookie': '_ga_8R1W3TJHY4=GS1.1.1744724220.1.1.1744724227.0.0.0; _ga_1E5VHFNL9Z=GS1.1.1744724220.1.1.1744724227.0.0.0; __utmb=192614060.3.9.1744724225521; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Apr+15+2025+19%3A37%3A07+GMT%2B0600+(Bangladesh+Standard+Time)&version=6.27.0&isIABGlobal=false&hosts=&consentId=9a25157e-0168-42df-bcc7-17ff690f197b&interactionCount=1&landingPath=https%3A%2F%2Fwww.morningstar.co.uk%2Fuk%2Flnpquickrank%2Fdefault.aspx&groups=C0001%3A1%2CC0003%3A0%2CC0002%3A0%2CC0004%3A0',
        'dnt': '1',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
    }
    page_no = 1
    ready = False
    switched_to_tab = False
    data_final = []
    fails = 0
    while True:
        print("Checking page: {}".format(page_no))
        if page_no > 1:
            soup = bs(resp, 'html.parser')
            form = soup.find('form', {'id': 'aspnetForm'})
            __VIEWSTATE = form.find(
                'input', {'id': '__VIEWSTATE'}).get('value')
            __VIEWSTATEGENERATOR = form.find(
                'input', {'id': '__VIEWSTATEGENERATOR'}).get('value')
            __EVENTVALIDATION = form.find(
                'input', {'id': '__EVENTVALIDATION'}).get('value')
            rv_token = re.findall(
                r'id="ctl00___RequestVerificationToken" value="(.*?)"', resp)[0]
            data = {
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlPageSize',
                '__EVENTARGUMENT': page_no,
                'ctl00_ContentPlaceHolder1_aFundQuickrankControl_scrtmgrFundQuickrank_HiddenField': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': __VIEWSTATE,
                '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                '__VIEWSTATEENCRYPTED': '',
                '__EVENTVALIDATION': __EVENTVALIDATION,
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$msTopBar$ddlTitleBarCurrency': 'BAS',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$msTopBar$ddlTitleBarTools': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlUniverse': '{}'.format(fund_type),
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlBrandingName': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlIncOrAcc': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlCategory': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlSustainablePreference': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$txtSearchKey': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnFilterBySelection': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlPageSize': '500',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnSelectedFunds': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnSelectedIndex': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnFirstFundNavDate': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnCustomImageFileIds': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnSelectedSecurityCount': '0',
                'ctl00$ContentPlaceHolder1$aFundQuickrankControl$hdnTabs': 'Snapshot,Performance,ShortTerm,Portfolio,FeesAndDetails,CalendarYear,Documents,Pricing',
                'ctl00$__RequestVerificationToken': rv_token
            }
            if not ready:
                page_no -= 1
                print("Setting page count to 500")
                data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$aFundQuickrankControl$ddlPageSize'
                data['__EVENTARGUMENT'] = ''
                ready = True
            elif not switched_to_tab:
                page_no -= 1
                print("Switching to {} tab".format(tab))
                data['__EVENTTARGET'] = 'TabAction'
                data['__EVENTARGUMENT'] = tab
                switched_to_tab = True
            else:
                data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$aFundQuickrankControl$AspNetPager'
                data['__EVENTARGUMENT'] = page_no
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-encoding': 'gzip, deflate, br',
                'cache-control': 'max-age=0',
                'content-type': 'application/x-www-form-urlencoded',
                # 'cookie': 'cookies=true; RT_uk_LANG=en-GB; ASP.NET_SessionId=qq3axobjnclaumyea1tqvtji; __RequestVerificationToken=ApqcXIOFsAg5OXnFusC-MdjfpgfgBedZrJuc0F_BYycO7J6DCL6Q-0-u4smycwr5qRf7m6RizhHdfy5J00tFIYwhXX1hYdgqkCDy5h6Aqck1',
                'dnt': '1',
                'origin': 'https://lt.morningstar.com',
                'priority': 'u=0, i',
                'referer': 'https://lt.morningstar.com/3y3wd9echv/fundquickrank/default.aspx',
                'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
            }
            try:
                # print(json.dumps(data, indent=4))
                resp = s.post(link, headers=headers,
                              data=data, timeout=timeout, proxies=proxies).text
                # open('resp.html', mode='w+', encoding='utf-8').write(resp)
            except:
                print("Failed to open {}".format(link))
                fails += 1
                if fails == retry_count:
                    break
                continue
        else:
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            print("Loading homepage")
            try:
                resp = s.get(link, headers=headers,
                             timeout=timeout, proxies=proxies).text
                page_no += 1
                # open('resp.html', mode='w+', encoding='utf-8').write(resp)
                continue
            except:
                print("Failed to open {}".format(link))
                fails += 1
                if fails == retry_count:
                    break
                continue
        if not switched_to_tab:
            continue
        soup = bs(resp, 'html.parser')
        table = soup.find(
            'table', {'id': 'ctl00_ContentPlaceHolder1_aFundQuickrankControl_gridResult'})
        if not table:
            print("No records table found")
            break
        table_rows = table.find_all('tr')
        headers_name = []
        headers_ready = False
        for row in table_rows:
            all_td = row.children
            column_texts = []
            secId = None
            json_mapping = {}
            for idx, td in enumerate(all_td):
                # check if it's a th
                if td.name == 'th':
                    headers_name.append(td.get_text(
                        separator='\n').strip().replace('\n', ' '))
                else:
                    try:
                        if 'snapshot.aspx?id=' in td.find('a').get('href'):
                            secId = td.find('a', href=True).get(
                                'href').split('id=')[1].split('&')[0]
                    except:
                        pass
                    try:
                        if 'gridClosePrice' in td.get('class', []):
                            column_texts.append(td.text.strip())
                        elif 'gridStarRating' in td.get('class', []):
                            try:
                                column_texts.append(td.find('img').get(
                                    'src').split('/')[-1].split('.')[0])
                            except:
                                column_texts.append(td.text.strip())
                        elif 'gridMedalistRatingNumber' in td.get('class', []):
                            try:
                                column_texts.append(
                                    "https://lt.morningstar.com/" + td.find('img').get('src'))
                            except:
                                column_texts.append(td.text.strip())
                        elif 'gridDocument' in td.get('class', []):
                            try:
                                doc_link = td.find('a', href=True).get('href')
                                column_texts.append(
                                    'https:' + doc_link if doc_link.startswith('//') else doc_link)
                            except:
                                column_texts.append(td.text.strip())
                        elif td.get('title'):
                            column_texts.append(td.get('title').strip())
                        elif td.find('img'):
                            column_texts.append(td.find('img').get('title'))
                        elif td.find('a'):
                            column_texts.append(td.find('a').get('title'))
                        else:
                            column_texts.append(td.text.strip())
                    except:
                        column_texts.append(td.text.strip())
            if not secId:
                continue
            if not headers_ready:
                headers_name = headers_name[2:]
                headers_ready = True
            column_texts = column_texts[3:-1]
            for header in headers_name:
                json_mapping[header] = ""
            json_mapping['SecId'] = secId
            counter = 0
            for index in range(len(headers_name)):
                if headers_name[counter] == '':
                    headers_name[counter] = 'Currency'
                json_mapping[headers_name[counter]] = column_texts[index]
                counter += 1
            # remove empty key 
            for key in list(json_mapping.keys()):
                if key == '':
                    del json_mapping[key]
            print(json_mapping)
            data_final.append(json_mapping)
        pagination_matches = soup.find(
            'td', {'class': 'ms_page_custom_label'}).text.strip()
        matches = re.findall(
            r'([\d]+)\-([\d]+) of ([\d]+)', pagination_matches)
        if matches:
            start, end, total = map(int, matches[0])
            print(f"Processed {start}-{end} of {total}")
            if end == total:
                break
        page_no += 1
    updateInDb(data_final, collection_name)


def worldThread(fund_type, country_code, country_code_db_name, file_handle):
    recordStatsOnDb(f"World {country_code.upper()} Fund Screener Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    try:
        scrapeFunds(
            fund_type, f'{country_code_db_name}_screener_data', file_handle)
    except:
        print(
            f"Failed to scrape World {country_code.upper()} funds due to an exception")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    try:
        scrapeOtherTabs('ShortTerm', fund_type,
                        f'{country_code_db_name}_screener_data')
    except:
        print(f"Failed to scrape short term tab due to an exception")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    try:
        scrapeOtherTabs('Performance', fund_type,
                        f'{country_code_db_name}_screener_data')
    except:
        print(f"Failed to scrape performance tab due to an exception")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    try:
        scrapeOtherTabs('Portfolio', fund_type,
                        f'{country_code_db_name}_screener_data')
    except:
        print(f"Failed to scrape portfolio tab due to an exception")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    try:
        scrapeOtherTabs('FeesAndDetails', fund_type,
                        f'{country_code_db_name}_screener_data')
    except:
        print(f"Failed to scrape fees and details tab due to an exception")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    try:
        scrapeOtherTabs('CalendarYear', fund_type,
                        f'{country_code_db_name}_screener_data')
    except:
        print(f"Failed to scrape calendar year tab due to an exception")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    try:
        scrapeOtherTabs('Documents', fund_type,
                        f'{country_code_db_name}_screener_data')
    except:
        print(f"Failed to scrape documents tab due to an exception")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    try:
        scrapeOtherTabs('Pricing', fund_type,
                        f'{country_code_db_name}_screener_data')
    except:
        print(f"Failed to scrape pricing tab due to an exception")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    recordStatsOnDb(f"World {country_code.upper()} Fund Screener Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == '__main__':
    # check if parameter is given or show usage guideline
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <country_code>")
        sys.exit(1)
    country_code = sys.argv[1].lower()
    if country_code not in world_fund_type_map:
        print("Invalid country code")
        sys.exit(1)
    fund_type = world_fund_type_map[country_code]
    country_code_db_name = world_api_map[country_code]
    out_file_name = f'{country_code_db_name}_sec_ids.txt'
    with open(out_file_name, 'w+', encoding='utf-8') as f:
        thread = Thread(target=worldThread, args=(
            fund_type, country_code, country_code_db_name, f))
        thread.start()
        thread.join()
