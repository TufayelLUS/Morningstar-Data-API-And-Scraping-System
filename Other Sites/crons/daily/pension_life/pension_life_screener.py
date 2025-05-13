import requests
import os
import json
from pymongo import MongoClient
from datetime import datetime
from time import sleep
import re
from bs4 import BeautifulSoup as bs
import traceback
from threading import Thread

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


def addToDb(data, collection, fund_type):
    print("Adding data to mongodb")
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection]
    # drop old data
    collection.delete_many({"FundType": fund_type})
    collection.insert_many(data)
    print("Data added to mongodb")
    client.close()


def updateInDb(data, collection):
    print("Updating data to mongodb")
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection]
    for doc in data:
        try:
            collection.update_one({'SecId': doc['SecId']}, {'$set': doc})
        except:
            pass
    client.close()


def getFromDb(filter=None, collection_name='fund_screener_data'):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection_name]
    if filter:
        return list(collection.find(filter))
    return list(collection.find())


def scrapeFunds(fund_type, collection_name):
    s = requests.Session()
    link = 'https://lt.morningstar.com/3y3wd9echv/fundquickranklnp/default.aspx?Universe=SAGBR$${}&LanguageId=en-GB'.format(
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
    if fund_type == 'PSA':
        out_file_name = 'pension_sec_ids.txt'
    else:
        out_file_name = 'life_sec_ids.txt'
    with open(out_file_name, 'w+', encoding='utf-8') as f:
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
                    '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlPageSize',
                    '__EVENTARGUMENT': page_no,
                    'ctl00_ContentPlaceHolder1_aFundQuickrankLNPControl_scrtmgrFundQuickrank_HiddenField': '',
                    '__LASTFOCUS': '',
                    '__VIEWSTATE': __VIEWSTATE,
                    '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                    '__VIEWSTATEENCRYPTED': '',
                    '__EVENTVALIDATION': __EVENTVALIDATION,
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$msTopBar$ddlTitleBarCurrency': 'BAS',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$msTopBar$ddlTitleBarLanguage': 'en-GB',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$msTopBar$ddlTitleBarTools': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlUniverse': 'SAGBR$${}'.format(fund_type),
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlInsuranceWrapper': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlCompany': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlBrandingName': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlABISector': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$txtSearchKey': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnFilterBySelection': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlPageSize': '500',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnSelectedFunds': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnSelectedIndex': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnFirstFundNavDate': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnCustomImageFileIds': '',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnSelectedSecurityCount': '0',
                    'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnTabs': 'Snapshot,ShortTerm,Performance,Portfolio,FeesAndDetails,CalendarYear,Categorization',
                    'ctl00$__RequestVerificationToken': rv_token
                }
                if not ready:
                    page_no -= 1
                    print("Setting page count to 500")
                    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$aFundScreenerResultControlIT$ddlPageSize'
                    data['__EVENTARGUMENT'] = ''
                    ready = True
                else:
                    data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$AspNetPager'
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
                    'referer': 'https://lt.morningstar.com/3y3wd9echv/fundquickranklnp/default.aspx?Universe=SAGBR$${}&LanguageId=en-GB'.format(fund_type),
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
                    resp = s.get(link, headers=headers, timeout=timeout, proxies=proxies).text
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
                'table', {'id': 'ctl00_ContentPlaceHolder1_aFundQuickrankLNPControl_gridResult'})
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
                            if 'gridClosePrice' in td.get('class', []):
                                column_texts.append(td.text.strip())
                            elif 'gridStarRating' in td.get('class', []):
                                column_texts.append(td.find('img').get(
                                    'src').split('/')[-1].split('.')[0])
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
                json_mapping['FundType'] = "SAGBR$${}".format(fund_type)
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
    addToDb(data_final, collection_name, "SAGBR$${}".format(fund_type))


def scrapeOtherTabs(tab, fund_type, collection_name):
    s = requests.Session()
    link = 'https://lt.morningstar.com/3y3wd9echv/fundquickranklnp/default.aspx?Universe=SAGBR$${}&LanguageId=en-GB'.format(
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
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlPageSize',
                '__EVENTARGUMENT': page_no,
                'ctl00_ContentPlaceHolder1_aFundQuickrankLNPControl_scrtmgrFundQuickrank_HiddenField': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': __VIEWSTATE,
                '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                '__VIEWSTATEENCRYPTED': '',
                '__EVENTVALIDATION': __EVENTVALIDATION,
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$msTopBar$ddlTitleBarCurrency': 'BAS',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$msTopBar$ddlTitleBarLanguage': 'en-GB',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$msTopBar$ddlTitleBarTools': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlUniverse': 'SAGBR$${}'.format(fund_type),
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlInsuranceWrapper': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlCompany': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlBrandingName': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlABISector': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$txtSearchKey': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnFilterBySelection': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlPageSize': '500',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnSelectedFunds': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnSelectedIndex': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnFirstFundNavDate': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnCustomImageFileIds': '',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnSelectedSecurityCount': '0',
                'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$hdnTabs': 'Snapshot,ShortTerm,Performance,Portfolio,FeesAndDetails,CalendarYear,Categorization',
                'ctl00$__RequestVerificationToken': rv_token
            }
            if not ready:
                page_no -= 1
                print("Setting page count to 500")
                data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$ddlPageSize'
                data['__EVENTARGUMENT'] = ''
                ready = True
            elif not switched_to_tab:
                page_no -= 1
                print("Switching to {} tab".format(tab))
                data['__EVENTTARGET'] = 'TabAction'
                data['__EVENTARGUMENT'] = tab
                switched_to_tab = True
            else:
                data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$aFundQuickrankLNPControl$AspNetPager'
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
                'referer': 'https://lt.morningstar.com/3y3wd9echv/fundquickranklnp/default.aspx?Universe=SAGBR$${}&LanguageId=en-GB'.format(fund_type),
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
                resp = s.get(link, headers=headers, timeout=timeout, proxies=proxies).text
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
            'table', {'id': 'ctl00_ContentPlaceHolder1_aFundQuickrankLNPControl_gridResult'})
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
                        if 'gridSustainabilityRating' in td.get('class', []):
                            try:
                                column_texts.append(
                                    'https://lt.morningstar.com' + td.find('img').get('src'))
                            except:
                                column_texts.append(td.text.strip())
                        elif 'gridClosePrice' in td.get('class', []):
                            column_texts.append(td.text.strip())
                        elif 'gridStarRating' in td.get('class', []):
                            column_texts.append(td.find('img').get(
                                'src').split('/')[-1].split('.')[0])
                        elif td.get('title'):
                            column_texts.append(td.get('title').strip())
                        elif td.find('img'):
                            column_texts.append(td.find('img').get('title'))
                        elif td.find('a'):
                            column_texts.append(td.find('a').get('title'))
                        else:
                            column_texts.append(td.text.strip())
                    except:
                        try:
                            column_texts.append(td.text.strip())
                        except:
                            column_texts.append('')
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


def pensionThread():
    recordStatsOnDb("Pension Fund Screener Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    for i in range(2):
        try:
            scrapeFunds('PSA', 'pension_screener_data')
            break
        except:
            print("Failed to scrape pension funds due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('ShortTerm', 'PSA', 'pension_screener_data')
            break
        except:
            print("Failed to scrape short term tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('Performance', 'PSA', 'pension_screener_data')
            break
        except:
            print("Failed to scrape performance tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('Portfolio', 'PSA', 'pension_screener_data')
            break
        except:
            print("Failed to scrape portfolio tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('FeesAndDetails', 'PSA', 'pension_screener_data')
            break
        except:
            print("Failed to scrape fees and details tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('CalendarYear', 'PSA', 'pension_screener_data')
            break
        except:
            print("Failed to scrape calendar year tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('Categorization', 'PSA', 'pension_screener_data')
            break
        except:
            print("Failed to scrape categorization tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    recordStatsOnDb("Pension Fund Screener Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def lifeThread():
    recordStatsOnDb("Life Fund Screener Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    for i in range(2):
        try:
            scrapeFunds('LSA', 'life_screener_data')
            break
        except:
            print("Failed to scrape life funds due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('ShortTerm', 'LSA', 'life_screener_data')
            break
        except:
            print("Failed to scrape short term tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('Performance', 'LSA', 'life_screener_data')
            break
        except:
            print("Failed to scrape performance tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('Portfolio', 'LSA', 'life_screener_data')
            break
        except:
            print("Failed to scrape portfolio tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('FeesAndDetails', 'LSA', 'life_screener_data')
            break
        except:
            print("Failed to scrape fees and details tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('CalendarYear', 'LSA', 'life_screener_data')
            break
        except:
            print("Failed to scrape calendar year tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    for i in range(2):
        try:
            scrapeOtherTabs('Categorization', 'LSA', 'life_screener_data')
            break
        except:
            print("Failed to scrape categorization tab due to an exception")
            with open('errors.txt', 'a') as f:
                f.write("[{}] Exception: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    recordStatsOnDb("Life Fund Screener Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == '__main__':
    pension_thread = Thread(target=pensionThread)
    life_thread = Thread(target=lifeThread)
    pension_thread.start()
    life_thread.start()
    pension_thread.join()
    life_thread.join()
