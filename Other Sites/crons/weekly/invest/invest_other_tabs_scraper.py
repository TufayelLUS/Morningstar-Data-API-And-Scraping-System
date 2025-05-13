import requests
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
import json
from concurrent.futures import ThreadPoolExecutor
import traceback
from datetime import datetime
import random


input_file_path = '../../daily/invest/invest_sec_ids.txt'
mongo_connection_string = "mongodb://localhost:27017/"
threads_count = 20
timeout = 40
retry_count = 5
proxy_list = ["http://ip:port", "http://ip:port"]
proxy_list = ["http://37.48.118.4:13151", "http://5.79.66.2:13151"]
overview_enabled = 0
risk_n_rating_enabled = 0
performance_enabled = 0
management_enabled = 0
documents_enabled = 1
portfolio_enabled = 0


def pickProxy(random_mode=False):
    if random_mode:
        return random.choice(proxy_list)
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


def cleanupForMongo(data):
    if isinstance(data, dict):
        new_dict = {}
        for key, value in data.items():
            new_key = key.replace(".", "")  # Remove dots from the key
            new_dict[new_key] = cleanupForMongo(value)
        return new_dict
    elif isinstance(data, list):
        return [cleanupForMongo(item) for item in data]
    else:
        return data  # Return unchanged if not a list or dict


def addToDB(collection, data, flag_update_only=False):
    data = cleanupForMongo(data)
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection]
    previous_data = collection.find_one({'SecId': data['SecId']})
    if previous_data:
        if flag_update_only:
            collection.update_one({'SecId': data['SecId']}, {
                                  '$set': {'Flag': data['Flag'], 'Flag Reason': data['Flag Reason']}})
        else:
            collection.update_one({'SecId': data['SecId']}, {'$set': data})
    else:
        collection.insert_one(data)
    client.close()


def getPageSectionDynamic(soup, container_id, has_empty_column_name=True, custom_parameter_name='Parameter'):
    container_obj = []
    data_container = soup.find(
        'div', id=container_id)
    if not data_container:
        print(f"{container_id} container not found")
    else:
        table = data_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            if has_empty_column_name:
                item_headers.insert(0, custom_parameter_name)
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    container_obj.append(
                        dict(zip(item_headers, row_data)))
    return container_obj


def getPageSectionLeftRight(soup, container_id, object_type):
    if object_type == 'dict':
        container_obj = {}
    else:
        container_obj = []
    data_container = soup.find(
        'div', id=container_id)
    if not data_container:
        print("Container not found")
    else:
        table = data_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                left_cell = row.find('th')
                right_cell = row.find('td')
                if left_cell and right_cell:
                    if object_type == 'dict':
                        container_obj[left_cell.text.strip()
                                      ] = right_cell.text.strip()
                    else:
                        container_obj.append({
                            'Parameter': left_cell.text.strip(),
                            'Value': right_cell.text.strip()
                        })
    return container_obj


def getDocuments(SecId, fund_type):
    final_data = {
        'SecId': SecId,
        'documents': []
    }
    link = f'https://lt.morningstar.com/3y3wd9echv/snapshot/default.aspx?tab=11&SecurityToken={SecId}%5D21%5D0%5D{fund_type}&Id={SecId}&ClientFund=0&CurrencyId=GBP'
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, proxies=proxies)
            if resp.status_code == 404:
                final_data['Flag'] = 'Deleted'
                final_data['Flag Reason'] = '404'
                return final_data
            resp = resp.text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        final_data['Flag'] = 'XProxy'
        final_data['Flag Reason'] = 'Proxy Failure'
        return final_data
    soup = bs(resp, 'html.parser')
    offering_documents_container = soup.find('div', id='DocumentsOffering')
    shareholder_reports_container = soup.find('div', id='DocumentsShareholder')
    other_documents_container = soup.find('div', id='DocumentsOther')
    # container = soup.find('div', id='SnapshotBodyContent')
    # if not container:
    #     print("Container not found with proxy {} for SecId {}".format(proxy, SecId))
    #     try:
    #         main_docs_text = soup.find(
    #             'div', id='snapshotTitleDiv').next_sibling.text.strip()
    #         if 'GBR' in main_docs_text:
    #             final_data['Flag'] = 'XGBR'
    #             final_data['Flag Reason'] = main_docs_text
    #         elif 'data is not available' in main_docs_text:
    #             final_data['Flag'] = 'NOT_AVAILABLE'
    #             final_data['Flag Reason'] = main_docs_text
    #         else:
    #             final_data['Xother'] = main_docs_text
    #             final_data['Flag Reason'] = main_docs_text
    #     except:
    #         final_data['Flag'] = 'ERROR'
    #         final_data['Flag Reason'] = 'Unknown'
    #     return final_data
    documents = []
    if offering_documents_container:
        table1 = offering_documents_container.find('table')
    else:
        table1 = None
    if shareholder_reports_container:
        table2 = shareholder_reports_container.find('table')
    else:
        table2 = None
    if other_documents_container:
        table3 = other_documents_container.find('table')
    else:
        table3 = None
    if not table1 and not table2 and not table3:
        print("Table not found with proxy {} for SecId {}".format(proxy, SecId))
        return final_data
    if table1:
        rows1 = table1.find_all('tr')
    else:
        rows1 = []
    if table2:
        rows2 = table2.find_all('tr')
    else:
        rows2 = []
    if table3:
        rows3 = table3.find_all('tr')
    else:
        rows3 = []
    for row in rows1:
        if row.find('a'):
            cells = row.find_all('td')
            # doc_id = cells[-1].find('a').get('href').split(
            #     'DocumentId=')[1].split('&')[0]
            # direct_doc_link = f'https://doc.morningstar.com/document/{doc_id}.msdoc/?clientid=morningstareurope&key=b05d173f8e969ca1'
            direct_doc_link = cells[-1].find('a', href=True).get('href')
            try:
                language = cells[1].text.strip()
            except:
                language = 'Undefined'
            try:
                effective_date = cells[2].text.strip()
            except:
                effective_date = 'Undefined'
            documents.append({
                'name': cells[0].text.strip(),
                'link': direct_doc_link,
                'language': language,
                'effective_date': effective_date
            })
    for row in rows2:
        if row.find('a'):
            cells = row.find_all('td')
            # doc_id = cells[-1].find('a').get('href').split(
            #     'DocumentId=')[1].split('&')[0]
            # direct_doc_link = f'https://doc.morningstar.com/document/{doc_id}.msdoc/?clientid=morningstareurope&key=b05d173f8e969ca1'
            direct_doc_link = cells[-1].find('a', href=True).get('href')
            try:
                language = cells[1].text.strip()
            except:
                language = 'Undefined'
            try:
                effective_date = cells[2].text.strip()
            except:
                effective_date = 'Undefined'
            documents.append({
                'name': cells[0].text.strip(),
                'link': direct_doc_link,
                'language': language,
                'effective_date': effective_date
            })
    for row in rows3:
        if row.find('a'):
            cells = row.find_all('td')
            # doc_id = cells[-1].find('a').get('href').split(
            #     'DocumentId=')[1].split('&')[0]
            # direct_doc_link = f'https://doc.morningstar.com/document/{doc_id}.msdoc/?clientid=morningstareurope&key=b05d173f8e969ca1'
            direct_doc_link = cells[-1].find('a', href=True).get('href')
            try:
                language = cells[1].text.strip()
            except:
                language = 'Undefined'
            try:
                effective_date = cells[2].text.strip()
            except:
                effective_date = 'Undefined'
            documents.append({
                'name': cells[0].text.strip(),
                'link': direct_doc_link,
                'language': language,
                'effective_date': effective_date
            })
    final_data['documents'] = documents
    return final_data


def getRiskNRating(SecId, fund_type):
    final_data = {
        'SecId': SecId,
        'rating': [],
        'risk': {}
    }
    link = f'https://lt.morningstar.com/3y3wd9echv/snapshot/default.aspx?tab=2&SecurityToken={SecId}%5D21%5D0%5D{fund_type}&Id={SecId}&ClientFund=0&CurrencyId=GBP'
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, proxies=proxies)
            if resp.status_code == 404:
                final_data['Flag'] = 'Deleted'
                final_data['Flag Reason'] = '404'
                return final_data
            resp = resp.text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        final_data['Flag'] = 'XProxy'
        final_data['Flag Reason'] = 'Proxy Failure'
        return final_data
    soup = bs(resp, 'html.parser')
    rating_container = soup.find('div', id='RatingReturnRisk')
    if not rating_container:
        print("Rating container not found")
        table = None
    else:
        table = rating_container.find('table')
    rating_data = []
    if not table:
        print("Table not found")
    else:
        rows = table.find_all('tr')
        for row in rows:
            cell_title = row.find('th')
            cells = row.find_all('td')
            if any([x and not x.get('headers', '') for x in cells]):
                continue
            if len(cells) < 3:
                continue
            sub_data = {
                'Year': cell_title.text.strip(),
                'Return': cells[0].text.strip(),
                'Risk': cells[1].text.strip(),
                'Rating': cells[2].find('img').get('alt') if cells[2].find('img') else cells[2].text.strip()
            }
            rating_data.append(sub_data)
    final_data['rating'] = rating_data
    modern_portfolio_stats = []
    modern_portfolio_stats_container = soup.find(
        'div', id='ModernPortfolioStatistics')
    if not modern_portfolio_stats_container:
        print("Modern portfolio stats container not found")
        table = None
    else:
        table = modern_portfolio_stats_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        if cell.text.strip() == '':
                            continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    modern_portfolio_stats.append(
                        dict(zip(item_headers, row_data)))
    final_data['modern_portfolio_stats'] = modern_portfolio_stats
    risk_data_container = soup.find('div', id='RiskReturnAnalysis')
    if not risk_data_container:
        print("Risk data container not found")
        table = None
    else:
        table = risk_data_container.find('table')
    risk_data = []
    item_headers = []
    if not table:
        print("Table not found")
    else:
        rows = table.find_all('tr')
        for row in rows:
            if row.parent.name == 'thead':
                all_cells = row.find_all('th')
                for cell in all_cells:
                    if cell.text.strip() == '':
                        continue
                    item_headers.append(cell.text.strip())
            else:
                cell_title = row.find('th')
                cells = row.find_all('td')
                row_data = []
                row_data.append(cell_title.text.strip())
                for cell in cells:
                    if cell.text.strip() == '':
                        continue
                    row_data.append(cell.text.strip())
                if not row_data:
                    continue
                risk_data.append(dict(zip(item_headers, row_data)))
    final_data['risk'] = {
        'headers': item_headers,
        'data': risk_data
    }
    return final_data


def getPerformance(SecId, fund_type):
    final_data = {
        'SecId': SecId,
        'AnnualReturns': [],
        'TrailingReturns': [],
        'QuarterlyReturns': []
    }
    link = f'https://lt.morningstar.com/3y3wd9echv/snapshot/default.aspx?tab=1&SecurityToken={SecId}%5D21%5D0%5D{fund_type}&Id={SecId}&ClientFund=0&CurrencyId=GBP'
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, proxies=proxies)
            if resp.status_code == 404:
                final_data['Flag'] = 'Deleted'
                final_data['Flag Reason'] = '404'
                return final_data
            resp = resp.text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        final_data['Flag'] = 'XProxy'
        final_data['Flag Reason'] = 'Proxy Failure'
        return final_data
    soup = bs(resp, 'html.parser')
    annual_returns_container = soup.find('div', id='AnnualReturns')
    annual_returns = []
    if not annual_returns_container:
        print("Container not found")
    else:
        table = annual_returns_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        if cell.find('img'):
                            row_data.append(
                                "https://lt.morningstar.com" + cell.find('img').get('src'))
                        elif cell.text.strip() == '':
                            continue
                        else:
                            row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    annual_returns.append(dict(zip(item_headers, row_data)))
            final_data['AnnualReturns'] = {
                'headers': item_headers,
                'data': annual_returns
            }
    trailing_returns_container = soup.find('div', id='TrailingReturns')
    trailing_returns = []
    if not trailing_returns_container:
        print("Container not found")
    else:
        table = trailing_returns_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        if cell.text.strip() == '':
                            continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    trailing_returns.append(dict(zip(item_headers, row_data)))
            final_data['TrailingReturns'] = {
                'headers': item_headers,
                'data': trailing_returns
            }
    quarterly_returns_container = soup.find('div', id='QuarterlyReturns')
    quarterly_returns = []
    if not quarterly_returns_container:
        print("Container not found")
    else:
        table = quarterly_returns_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        if cell.text.strip() == '':
                            continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    quarterly_returns.append(dict(zip(item_headers, row_data)))
            final_data['QuarterlyReturns'] = {
                'headers': item_headers,
                'data': quarterly_returns
            }
    return final_data


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
    link = f'https://www.morningstar.co.uk/uk/funds/snapshot/snapshot.aspx?id={SecIds}&tab=13&investmentType=SA'
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
    for iframe in iframes:
        if hasattr(iframe, 'src') and 'SecurityTokenList=' in iframe.get('src'):
            item_split = iframe.get('src').split(
                'SecurityTokenList=')[1].split('&')[0]
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
        names = [name for name in names if name != '']
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
        all_charts.append(final_data)

    return all_charts


def getManagement(SecId, fund_type):
    final_data = {
        'SecId': SecId,
        'management': {},
        'manager': []
    }
    link = f'https://lt.morningstar.com/3y3wd9echv/snapshot/default.aspx?tab=4&SecurityToken={SecId}%5D21%5D0%5D{fund_type}&Id={SecId}&ClientFund=0&CurrencyId=GBP'
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, proxies=proxies)
            if resp.status_code == 404:
                final_data['Flag'] = 'Deleted'
                final_data['Flag Reason'] = '404'
                return final_data
            resp = resp.text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        final_data['Flag'] = 'XProxy'
        final_data['Flag Reason'] = 'Proxy Failure'
        return final_data
    soup = bs(resp, 'html.parser')
    management_data = {}
    management_container = soup.find(
        'div', id='ManagementCompanyDetails')
    if not management_container:
        print("Container not found")
    else:
        table = management_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                left_cell = row.find('th').text.strip()
                try:
                    right_cell = row.find('td').get_text(
                        separator='\n').strip()
                except:
                    continue
                management_data[left_cell] = right_cell
    final_data['management'] = management_data
    fund_container = soup.find(
        'div', id='SecurityDetails')
    fund_data = {}
    if not fund_container:
        print("Container not found")
    else:
        table = fund_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                left_cell = row.find('th').text.strip()
                right_cell = row.find('td').get_text(separator='\n').strip()
                fund_data[left_cell] = right_cell
    fund_advisor_container = soup.find(
        'div', id='SecurityAdvisors')
    fund_data['advisors'] = []
    if fund_advisor_container:
        advisors = fund_advisor_container.find_all('div', {'class': 'item'})
        for advisor in advisors:
            fund_data['advisors'].append(
                advisor.get_text(separator='\n').strip())
    final_data['fund'] = fund_data
    fund_manager_container = soup.find(
        'div', id='SecurityManagers')
    managers_data = []
    if not fund_manager_container:
        print("Container not found")
    else:
        table = fund_manager_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            manager_data = {}
            for row in rows:
                left_cell = row.find('th').text.strip()
                try:
                    right_cell = row.find('td').get_text(
                        separator='\n').strip()
                except:
                    continue
                if left_cell == 'Fund Manager':
                    if len(manager_data) > 0:
                        managers_data.append(manager_data)
                    manager_data = {}
                manager_data[left_cell] = right_cell
            if len(manager_data) > 0:
                managers_data.append(manager_data)
    final_data['manager'] = managers_data
    return final_data


def getOverview(SecId, fund_type):
    final_data = {
        'SecId': SecId,
        'Title': '',
        'CategoryName': '',
        'BenchmarkName': '',
        'GrowthOf100': {},
        'keyStats': {},
        'InvestmentObjective': '',
        'MorningstarRating': '',
        'ESGRating': '',
        'SRRI': '',
        'MorningstarMedalistRating': '',
        'Returns': {},
        'ReturnsDate': '',
        'CategoryBenchMark': [],
        'PortfolioProfile': {}
    }
    link = f'https://lt.morningstar.com/3y3wd9echv/snapshot/default.aspx?tab=0&SecurityToken={SecId}%5D21%5D0%5D{fund_type}&Id={SecId}&ClientFund=0&CurrencyId=GBP'
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, proxies=proxies)
            if resp.status_code == 404:
                final_data['Flag'] = 'Deleted'
                final_data['Flag Reason'] = '404'
                return final_data
            resp = resp.text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        final_data['Flag'] = 'XProxy'
        final_data['Flag Reason'] = 'Proxy Failure'
        return final_data
    soup = bs(resp, 'html.parser')
    title_container = soup.find('div', id='SnapshotTitle')
    if title_container:
        try:
            final_data['Title'] = title_container.find('h1').text.strip()
        except:
            pass
    category_data_container = soup.find('div', id='ComparatorsList')
    if category_data_container:
        try:
            final_data['CategoryName'] = category_data_container.find('li', {'class': 'category'}).find('span', {'class': 'name'}).text.strip()
        except:
            pass
        try:
            final_data['BenchmarkName'] = category_data_container.find('li', {'class': 'benchmark'}).find('span', {'class': 'name'}).text.strip()
        except:
            pass
    final_data['GrowthOf100'] = getPageSectionDynamic(soup, 'AnnualReturns')
    morningstar_rating_container = soup.find('div', id='MorningstarRating')
    if morningstar_rating_container:
        try:
            final_data['MorningstarRating'] = morningstar_rating_container.find(
                'img').get('src').split('/')[-1].split('.')[0]
        except:
            pass
    esg_rating_container = soup.find(
        'div', id='MorningstarSustainabilityRating')
    if esg_rating_container:
        try:
            final_data['ESGRating'] = esg_rating_container.find(
                'img').get('alt')
        except:
            pass
    morningstar_medalist_rating_container = soup.find(
        'div', id='MorningstarMedalistRating')
    if morningstar_medalist_rating_container:
        try:
            final_data['MorningstarMedalistRating'] = morningstar_medalist_rating_container.find(
                'div', {'class': 'item'}).text.strip()
            if final_data['MorningstarMedalistRating'].strip() == '':
                final_data['MorningstarMedalistRating'] = morningstar_medalist_rating_container.find(
                'div', {'class': 'item'}).find('img').get('alt')
        except:
            pass
    srri_container = soup.find('div', id='Srri')
    if srri_container:
        try:
            final_data['SRRI'] = srri_container.find(
                'li', {'class': 'selected'}).text.strip()
        except:
            final_data['SRRI'] = 'Not Defined'
    key_stats_container = soup.find('div', id='KeyStats')
    key_stats = {}
    if not key_stats_container:
        print("Container not found")
    else:
        table = key_stats_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                left_cell = row.find('th').text.strip()
                try:
                    right_cell = row.find('td').get_text(
                        separator='\n').strip()
                except:
                    continue
                key_stats[left_cell] = right_cell
    final_data['keyStats'] = key_stats
    investment_objective_container = soup.find(
        'div', id='InvestmentObjective')
    if not investment_objective_container:
        print("Container not found")
    else:
        try:
            final_data['InvestmentObjective'] = investment_objective_container.find(
                'div', {'class': 'item'}).text.strip()
        except:
            pass
    final_data['Returns'] = getPageSectionDynamic(soup, 'TrailingReturns')
    trailing_returns_date_container = soup.find('div', id='TrailingReturns')
    if trailing_returns_date_container:
        try:
            final_data['ReturnsDate'] = trailing_returns_date_container.find(
                'span', {'class': 'date'}).text.strip()
        except:
            pass
    category_benchmark_container = soup.find(
        'div', id='Benchmarks')
    benchmark_data = []
    if not category_benchmark_container:
        print("Container not found")
    else:
        benchmarks = category_benchmark_container.find_all(
            'div', {'class': 'item'})
        for benchmark in benchmarks:
            try:
                left_cell = benchmark.find('h3').text.strip()
            except:
                continue
            try:
                right_cell = benchmark.find('div').text.strip()
            except:
                continue
            benchmark_data.append({
                "Type": left_cell,
                "Value": right_cell
            })
    final_data['CategoryBenchMark'] = benchmark_data
    portfolio_overview_data = {}
    equity_style_container = soup.find(
        'div', {'id': 'StyleBoxEquity'})
    equity_style_grid = [
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0]
    ]
    if equity_style_container:
        style_data_box = equity_style_container.find(
            'div', {'class': 'msStylebox'})
        row = 0
        col = 0
        for grid in style_data_box.children:
            if grid.get('style'):
                equity_style_grid[row][col] = 1
                col += 1
                if col == 3:
                    col = 0
                    row += 1
            else:
                pass
    else:
        pass
    try:
        right_img = "https://lt.morningstar.com" + soup.find('img', {'id': 'styleBoxLargeMidSmallImageLabelEquity'}).get(
            'src', '').replace('../..', '')
    except:
        right_img = ''
    try:
        right_img_2 = "https://lt.morningstar.com" + soup.find('img', {'id': 'styleBoxSizeImageLabel'}).get(
            'src', '').replace('../..', '')
    except:
        right_img_2 = ''
    try:
        bottom_img = "https://lt.morningstar.com" + soup.find('img', {'id': 'styleBoxValueBlendGrowthImageLabelEquity'}).get(
            'src', '').replace('../..', '')
    except:
        bottom_img = ''
    try:
        bottom_img_2 = "https://lt.morningstar.com" + soup.find('img', {'id': 'styleBoxStyleImageLabel'}).get(
            'src', '').replace('../..', '')
    except:
        bottom_img_2 = ''
    portfolio_overview_data['Equity Style Grid'] = {
        "right_images": [
            right_img,
            right_img_2
        ],
        "bottom_images": [
            bottom_img,
            bottom_img_2
        ],
        "state": equity_style_grid
    }
    fixed_income_style_container = soup.find(
        'div', {'id': 'StyleBoxFixedIncome'})
    fixed_income_style_grid = [
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0]
    ]
    if fixed_income_style_container:
        style_data_box = fixed_income_style_container.find(
            'div', {'class': 'msStylebox'})
        row = 0
        col = 0
        for grid in style_data_box.children:
            if grid.get('style'):
                fixed_income_style_grid[row][col] = 1
                col += 1
                if col == 3:
                    col = 0
                    row += 1
            else:
                pass
    else:
        pass
    try:
        right_img = "https://lt.morningstar.com" + soup.find('img', {'id': 'styleBoxLargeMidSmallImageLabelFixedIncome'}).get(
            'src', '').replace('../..', '')
    except:
        right_img = ''
    try:
        right_img_2 = "https://lt.morningstar.com" + soup.find('img', {'id': 'styleBoxCreditQualityImageLabel'}).get(
            'src', '').replace('../..', '')
    except:
        right_img_2 = ''
    try:
        bottom_img = "https://lt.morningstar.com" + soup.find('img', {'id': 'styleBoxValueBlendGrowthImageLabelFixedIncome'}).get(
            'src', '').replace('../..', '')
    except:
        bottom_img = ''
    try:
        bottom_img_2 = "https://lt.morningstar.com" + soup.find('img', {'id': 'styleBoxInterestRateImageLabel'}).get(
            'src', '').replace('../..', '')
    except:
        bottom_img_2 = ''
    portfolio_overview_data['Fixed Income Style Grid'] = {
        "right_images": [
            right_img,
            right_img_2
        ],
        "bottom_images": [
            bottom_img,
            bottom_img_2
        ],
        "state": equity_style_grid
    }
    asset_alloc_table = soup.find(
        'div', {'id': 'AssetAllocationNetTable'})
    asset_alloc_data = []
    if not asset_alloc_table:
        print("Table not found")
    else:
        table = asset_alloc_table.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    asset_alloc_data.append(
                        dict(zip(item_headers, row_data)))
    portfolio_overview_data['Asset Allocation'] = asset_alloc_data
    top_five_regions_container = soup.find(
        'div', {'id': 'TopRegions'})
    top_five_regions_data = []
    if not top_five_regions_container:
        print("Table not found")
    else:
        table = top_five_regions_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    top_five_regions_data.append(
                        dict(zip(item_headers, row_data)))
    portfolio_overview_data['Top 5 Regions'] = top_five_regions_data
    top_five_sectors_container = soup.find(
        'div', {'id': 'TopStockSectors'})
    top_five_sectors_data = []
    if not top_five_sectors_container:
        print("Table not found")
    else:
        table = top_five_sectors_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    top_five_sectors_data.append(
                        dict(zip(item_headers, row_data)))
    portfolio_overview_data['Top 5 Sectors'] = top_five_sectors_data
    top_five_holdings_container = soup.find(
        'div', {'id': 'TopHoldings'})
    top_five_holdings_data = []
    if not top_five_holdings_container:
        print("Table not found")
    else:
        table = top_five_holdings_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cells = row.find_all('td')
                    row_data = []
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                    if not row_data:
                        continue
                    top_five_holdings_data.append(
                        dict(zip(item_headers, row_data)))
            # remove incomplete data
            top_five_holdings_data = [
                x for x in top_five_holdings_data if len(x) == len(item_headers)]
    portfolio_overview_data['Top 5 Holdings'] = top_five_holdings_data
    fixed_income_sector_container = soup.find(
        'div', {'id': 'TopFixedIncomeSectors'})
    fixed_income_sector_data = []
    if not fixed_income_sector_container:
        print("Table not found")
    else:
        table = fixed_income_sector_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cells = row.find_all('td')
                    row_data = []
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    fixed_income_sector_data.append(
                        dict(zip(item_headers, row_data)))
    portfolio_overview_data['Fixed Income Sectors'] = fixed_income_sector_data
    final_data['PortfolioProfile'] = portfolio_overview_data
    return final_data


def getAPIToken(SecId):
    link = f'https://www.morningstar.co.uk/Common/funds/snapshot/SustainabilitySAL.aspx?Site=uk&FC={SecId}&IT=FO&LANG=en-GB&LITTLEMODE=True'
    loaded = False
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
        return None
    token = None
    try:
        token = resp.split('const maasToken = "')[1].split('"')[0]
    except:
        pass
    return token


def getPortfolio(SecId, fund_type):
    final_data = {
        'SecId': SecId,
        'Asset Allocation': {},
        'Style Measures': {},
        'Size': {},
        'Market Cap': {},
        'Maturity Distribution': {},
        'Maturity Summary': {},
        'Credit Quality Breakdown': {},
        'Exposure': {
            'Sector': {},
            'Region': {}
        },
        'Fixed Income Sector Weightings': {},
        'Holdings': {}
    }
    link = f'https://lt.morningstar.com/3y3wd9echv/snapshot/default.aspx?tab=3&SecurityToken={SecId}%5D21%5D0%5D{fund_type}&Id={SecId}&ClientFund=0&CurrencyId=GBP'
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, proxies=proxies)
            if resp.status_code == 404:
                final_data['Flag'] = 'Deleted'
                final_data['Flag Reason'] = '404'
                return final_data
            resp = resp.text
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        final_data['Flag'] = 'XProxy'
        final_data['Flag Reason'] = 'Proxy Failure'
        return final_data
    soup = bs(resp, 'html.parser')
    style_measures_container = soup.find('div', id='ValuationsAndGrowthRates')
    style_measures_data = []
    if not style_measures_container:
        print("Container not found")
    else:
        table = style_measures_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    style_measures_data.append(
                        dict(zip(item_headers, row_data)))
    final_data['Style Measures'] = style_measures_data
    size_container = soup.find('div', id='FundAndShareClassSize')
    size_data = []
    if not size_container:
        print("Container not found")
    else:
        table = size_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                left_cell = row.find('th')
                right_cell = row.find('td')
                if left_cell and right_cell:
                    size_data.append({
                        'Parameter': left_cell.text.strip(),
                        'Value': right_cell.text.strip()
                    })
    final_data['Size'] = size_data
    market_cap_container = soup.find('div', id='MarketCapitalisation')
    market_cap_data = []
    if not market_cap_container:
        print("Container not found")
    else:
        table = market_cap_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    market_cap_data.append(dict(zip(item_headers, row_data)))
    final_data['Market Cap'] = market_cap_data
    maturity_distribution_container = soup.find(
        'div', id='MaturityDistribution')
    maturity_distribution_data = []
    if not maturity_distribution_container:
        print("Container not found")
    else:
        table = maturity_distribution_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    maturity_distribution_data.append(
                        dict(zip(item_headers, row_data)))
    final_data['Maturity Distribution'] = maturity_distribution_data
    maturity_summary_container = soup.find('div', id='MaturitySummary')
    maturity_summary_data = []
    if not maturity_summary_container:
        print("Container not found")
    else:
        table = maturity_summary_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            item_headers.insert(0, 'Parameter')
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\\n', ' '))
                else:
                    cell_title = row.find('th')
                    cells = row.find_all('td')
                    row_data = []
                    row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    maturity_summary_data.append(
                        dict(zip(item_headers, row_data)))
    final_data['Maturity Summary'] = maturity_summary_data
    final_data['Credit Quality Breakdown'] = getPageSectionDynamic(
        soup, 'CreditQuality')
    final_data['Fixed Income Sector Weightings'] = getPageSectionDynamic(
        soup, 'FixedIncomeSectorWeightings')
    final_data['Asset Allocation'] = getPageSectionDynamic(
        soup, 'AssetAllocationNetTable')
    final_data['Holdings']['Summary'] = getPageSectionLeftRight(
        soup, 'TopHoldingsSummary', 'list')
    final_data['Exposure']['Region'] = getPageSectionDynamic(
        soup, 'WorldRegions', custom_parameter_name='Region')
    try:
        final_data['Exposure']['RegionDate'] = soup.find('div', id='WorldRegions').find('table').find('caption', {'class': 'sectionHeader'}).find('span', {'class': 'date'}).text.strip()
    except:
        final_data['Exposure']['RegionDate'] = 'Undefined'
    final_data['Exposure']['Sector'] = getPageSectionDynamic(
        soup, 'StockSectorWeightings', custom_parameter_name='Sector')
    try:
        final_data['Exposure']['SectorDate'] = soup.find('div', id='StockSectorWeightings').find('table').find('caption', {'class': 'sectionHeader'}).find('span', {'class': 'date'}).text.strip()
    except:
        final_data['Exposure']['SectorDate'] = 'Undefined'
    top_holding_container = soup.find('div', id='TopHoldings')
    top_holding_data = []
    if not top_holding_container:
        print("Container not found")
    else:
        table = top_holding_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            item_headers = []
            for row in rows:
                if row.parent.name == 'thead':
                    all_cells = row.find_all('th')
                    for cell in all_cells:
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.get_text(
                            separator='\n').strip().replace('\n', ' '))
                else:
                    cells = row.find_all('td')
                    row_data = []
                    first = True
                    for cell in cells:
                        if first:
                            first = False
                            continue
                        elif 'sectorIcon' in cell.get('class', []):
                            try:
                                row_data.append(cell.find('img').get('alt'))
                            except:
                                row_data.append(cell.text.strip())
                            continue
                        else:
                            row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    top_holding_data.append(dict(zip(item_headers, row_data)))
    final_data['Holdings']['Top Holdings'] = top_holding_data
    return final_data


def processInThread(SecId):
    print(f"Processing {SecId}")
    if overview_enabled:
        for i in range(retry_count):
            try:
                overview = getOverview(SecId)
                if overview.get('Flag'):
                    if overview.get('Flag') == 'Deleted':
                        addToDB('invest_overview', overview, True)
                    else:
                        addToDB('invest_overview', overview)
                    break
                # skip if no overview and not last retry
                if not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark') and i < retry_count - 1:
                    continue
                print(json.dumps(overview, indent=4))
                # check in the last loop if we have missing
                if i == retry_count - 1 and (not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark')):
                    overview['Flag'] = '5xFailed'
                    overview['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('invest_overview', overview)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')
    if documents_enabled:
        for i in range(retry_count):
            try:
                documents = getDocuments(SecId)
                if documents.get('Flag'):
                    if documents.get('Flag') == 'Deleted':
                        addToDB('invest_documents', documents, True)
                    else:
                        addToDB('invest_documents', documents)
                    break
                # skip if no documents and not last retry
                if not documents.get('documents') and i < retry_count - 1:
                    continue
                print(documents)
                # check in the last loop if we have missing
                if i == retry_count - 1 and not documents.get('documents'):
                    documents['Flag'] = '5xFailed'
                    documents['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('invest_documents', documents)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')
    if risk_n_rating_enabled:
        for i in range(retry_count):
            try:
                risk_n_rating = getRiskNRating(SecId)
                if risk_n_rating.get('Flag'):
                    if risk_n_rating.get('Flag') == 'Deleted':
                        addToDB('invest_risk_n_rating', risk_n_rating, True)
                    else:
                        addToDB('invest_risk_n_rating', risk_n_rating)
                    break
                # skip if no risk_n_rating and not last retry
                if not risk_n_rating.get('rating') and not risk_n_rating.get('risk') and i < retry_count - 1:
                    continue
                print(risk_n_rating)
                if i == retry_count - 1 and (not risk_n_rating.get('rating') and not risk_n_rating.get('risk')):
                    risk_n_rating['Flag'] = '5xFailed'
                    risk_n_rating['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('invest_risk_n_rating', risk_n_rating)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')
    if performance_enabled:
        for i in range(retry_count):
            try:
                performance = getPerformance(SecId)
                if performance.get('Flag'):
                    if performance.get('Flag') == 'Deleted':
                        addToDB('invest_performance', performance, True)
                    else:
                        addToDB('invest_performance', performance)
                    break
                # skip if no performance and not last retry
                if not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns') and i < retry_count - 1:
                    continue
                print(json.dumps(performance, indent=4))
                if i == retry_count - 1 and (not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns')):
                    performance['Flag'] = '5xFailed'
                    performance['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('invest_performance', performance)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')
    if management_enabled:
        for i in range(retry_count):
            try:
                management = getManagement(SecId)
                if management.get('Flag'):
                    if management.get('Flag') == 'Deleted':
                        addToDB('invest_management', management, True)
                    else:
                        addToDB('invest_management', management)
                    break
                # skip if no management and not last retry
                if not management.get('management') and not management.get('otherShareClasses') and not management.get('manager') and i < retry_count - 1:
                    continue
                print(json.dumps(management, indent=4))
                if i == retry_count - 1 and (not management.get('management') and not management.get('otherShareClasses') and not management.get('manager')):
                    management['Flag'] = '5xFailed'
                    management['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('invest_management', management)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')
    if portfolio_enabled:
        for i in range(2):
            try:
                portfolio_data = getPortfolio(SecId)
                if portfolio_data.get('Flag'):
                    if portfolio_data.get('Flag') == 'Deleted':
                        addToDB('invest_portfolio', portfolio_data, True)
                    else:
                        addToDB('invest_portfolio', portfolio_data)
                    break
                # skip if no portfolio and not last retry
                if not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings') and i < retry_count - 1:
                    continue
                print(json.dumps(portfolio_data, indent=4))
                # check in the last loop if we have missing
                if i == retry_count - 1 and (not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings')):
                    portfolio_data['Flag'] = '5xFailed'
                    portfolio_data['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('invest_portfolio', portfolio_data)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb("Invest Other Tabs Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(input_file_path, 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("Invest Other Tabs Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
