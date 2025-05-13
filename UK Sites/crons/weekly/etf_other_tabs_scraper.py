import requests
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
import json
from concurrent.futures import ThreadPoolExecutor
import traceback
from datetime import datetime
import random


# input_file_path = 'etf_sec_ids.txt'
input_file_path = '../daily/etf_sec_ids.txt'
mongo_connection_string = "mongodb://localhost:27017/"
threads_count = 20
timeout = 40
retry_count = 5
proxy_list = ["http://ip:port", "http://ip:port"]
overview_enabled = 1
risk_n_rating_enabled = 1
performance_enabled = 1
management_enabled = 1
documents_enabled = 1
portfolio_enabled = 1


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


def getDocuments(SecId):
    final_data = {
        'SecId': SecId,
        'documents': []
    }
    link = f'https://www.morningstar.co.uk/uk/etf/snapshot/snapshot.aspx?id={SecId}&tab=12&InvestmentType=FE'
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
    container = soup.find('div', id='managementFeesDiv')
    if not container:
        print("Container not found with proxy {} for SecId {}".format(proxy, SecId))
        try:
            main_docs_text = soup.find(
                'div', id='snapshotTitleDiv').next_sibling.text.strip()
            if 'GBR' in main_docs_text:
                final_data['Flag'] = 'XGBR'
                final_data['Flag Reason'] = main_docs_text
            elif 'data is not available' in main_docs_text:
                final_data['Flag'] = 'NOT_AVAILABLE'
                final_data['Flag Reason'] = main_docs_text
            else:
                final_data['Xother'] = main_docs_text
                final_data['Flag Reason'] = main_docs_text
        except:
            final_data['Flag'] = 'ERROR'
            final_data['Flag Reason'] = 'Unknown'
        return final_data
    documents = []
    table = container.find('table')
    if not table:
        print("Table not found with proxy {} for SecId {}".format(proxy, SecId))
        return final_data
    rows = table.find_all('tr')
    for row in rows:
        if row.find('a'):
            cells = row.find_all('td')
            doc_id = cells[-1].find('a').get('href').split(
                'DocumentId=')[1].split('&')[0]
            direct_doc_link = f'https://doc.morningstar.com/document/{doc_id}.msdoc/?clientid=euretailsite&key=4063b31408c7055e'
            documents.append({
                'name': cells[0].text.strip(),
                'link': direct_doc_link
            })
    final_data['documents'] = documents
    return final_data


def getRiskNRating(SecId):
    final_data = {
        'SecId': SecId,
        'rating': [],
        'risk': {}
    }
    link = f'https://www.morningstar.co.uk/uk/etf/snapshot/snapshot.aspx?id={SecId}&tab=2&InvestmentType=FE'
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
    rating_container = soup.find('div', id='ratingRatingDiv')
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
            cells = row.find_all('td')
            if any([x and 'heading' in x.get('class', '') for x in cells]):
                continue
            if len(cells) < 4:
                continue
            sub_data = {
                'Year': cells[0].text.strip(),
                'Return': cells[1].text.strip(),
                'Risk': cells[2].text.strip(),
                'Rating': cells[3].find('img').get('alt') if cells[3].find('img') else cells[3].text.strip()
            }
            rating_data.append(sub_data)
    final_data['rating'] = rating_data
    risk_left = soup.find('div', id='ratingRiskLeftDiv')
    risk_left_data = {}
    if risk_left:
        table = risk_left.find('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                risk_left_data[cells[0].text.strip()] = cells[1].text.strip()
    risk_right = soup.find('div', id='ratingRiskRightDiv')
    risk_right_data = {}
    if risk_right:
        table = risk_right.find('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                risk_right_data[cells[0].text.strip()] = cells[1].text.strip()
    # merge risk_left and risk_right
    risk_data = {}
    risk_data.update(risk_left_data)
    risk_data.update(risk_right_data)
    final_data['risk'] = risk_data
    return final_data


def getPerformance(SecId):
    final_data = {
        'SecId': SecId,
        'AnnualReturns': [],
        'TrailingReturns': [],
        'QuarterlyReturns': []
    }
    link = f'https://www.morningstar.co.uk/uk/etf/snapshot/snapshot.aspx?id={SecId}&tab=1&InvestmentType=FE'
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
    annual_returns_container = soup.find('div', id='returnsCalenderYearDiv')
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
            item_headers.insert(0, "Parameter")
            for row in rows:
                cells = row.find_all('td')
                if any([x and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                row_data = []
                for cell in cells:
                    if 'heading' in cell.get('class'):
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.text.strip())
                    else:
                        if cell.text.strip() == '':
                            continue
                        row_data.append(cell.text.strip())
                if not row_data:
                    continue
                annual_returns.append(dict(zip(item_headers, row_data)))
            final_data['AnnualReturns'] = {
                'headers': item_headers,
                'data': annual_returns
            }
    trailing_returns_container = soup.find('div', id='returnsTrailingDiv')
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
            item_headers.insert(0, "Parameter")
            for row in rows:
                cells = row.find_all('td')
                if any([x and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                row_data = []
                for cell in cells:
                    if 'heading' in cell.get('class'):
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.text.strip())
                    else:
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
    quarterly_returns_container = soup.find('div', id='returnsQuarterlyDiv')
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
            item_headers.insert(0, "Parameter")
            for row in rows:
                cells = row.find_all('td')
                if any([x and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                row_data = []
                for cell in cells:
                    if 'heading' in cell.get('class'):
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.text.strip())
                    else:
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
    link = f'https://www.morningstar.co.uk/uk/etf/snapshot/snapshot.aspx?id={SecIds}&tab=13&InvestmentType=FE'
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


def getManagement(SecId):
    final_data = {
        'SecId': SecId,
        'management': {},
        'otherShareClasses': [],
        'manager': []
    }
    link = f'https://www.morningstar.co.uk/uk/etf/snapshot/snapshot.aspx?id={SecId}&tab=4&InvestmentType=FE'
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
    management_left_container = soup.find(
        'div', id='managementManagementFundCompanyDiv')
    if not management_left_container:
        print("Container not found")
    else:
        table = management_left_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            last_label = ''
            no_more = False
            for row in rows:
                cells = row.find_all('td')
                for cell in cells:
                    if cell.text.strip() == '':
                        continue
                    if 'label' in cell.get('class'):
                        if not management_data.get(cell.text.strip()):
                            management_data[cell.text.strip()] = ''
                        last_label = cell.text.strip()
                    elif 'heading' in cell.get('class'):
                        last_label = cell.text.strip()
                        if last_label == 'Other Share Classes':
                            no_more = True
                            break
                        if not management_data.get(last_label):
                            management_data[last_label] = ''
                    else:
                        if management_data.get(last_label):
                            management_data[last_label] += '\n' + \
                                cell.text.strip()
                        else:
                            management_data[last_label] = cell.text.strip()
                if no_more:
                    break
    management_right_container = soup.find(
        'div', id='managementManagementFundManagerDiv')
    if not management_right_container:
        print("Container not found")
    else:
        table = management_right_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            last_label = ''
            for row in rows:
                cells = row.find_all('td')
                for cell in cells:
                    if cell.text.strip() == '':
                        continue
                    if 'label' in cell.get('class'):
                        if not management_data.get(cell.text.strip()):
                            management_data[cell.text.strip()] = ''
                        last_label = cell.text.strip()
                    elif 'heading' in cell.get('class'):
                        if not management_data.get(cell.text.strip()):
                            management_data[cell.text.strip()] = ''
                        last_label = cell.text.strip()
                    else:
                        if management_data.get(last_label):
                            management_data[last_label] += '\n' + \
                                cell.text.strip()
                        else:
                            management_data[last_label] = cell.text.strip()
    if management_data.get('Address'):
        management_data['Address'] = "\n".join(
            [line.strip() for line in management_data['Address'].split('\n') if line.strip()])
    final_data['management'] = management_data
    fund_manager_container = soup.find(
        'div', id='managementManagementDiv')
    managers_data = []
    if not fund_manager_container:
        print("Container not found")
    else:
        tables = fund_manager_container.find_all('table')
        if len(tables) == 0:
            print("Table not found")
        for table in tables:
            manager_data = {}
            rows = table.find_all('tr')
            last_label = ''
            for row in rows:
                cells = row.find_all('td')
                for cell in cells:
                    if cell.text.strip() == '' or cell.get('class') is None:
                        continue
                    if 'label' in cell.get('class'):
                        if not manager_data.get(cell.text.strip()):
                            manager_data[cell.text.strip()] = ''
                        last_label = cell.text.strip()
                    elif 'heading' in cell.get('class'):
                        if not manager_data.get(cell.text.strip()):
                            manager_data[cell.text.strip()] = ''
                        last_label = cell.text.strip()
                    else:
                        if manager_data.get(last_label):
                            manager_data[last_label] += '\n' + \
                                cell.text.strip()
                        else:
                            manager_data[last_label] = cell.text.strip()
            if manager_data and not manager_data.get('Name of Company') and (manager_data.get('Fund Manager') or manager_data.get('Biography')):
                managers_data.append(manager_data)
    for i, data in enumerate(managers_data):
        if i == 0:
            continue
        if data.get('Biography'):
            print(managers_data[i-1])
            managers_data[i-1]['Biography'] = data['Biography']
            managers_data[i]['Biography'] = ''
    managers_data = [
        data for data in managers_data if data.get('Fund Manager')]
    final_data['manager'] = managers_data

    other_share_classes_container = soup.find(
        'td', {'id': 'OtherShareClasses'})
    if other_share_classes_container:
        all_links = other_share_classes_container.find_all('a', href=True)
        for link in all_links:
            final_data['otherShareClasses'].append({
                'SecId': link['href'].split('id=')[1].split('&')[0],
                'name': link.text.strip(),
                'link': 'https://www.morningstar.co.uk/uk/etf/snapshot/' + link['href']
            })
    return final_data


def getOverview(SecId):
    final_data = {
        'SecId': SecId,
        'keyStats': {},
        'Sustainability': '',
        'CorporateESGPillars': {},
        'InvestmentObjective': '',
        'Returns': {},
        'Management': {},
        'CategoryBenchMark': [],
        'PrimaryObjective': {},
        'PortfolioProfile': {}
    }
    link = f'https://www.morningstar.co.uk/uk/etf/snapshot/snapshot.aspx?id={SecId}'
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
    try:
        fc_id = resp.split("var FC =")[1].split("'")[1].split("'")[0]
    except:
        fc_id = SecId
    soup = bs(resp, 'html.parser')
    key_stats_container = soup.find('div', id='overviewQuickstatsDiv')
    key_stats = {}
    if not key_stats_container:
        print("Container not found")
        return final_data
    try:
        fc_id = resp.split("var FC =")[1].split("'")[1].split("'")[0]
    except:
        fc_id = SecId
    soup = bs(resp, 'html.parser')
    key_stats_container = soup.find('div', id='overviewQuickstatsDiv')
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
                cells = row.find_all('td')
                if len(cells) < 3:
                    continue
                key_stats[cells[0].text.strip()] = cells[-1].text.strip()
    final_data['keyStats'] = key_stats
    investment_objective_container = soup.find(
        'div', id='overviewObjectiveDiv')
    if not investment_objective_container:
        print("Container not found")
    else:
        table = investment_objective_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            found = False
            for row in rows:
                cells = row.find_all('td')
                for cell in cells:
                    if 'value' in cell.get('class'):
                        final_data['InvestmentObjective'] = cell.text.strip()
                        found = True
                        break
                if found:
                    break
    final_data['InvestmentObjective'] = final_data['InvestmentObjective']
    returns_container = soup.find('div', id='TrailingReturnsOverview')
    returns_data = {}
    if not returns_container:
        print("Container not found")
    else:
        table = returns_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cells = row.find_all('td', class_=True)
                if not cells or any([x and x.get('class') and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                if len(cells) < 2:
                    continue
                returns_data[cells[0].text.strip()] = cells[-1].text.strip()
    final_data['Returns'] = returns_data

    management_container = soup.find('div', id='FundManagersOverview')
    management_data = {}
    if not management_container:
        print("Container not found")
    else:
        table = management_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                try:
                    left_cell = row.find(
                        'div', {'class': 'overviewManagerNameDiv'}).text.strip()
                    right_cell = row.find(
                        'div', {'class': 'overviewManagerStartDateDiv'}).text.strip()
                except:
                    continue
                management_data[left_cell] = right_cell
    final_data['Management'] = management_data
    category_benchmark_container = soup.find(
        'div', id='overviewBenchmarkDiv2Cols')
    benchmark_data = []
    if not category_benchmark_container:
        print("Container not found")
    else:
        table = category_benchmark_container.find('table')
        item_headers = []
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if any([x and x.get('class') and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                row_data = []
                for cell in cells:
                    if cell and cell.get('class') and 'heading' in cell.get('class'):
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.text.strip())
                    else:
                        if cell.text.strip() == '':
                            continue
                        row_data.append(cell.text.strip())
                if not row_data:
                    continue
                benchmark_data.append(dict(zip(item_headers, row_data)))
    final_data['CategoryBenchMark'] = benchmark_data
    primary_objective_container = soup.find(
        'div', id='overviewPrimaryObjectiveDiv')
    objective_data = {}
    if not primary_objective_container:
        print("Container not found")
    else:
        table = primary_objective_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                objective_data[cells[0].text.strip()] = cells[-1].text.strip()
    final_data['PrimaryObjective'] = objective_data
    portfolio_overview_data = {}
    equity_style_container = soup.find(
        'div', {'id': 'overviewPortfolioEquityStyleDiv'})
    if equity_style_container:
        try:
            equity_style_image_link = 'https://www.morningstar.co.uk' + equity_style_container.find('img').get('src')
        except:
            equity_style_image_link = None
    else:
        equity_style_image_link = None
    portfolio_overview_data['Equity Style Image'] = equity_style_image_link
    fixed_income_style_container = soup.find(
        'div', {'id': 'overviewPortfolioBondStyleDiv'})
    if fixed_income_style_container:
        try:
            fixed_income_style_image_link = 'https://www.morningstar.co.uk' + fixed_income_style_container.find('img').get('src')
        except:
            fixed_income_style_image_link = None
    else:
        fixed_income_style_image_link = None
    portfolio_overview_data['Fixed Income Style Image'] = fixed_income_style_image_link
    asset_alloc_table = soup.find(
        'div', {'id': 'overviewPortfolioAssetAllocationDiv'})
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
            item_headers.insert(0, 'Asset Type')
            for row in rows:
                cells = row.find_all('td')
                if any([x and x.get('class') and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                row_data = []
                for cell in cells:
                    if cell and cell.get('class') and 'heading' in cell.get('class'):
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.text.strip())
                    else:
                        if cell.text.strip() == '':
                            continue
                        row_data.append(cell.text.strip())
                if not row_data:
                    continue
                asset_alloc_data.append(dict(zip(item_headers, row_data)))
    portfolio_overview_data['Asset Allocation'] = asset_alloc_data
    top_five_regions_container = soup.find(
        'div', {'id': 'overviewPortfolioTopRegionsDiv'})
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
                cells = row.find_all('td')
                if any([x and x.get('class') and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                row_data = []
                for cell in cells:
                    if cell and cell.get('class') and 'heading' in cell.get('class'):
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.text.strip())
                    else:
                        if cell.text.strip() == '':
                            continue
                        row_data.append(cell.text.strip())
                if not row_data:
                    continue
                top_five_regions_data.append(dict(zip(item_headers, row_data)))
    portfolio_overview_data['Top 5 Regions'] = top_five_regions_data
    top_five_sectors_container = soup.find(
        'div', {'id': 'overviewPortfolioTopSectorsDiv'})
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
                cells = row.find_all('td')
                if any([x and x.get('class') and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                row_data = []
                for cell in cells:
                    if cell and cell.get('class') and 'heading' in cell.get('class'):
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.text.strip())
                    else:
                        if cell.text.strip() == '':
                            continue
                        row_data.append(cell.text.strip())
                if not row_data:
                    continue
                top_five_sectors_data.append(dict(zip(item_headers, row_data)))
    portfolio_overview_data['Top 5 Sectors'] = top_five_sectors_data
    top_five_holdings_container = soup.find(
        'div', {'id': 'overviewPortfolioTopHoldingsDiv'})
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
                cells = row.find_all('td')
                if any([x and x.get('class') and 'titleBarHeading' in x.get('class') for x in cells]):
                    continue
                row_data = []
                for cell in cells:
                    if cell and cell.get('class') and 'heading' in cell.get('class'):
                        if cell.text.strip() == '':
                            continue
                        item_headers.append(cell.text.strip())
                    else:
                        row_data.append(cell.text.strip())
                if not row_data:
                    continue
                top_five_holdings_data.append(
                    dict(zip(item_headers, row_data)))
    portfolio_overview_data['Top 5 Holdings'] = top_five_holdings_data
    final_data['PortfolioProfile'] = portfolio_overview_data
    additional_overview = getAdditionalOverview(fc_id)
    final_data['Sustainability'] = additional_overview.get(
        'esgData', {}).get('sustainabilityFundQuintile')
    final_data['CorporateESGPillars'] = {
        'Environmental': additional_overview.get('esgScoreCalculation', {}).get('environmentalScore'),
        'Social': additional_overview.get('esgScoreCalculation', {}).get('socialScore'),
        'Governance': additional_overview.get('esgScoreCalculation', {}).get('governanceScore'),
        'Unallocated': additional_overview.get('esgScoreCalculation', {}).get('portfolioUnallocatedEsgRiskScore')
    }
    return final_data


def getAdditionalOverview(SecId):
    link = f'https://www.us-api.morningstar.com/sal/sal-service/etf/esg/v1/{SecId}/data?languageId=en-GB&locale=en-GB&clientId=EC&benchmarkId=category&component=sal-sustainability&version=4.13.0'
    token = getAPIToken(SecId)
    if not token:
        return {}
    headers['Authorization'] = f'Bearer {token}'
    loaded = False
    for i in range(retry_count):
        proxy = pickProxy()
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            resp = requests.get(link, headers=headers,
                                timeout=timeout, proxies=proxies).json()
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return {}
    return resp


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


class Portfolio:
    # asset allocation
    # factor profile
    # style measures > measures, market cap
    # exposure > sector, region
    # financial metrics
    # holdings stats
    # holding names
    def __init__(self, SecId):
        self.SecId = SecId
        self.final_data = {
            'SecId': SecId,
            'Asset Allocation': {},
            'Factor Profile': {},
            'Style Measures': {},
            'Exposure': {},
            'Financial Metrics': {},
            'Holdings': {}
        }
        self.token = None

    def processEverything(self):
        link = f'https://www.morningstar.co.uk/uk/etf/snapshot/snapshot.aspx?id={self.SecId}&tab=3&InvestmentType=FE'
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
                    self.final_data['Flag'] = 'Deleted'
                    self.final_data['Flag Reason'] = '404'
                    return self.final_data
                resp = resp.text
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            self.final_data['Flag'] = 'XProxy'
            self.final_data['Flag Reason'] = 'Proxy Failure'
            return self.final_data
        try:
            fc_id = resp.split("var FC =")[1].split("'")[1].split("'")[0]
        except:
            fc_id = self.SecId
        self.SecId = fc_id
        self.token = getAPIToken(self.SecId)
        if not self.token:
            return self.final_data
        asset_allocation = self.getAssetAllocation(self.SecId)
        factor_profile = self.getFactorProfile(self.SecId)
        style_measures = self.getStyleMeasures(self.SecId)
        exposure = self.getExposure(self.SecId)
        financial_metrics = self.getFinancialMetrics(self.SecId)
        holdings = self.getHoldings(self.SecId)
        self.final_data['Asset Allocation'] = asset_allocation
        self.final_data['Factor Profile'] = factor_profile
        self.final_data['Style Measures'] = style_measures
        self.final_data['Exposure'] = exposure
        self.final_data['Financial Metrics'] = financial_metrics
        self.final_data['Holdings'] = holdings
        return self.final_data

    def getAssetAllocation(self, SecId):
        link = f'https://www.us-api.morningstar.com/sal/sal-service/etf/process/asset/v2/{SecId}/data?languageId=en-GB&locale=en-GB&clientId=EC&benchmarkId=category&component=sal-mip-asset-allocation&version=4.13.0'
        if not self.token:
            return {}
        headers['Authorization'] = f'Bearer {self.token}'
        loaded = False
        for i in range(retry_count):
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                resp = requests.get(link, headers=headers,
                                    timeout=timeout, proxies=proxies).json()
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            return {}
        mapping = resp.get('globalAllocationMap', {})
        mapping_uk = resp.get('allocationMap', {})
        data = {
            'Equity': {
                'Net': round(float(mapping.get('assetAllocEquity', {}).get('netAllocation', 0)), 2) if mapping.get('assetAllocEquity', {}).get('netAllocation', 0) else "-",
                'Short': round(float(mapping.get('assetAllocEquity', {}).get('shortAllocation', 0)), 2) if mapping.get('assetAllocEquity', {}).get('shortAllocation', 0) else "-",
                'Long': round(float(mapping.get('assetAllocEquity', {}).get('longAllocation', 0)), 2) if mapping.get('assetAllocEquity', {}).get('longAllocation', 0) else "-",
                'Cat.': round(float(mapping.get('assetAllocEquity', {}).get('longAllocationCategory', 0)), 2) if mapping.get('assetAllocEquity', {}).get('longAllocationCategory', 0) else "-",
                'Index': round(float(mapping.get('assetAllocEquity', {}).get('longAllocationIndex', 0)), 2) if mapping.get('assetAllocEquity', {}).get('longAllocationIndex', 0) else "-"
            },
            'Property': {
                'Net': round(float(mapping_uk.get('UKAssetAllocProperty', {}).get('netAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocProperty', {}).get('netAllocation', 0) else "-",
                'Short': round(float(mapping_uk.get('UKAssetAllocProperty', {}).get('shortAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocProperty', {}).get('shortAllocation', 0) else "-",
                'Long': round(float(mapping_uk.get('UKAssetAllocProperty', {}).get('longAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocProperty', {}).get('longAllocation', 0) else "-",
                'Cat.': round(float(mapping_uk.get('UKAssetAllocProperty', {}).get('longAllocationCategory', 0)), 2) if mapping_uk.get('UKAssetAllocProperty', {}).get('longAllocationCategory', 0) else "-",
                'Index': round(float(mapping_uk.get('UKAssetAllocProperty', {}).get('longAllocationIndex', 0)), 2) if mapping_uk.get('UKAssetAllocProperty', {}).get('longAllocationIndex', 0) else "-"
            },
            'Fixed Income': {
                'Net': round(float(mapping.get('assetAllocFixedIncome', {}).get('netAllocation', 0)), 2) if mapping.get('assetAllocFixedIncome', {}).get('netAllocation', 0) else "-",
                'Short': round(float(mapping.get('assetAllocFixedIncome', {}).get('shortAllocation', 0)), 2) if mapping.get('assetAllocFixedIncome', {}).get('shortAllocation', 0) else "-",
                'Long': round(float(mapping.get('assetAllocFixedIncome', {}).get('longAllocation', 0)), 2) if mapping.get('assetAllocFixedIncome', {}).get('longAllocation', 0) else "-",
                'Cat.': round(float(mapping.get('assetAllocFixedIncome', {}).get('longAllocationCategory', 0)), 2) if mapping.get('assetAllocFixedIncome', {}).get('longAllocationCategory', 0) else "-",
                'Index': round(float(mapping.get('assetAllocFixedIncome', {}).get('longAllocationIndex', 0)), 2) if mapping.get('assetAllocFixedIncome', {}).get('longAllocationIndex', 0) else "-"
            },
            'Other': {
                'Net': round(float(mapping_uk.get('UKAssetAllocOther', {}).get('netAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocOther', {}).get('netAllocation', 0) else "-",
                'Short': round(float(mapping_uk.get('UKAssetAllocOther', {}).get('shortAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocOther', {}).get('shortAllocation', 0) else "-",
                'Long': round(float(mapping_uk.get('UKAssetAllocOther', {}).get('longAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocOther', {}).get('longAllocation', 0) else "-",
                'Cat.': round(float(mapping_uk.get('UKAssetAllocOther', {}).get('longAllocationCategory', 0)), 2) if mapping_uk.get('UKAssetAllocOther', {}).get('longAllocationCategory', 0) else "-",
                'Index': round(float(mapping_uk.get('UKAssetAllocOther', {}).get('longAllocationIndex', 0)), 2) if mapping_uk.get('UKAssetAllocOther', {}).get('longAllocationIndex', 0) else "-"
            },
            'Cash': {
                'Net': round(float(mapping_uk.get('UKAssetAllocCash', {}).get('netAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocCash', {}).get('netAllocation', 0) else "-",
                'Short': round(float(mapping_uk.get('UKAssetAllocCash', {}).get('shortAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocCash', {}).get('shortAllocation', 0) else "-",
                'Long': round(float(mapping_uk.get('UKAssetAllocCash', {}).get('longAllocation', 0)), 2) if mapping_uk.get('UKAssetAllocCash', {}).get('longAllocation', 0) else "-",
                'Cat.': round(float(mapping_uk.get('UKAssetAllocCash', {}).get('longAllocationCategory', 0)), 2) if mapping_uk.get('UKAssetAllocCash', {}).get('longAllocationCategory', 0) else "-",
                'Index': round(float(mapping_uk.get('UKAssetAllocCash', {}).get('longAllocationIndex', 0)), 2) if mapping_uk.get('UKAssetAllocCash', {}).get('longAllocationIndex', 0) else "-"
            }
        }
        return data

    def getFactorProfile(self, SecId):
        link = f'https://www.us-api.morningstar.com/sal/sal-service/etf/factorProfile/{SecId}/data?languageId=en-GB&locale=en-GB&clientId=EC&benchmarkId=category&component=sal-mip-factor-profile&version=4.13.0'
        if not self.token:
            return {}
        headers['Authorization'] = f'Bearer {self.token}'
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
                if resp.status_code != 200:
                    return {}
                resp = resp.json()
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            return {}
        # return every raw data
        return resp

    def getStyleMeasures(self, SecId):
        link = f'https://www.us-api.morningstar.com/sal/sal-service/etf/process/stockStyle/v2/{SecId}/data?languageId=en-GB&locale=en-GB&clientId=EC&benchmarkId=category&component=sal-mip-measures&version=4.13.0'
        if not self.token:
            return {}
        headers['Authorization'] = f'Bearer {self.token}'
        loaded = False
        for i in range(retry_count):
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                resp = requests.get(link, headers=headers,
                                    timeout=timeout, proxies=proxies).json()
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            return {}
        # return every raw data
        return resp

    def getExposure(self, SecId):
        sector = self.getSector(SecId)
        region = self.getRegion(SecId)
        exposure = {
            'Sector': sector,
            'Region': region
        }
        return exposure

    def getSector(self, SecId):
        link = f'https://www.us-api.morningstar.com/sal/sal-service/etf/portfolio/v2/sector/{SecId}/data?languageId=en-GB&locale=en-GB&clientId=EC&benchmarkId=category&component=sal-mip-sector-exposure&version=4.13.0'
        if not self.token:
            return {}
        headers['Authorization'] = f'Bearer {self.token}'
        loaded = False
        for i in range(retry_count):
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                resp = requests.get(link, headers=headers,
                                    timeout=timeout, proxies=proxies).json()
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            return {}
        # return every raw data
        return resp

    def getRegion(self, SecId):
        link = f'https://www.us-api.morningstar.com/sal/sal-service/etf/portfolio/regionalSector/{SecId}/data?languageId=en-GB&locale=en-GB&clientId=EC&benchmarkId=category&component=sal-mip-region-exposure&version=4.13.0'
        if not self.token:
            return {}
        headers['Authorization'] = f'Bearer {self.token}'
        loaded = False
        for i in range(retry_count):
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                resp = requests.get(link, headers=headers,
                                    timeout=timeout, proxies=proxies).json()
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            return {}
        # return every raw data
        return resp

    def getFinancialMetrics(self, SecId):
        link = f'https://www.us-api.morningstar.com/sal/sal-service/etf/process/financialMetrics/{SecId}/data?languageId=en-GB&locale=en-GB&clientId=EC&benchmarkId=category&component=sal-mip-financial-metrics&version=4.13.0'
        if not self.token:
            return {}
        headers['Authorization'] = f'Bearer {self.token}'
        loaded = False
        for i in range(retry_count):
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                resp = requests.get(link, headers=headers,
                                    timeout=timeout, proxies=proxies).json()
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            return {}
        # return every raw data
        return resp

    def getHoldings(self, SecId):
        realtime_token, sal_content_type = self.getRealtimeToken()
        link = f'https://www.us-api.morningstar.com/sal/sal-service/etf/portfolio/holding/v2/{SecId}/data?premiumNum=100&freeNum=25&languageId=en-GB&locale=en-GB&clientId=EC&benchmarkId=category&component=sal-mip-holdings&version=4.13.0'
        if not self.token:
            return {}
        headers['Authorization'] = f'Bearer {self.token}'
        headers['x-api-realtime-e'] = realtime_token
        headers['x-sal-contenttype'] = sal_content_type
        loaded = False
        for i in range(retry_count):
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                resp = requests.get(link, headers=headers,
                                    timeout=timeout, proxies=proxies).json()
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            return {}
        data = {
            'Holdings Summary': {
                'Equity Holdings': resp.get('numberOfEquityHoldings', 0),
                'Bond Holdings': resp.get('numberOfBondHolding', 0),
                'Other Holdings': resp.get('numberOfOtherHolding', 0),
                '% Assets in Top 10 Holdings': resp.get('numberOfPropertyHoldings', 0),
                'Active Shares': resp.get('holdingActiveShare', {}).get('activeShareValue', 0),
                'Reported Turnover %': resp.get('holdingSummary', {}).get('lastTurnover', 0),
                'Reported Turnover Date': resp.get('holdingSummary', {}).get('LastTurnoverDate', 0),
                'Women Directors %': resp.get('holdingSummary', {}).get('womenDirectors', 0),
                'Women Executives %': resp.get('holdingSummary', {}).get('womenExecutives', 0)
            },
            'Holdings List': {
                'Equity': resp.get('equityHoldingPage', {}).get('holdingList', []),
                'Bond': resp.get('boldHoldingPage', {}).get('holdingList', []),
                'Other': resp.get('otherHoldingPage', {}).get('holdingList', [])
            }
        }
        return data

    def getRealtimeToken(self):
        link = f'https://www.us-api.morningstar.com/EC-config/v1/configurationfiles/core.sal-wrapped-fund-report-premium-v4_12_1/prod'
        if not self.token:
            return None, None
        headers['Authorization'] = f'Bearer {self.token}'
        loaded = False
        for i in range(retry_count):
            proxy = pickProxy()
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                resp = requests.get(link, headers=headers,
                                    timeout=timeout, proxies=proxies).json()
                loaded = True
                break
            except:
                print("Failed to open {}".format(link))
        if not loaded:
            return None, None
        try:
            token = resp.get('settings', {}).get('sal', {}).get(
                'service', {}).get('tokenRealtime')
            salcontenttype = resp.get('settings', {}).get(
                'sal', {}).get('service', {}).get('salContentType')
        except:
            print("Failed to get token")
            return None, None
        return token, salcontenttype


def processInThread(SecId):
    print(f"Processing {SecId}")
    if overview_enabled:
        for i in range(retry_count):
            try:
                overview = getOverview(SecId)
                if overview.get('Flag'):
                    if overview.get('Flag') == 'Deleted':
                        addToDB('etf_overview', overview, True)
                    else:
                        addToDB('etf_overview', overview)
                    break
                # skip if no overview and not last retry
                if not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark') and i < retry_count - 1:
                    continue
                print(json.dumps(overview, indent=4))
                # check in the last loop if we have missing
                if i == retry_count - 1 and (not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark')):
                    overview['Flag'] = '5xFailed'
                    overview['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('etf_overview', overview)
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
                        addToDB('etf_documents', documents, True)
                    else:
                        addToDB('etf_documents', documents)
                    break
                # skip if no documents and not last retry
                if not documents.get('documents') and i < retry_count - 1:
                    continue
                print(documents)
                # check in the last loop if we have missing
                if i == retry_count - 1 and not documents.get('documents'):
                    documents['Flag'] = '5xFailed'
                    documents['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('etf_documents', documents)
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
                        addToDB('etf_risk_n_rating', risk_n_rating, True)
                    else:
                        addToDB('etf_risk_n_rating', risk_n_rating)
                    break
                # skip if no risk_n_rating and not last retry
                if not risk_n_rating.get('rating') and not risk_n_rating.get('risk') and i < retry_count - 1:
                    continue
                print(risk_n_rating)
                if i == retry_count - 1 and (not risk_n_rating.get('rating') and not risk_n_rating.get('risk')):
                    risk_n_rating['Flag'] = '5xFailed'
                    risk_n_rating['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('etf_risk_n_rating', risk_n_rating)
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
                        addToDB('etf_performance', performance, True)
                    else:
                        addToDB('etf_performance', performance)
                    break
                # skip if no performance and not last retry
                if not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns') and i < retry_count - 1:
                    continue
                print(json.dumps(performance, indent=4))
                if i == retry_count - 1 and (not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns')):
                    performance['Flag'] = '5xFailed'
                    performance['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('etf_performance', performance)
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
                        addToDB('etf_management', management, True)
                    else:
                        addToDB('etf_management', management)
                    break
                # skip if no management and not last retry
                if not management.get('management') and not management.get('otherShareClasses') and not management.get('manager') and i < retry_count - 1:
                    continue
                print(json.dumps(management, indent=4))
                if i == retry_count - 1 and (not management.get('management') and not management.get('otherShareClasses') and not management.get('manager')):
                    management['Flag'] = '5xFailed'
                    management['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('etf_management', management)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')
    if portfolio_enabled:
        for i in range(2):
            try:
                portfolio = Portfolio(SecId)
                portfolio_data = portfolio.processEverything()
                if portfolio_data.get('Flag'):
                    if portfolio_data.get('Flag') == 'Deleted':
                        addToDB('etf_portfolio', portfolio_data, True)
                    else:
                        addToDB('etf_portfolio', portfolio_data)
                    break
                # skip if no portfolio and not last retry
                if not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings') and i < retry_count - 1:
                    continue
                print(json.dumps(portfolio_data, indent=4))
                # check in the last loop if we have missing
                if i == retry_count - 1 and (not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings')):
                    portfolio_data['Flag'] = '5xFailed'
                    portfolio_data['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('etf_portfolio', portfolio_data)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb("ETF Other Tabs Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(input_file_path, 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("ETF Other Tabs Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
