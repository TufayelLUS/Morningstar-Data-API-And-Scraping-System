import requests
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
import json
from concurrent.futures import ThreadPoolExecutor
import traceback
from datetime import datetime
import random


input_file_path = '../../daily/equity/equity_sec_ids.txt'
mongo_connection_string = "mongodb://localhost:27017/"
threads_count = 10
timeout = 40
retry_count = 5
proxy_list = ["http://ip:port", "http://ip:port"]
overview_enabled = 0
documents_enabled = 1


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
                    if cell_title:
                        row_data.append(cell_title.text.strip())
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        if not cell:
                            continue
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
    document_type_label = {
        "201": "Quarterly Report",
        "202": "Annual Report",
        "203": "Key Announcements",
        "205": "Registration",
        "206": "Significant Ownership",
        "207": "Insider Activities",
        "209": "Corporate Responsibility Report",
        "210": "Interim Report",
        "211": "Filings_Documents_211",
        "251": "Prospectus",
        "252": "Prospectus Supplement"
    }
    final_data = {
        'SecId': SecId,
        'documents': []
    }
    documents_container = {}
    link = f'https://lt.morningstar.com/api/rest.svc/security_documents/3y3wd9echv'
    for category_name in ['communication', 'financials', 'announcements']:
        documents = []
        page_no = 0
        while True:
            params = {
                'id': f'{SecId}]3]0]{fund_type}',
                'currencyId': 'GBP',
                'languageId': 'en-GB',
                'pageSize': 10000,
                'pageNumber': page_no,
                'moduleId': 59,
                'documentCategory': category_name,
                'outputType': 'json'
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
                                        timeout=timeout, params=params, proxies=proxies)
                    if resp.status_code == 404:
                        final_data['Flag'] = 'Deleted'
                        final_data['Flag Reason'] = '404'
                        return final_data
                    resp = resp.json()
                    loaded = True
                    break
                except:
                    print("Failed to open {}".format(link))
            if not loaded:
                final_data['Flag'] = 'XProxy'
                final_data['Flag Reason'] = 'Proxy Failure'
                break
            all_documents = resp.get('Documents', {}).get('Document', [])
            for document in all_documents:
                document_type = document.get('DocumentType')
                document_name = None
                document_attributes = document.get('Attributes', [])
                for attribute in document_attributes:
                    attribute = attribute.get('Attribute', [])
                    for sub_attribute in attribute:
                        if sub_attribute.get('Group') == 'Document Type':
                            document_name = sub_attribute.get('Value')
                document_url = document.get('DownloadUrl')
                try:
                    language = ", ".join([x.get('value') for x in document.get(
                        'LanguageId')[0].get('Language', [])])
                except:
                    language = 'Undefined'
                effective_date = document.get('EffectiveDate')
                # convert to dd/mm/yyyy
                if effective_date:
                    try:
                        effective_date = datetime.strptime(
                            effective_date, '%Y-%m-%d').strftime('%d/%m/%Y')
                    except:
                        effective_date = 'Undefined'
                if document_name is None:
                    document_name = document_type_label.get(document_type)
                if document_name and document_url:
                    documents.append({
                        'name': document_name,
                        'url': document_url,
                        'language': language,
                        'effective_date': effective_date
                    })
            page_no += 1
            total_pages = resp.get('Documents', {}).get(
                'TotalNumberOfPages', 1)
            if page_no >= int(total_pages):
                break
        documents_container[category_name.title()] = documents
    final_data['documents'] = documents_container
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


def getOverview(SecId, fund_type):
    final_data = {
        'SecId': SecId,
        'Title': '',
        'SummaryQuotation': {},
        'SummaryPrice': {},
        'SummaryVolume': {},
        'CompanyProfile': {},
        'Outlook': {},
        'Ratios': {},
        'TotalReturns': {},
        'keyStats': {},
        'Financials': {},
        'Dividends': [],
        'BrokerSentiment': '',
        'BrokerForecasts': {},
        'DirectorDealings': {}
    }
    link = f'https://lt.morningstar.com/3y3wd9echv/stockreport/default.aspx?tab=0&vw=sum&SecurityToken={SecId}%5D3%5D0%5D{fund_type}&Id={SecId}&ClientFund=0&CurrencyId=GBP'
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
    if not title_container:
        print("Title container not found")
    else:
        try:
            fund_title_container = title_container.find('h1')
            fund_title = fund_title_container.find('span', {'class': 'securityName'}).text.strip(
            ) + ' ' + fund_title_container.find('span', {'class': 'securitySymbol'}).text.strip()
            final_data['Title'] = fund_title
        except:
            try:
                final_data['Title'] = title_container.find('h1').text.strip()
            except:
                pass
    summary_quotation_container = soup.find(
        'div', id='IntradayPriceSummaryQuotationNarrow')
    if not summary_quotation_container:
        print("Container not found")
    else:
        try:
            last_price = summary_quotation_container.find(
                'span', {'id': 'Col0Price'}).text.strip()
        except:
            last_price = ''
        try:
            day_change = summary_quotation_container.find(
                'span', {'id': 'Col0PriceDetail'}).text.strip()
        except:
            day_change = ''
        try:
            price_time = summary_quotation_container.find(
                'p', {'id': 'Col0PriceTime'}).text.strip()
        except:
            price_time = ''
        final_data['SummaryQuotation'] = {
            'Last Price': last_price,
            'Day Change': day_change,
            'Price Time': price_time
        }
    summary_price_container = soup.find(
        'div', id='IntradayPriceSummaryPriceNarrow')
    summary_price_data = {}
    if not summary_price_container:
        print("Container not found")
    else:
        table = summary_price_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                try:
                    left_cell = row.find('th').text.strip()
                    right_cell = row.find('td').get_text(
                        separator='\n').strip()
                except:
                    continue
                summary_price_data[left_cell] = right_cell
    final_data['SummaryPrice'] = summary_price_data
    summary_volume_container = soup.find(
        'div', id='IntradayPriceSummaryVolumeNarrow')
    summary_volume_data = {}
    if not summary_volume_container:
        print("Container not found")
    else:
        table = summary_volume_container.find('table')
        if not table:
            print("Table not found")
        else:
            rows = table.find_all('tr')
            for row in rows:
                try:
                    left_cell = row.find('th').text.strip()
                    right_cell = row.find('td').get_text(
                        separator='\n').strip()
                except:
                    continue
                summary_volume_data[left_cell] = right_cell
    final_data['SummaryVolume'] = summary_volume_data
    company_profile_container = soup.find('div', id='CompanyProfile')
    if not company_profile_container:
        company_profile_container = soup.find(
            'div', id='CompanyProfileHemscott')
    company_profile = {}
    if not company_profile_container:
        print("Container not found")
    else:
        sections = company_profile_container.find_all('div', {'class': 'item'})
        for section in sections:
            try:
                header = section.find('h3').text.strip()
                value = section.find('h3').next_sibling.text.strip()
            except:
                if not 'Company Profile' in company_profile.keys():
                    header = 'Company Profile'
                    value = section.text.strip()
            company_profile[header] = value
    final_data['CompanyProfile'] = company_profile
    outlook_container = soup.find('div', id='Outlook')
    outlook_data = {}
    if not outlook_container:
        outlook_container = soup.find('div', id='OutlookHemscott')
    if not outlook_container:
        print("Container not found")
    else:
        sections = outlook_container.find_all('div', {'class': 'item'})
        for section in sections:
            try:
                header = section.find('h3').text.strip()
                value = section.find('h3').next_sibling.text.strip()
            except:
                if not 'Outlook' in outlook_data.keys():
                    header = 'Outlook'
                    value = section.text.strip()
            outlook_data[header] = value
    final_data['Outlook'] = outlook_data
    key_stat_detector = soup.find('div', id='OverviewRatios')
    if key_stat_detector:
        key_stats_or_ratios_container = getPageSectionDynamic(
            soup, 'OverviewRatios')
        if not key_stats_or_ratios_container:
            key_stats_or_ratios_container = getPageSectionDynamic(
                soup, 'OverviewRatiosHemscott')
            key_stat_detector = soup.find('div', id='OverviewRatiosHemscott')
    else:
        key_stat_detector = soup.find('div', id='OverviewRatiosHemscott')
        key_stats_or_ratios_container = getPageSectionDynamic(
            soup, 'OverviewRatiosHemscott')
        # remove incomplete keystat if the key count is less
    key_stats_or_ratios_container = [
        item for item in key_stats_or_ratios_container if len(item) > 1]
    if key_stat_detector:
        table = key_stat_detector.find('table')
        if table:
            table_heading = table.find(
                'caption', {'class': 'sectionHeader'}).text.strip()
            if table_heading == "Ratios":
                final_data['Ratios'] = key_stats_or_ratios_container
            else:
                final_data['keyStats'] = key_stats_or_ratios_container
    total_returns_container = soup.find('div', id='TotalReturns')
    total_returns_data = {}
    if not total_returns_container:
        total_returns_container = soup.find('div', id='TotalReturnsHemscott')
    if total_returns_container:
        try:
            total_returns_date = total_returns_container.find(
                'span', {'class': 'date'}).text.strip()
        except:
            total_returns_date = 'Undefined'
        total_returns_data['Date'] = total_returns_date
        all_divs = total_returns_container.find_all('div')
        for div in all_divs:
            div_name = div.get('id')
            if not div_name:
                continue
            total_returns_data[div_name] = {}
            try:
                total_returns_data[div_name]['Image'] = div.find(
                    'td', {'class': 'totalReturnsIndicator'}).find('img').get('src')
            except:
                total_returns_data[div_name]['Image'] = 'Undefined'
            total_returns_data[div_name]['Data'] = []
            table = div.find('table')
            if not table:
                continue
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
                    try:
                        row_data.append(cell_title.text.strip())
                    except:
                        continue
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    total_returns_data[div_name]['Data'].append(
                        dict(zip(item_headers, row_data)))
    final_data['TotalReturns'] = total_returns_data
    financials_container = soup.find('div', id='OverviewFinancials')
    if not financials_container:
        financials_container = soup.find(
            'div', id='OverviewFinancialsHemscott')
    financials_data = []
    if not financials_container:
        print("Container not found")
    else:
        table = financials_container.find('table')
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
                    try:
                        row_data.append(cell_title.text.strip())
                    except:
                        continue
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    financials_data.append(
                        dict(zip(item_headers, row_data)))
    financials_data = [x for x in financials_data if len(x) > 1]
    final_data['Financials'] = financials_data
    dividends_container = soup.find('div', id='OverviewDividends')
    if not dividends_container:
        dividends_container = soup.find('div', id='OverviewDividendsHemscott')
    dividend_data = []
    if not dividends_container:
        print("Container not found")
    else:
        table = dividends_container.find('table')
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
                    try:
                        row_data.append(cell_title.text.strip())
                    except:
                        continue
                    for cell in cells:
                        # if cell.text.strip() == '':
                        #     continue
                        row_data.append(cell.text.strip())
                    if not row_data:
                        continue
                    dividend_data.append(
                        dict(zip(item_headers, row_data)))
    final_data['Dividends'] = dividend_data
    broker_forecasts_detector = soup.find('div', id='OverviewBrokerForecasts')
    if broker_forecasts_detector:
        broker_forecasts_container = getPageSectionDynamic(
            soup, 'OverviewBrokerForecasts', has_empty_column_name=False)
    else:
        broker_forecasts_container = getPageSectionDynamic(
            soup, 'OverviewBrokerForecastsHemscott', has_empty_column_name=False)
    broker_forecasts_container = [
        x for x in broker_forecasts_container if len(x) > 1]
    final_data['BrokerForecasts'] = broker_forecasts_container
    director_dealings_detector = soup.find(
        'div', id='OverviewDirectorDealings')
    if director_dealings_detector:
        director_dealings_container = getPageSectionDynamic(
            soup, 'OverviewDirectorDealings', has_empty_column_name=False)
    else:
        director_dealings_container = getPageSectionDynamic(
            soup, 'OverviewDirectorDealingsHemscott', has_empty_column_name=False)
    director_dealings_container = [
        x for x in director_dealings_container if len(x) > 1]
    final_data['DirectorDealings'] = director_dealings_container
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


def processInThread(SecId):
    print(f"Processing {SecId}")
    if overview_enabled:
        for i in range(retry_count):
            try:
                overview = getOverview(SecId)
                if overview.get('Flag'):
                    if overview.get('Flag') == 'Deleted':
                        addToDB('equity_overview', overview, True)
                    else:
                        addToDB('equity_overview', overview)
                    break
                # skip if no overview and not last retry
                if not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark') and i < retry_count - 1:
                    continue
                print(json.dumps(overview, indent=4))
                # check in the last loop if we have missing
                if i == retry_count - 1 and (not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark')):
                    overview['Flag'] = '5xFailed'
                    overview['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('equity_overview', overview)
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
                        addToDB('equity_documents', documents, True)
                    else:
                        addToDB('equity_documents', documents)
                    break
                # skip if no documents and not last retry
                if not documents.get('documents') and i < retry_count - 1:
                    continue
                print(documents)
                # check in the last loop if we have missing
                if i == retry_count - 1 and not documents.get('documents'):
                    documents['Flag'] = '5xFailed'
                    documents['Flag Reason'] = 'Failed after 5 attempts'
                addToDB('equity_documents', documents)
                break
            except:
                with open('errors.txt', 'a') as f:
                    f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    # print(json.dumps(getOverview('0P00007NZP', 'E0EXG$XLON'), indent=4))
    # print(json.dumps(getOverview('0P0000CF7T', 'E0EXG$XLIS'), indent=4))
    # exit()
    recordStatsOnDb("Equity Other Tabs Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(input_file_path, 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("Equity Other Tabs Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
