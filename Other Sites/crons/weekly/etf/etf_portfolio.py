from etf_other_tabs_scraper import getPortfolio, recordStatsOnDb, retry_count, addToDB
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import traceback
import sys

processed = 0
empty_records = 0


def processInThread(SecId):
    global processed
    global empty_records
    fund_type = SecId.split('|')[1]
    SecId = SecId.split('|')[0]
    print("Processing {}".format(SecId))
    for i in range(2):
        try:
            portfolio_data = getPortfolio(SecId, fund_type)
            if portfolio_data.get('Flag'):
                if portfolio_data.get('Flag') == 'Deleted':
                    addToDB(f'etf_portfolio', portfolio_data, True)
                else:
                    addToDB(f'etf_portfolio', portfolio_data)
                break
            # skip if no portfolio and not last retry
            if not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Holdings') and i < retry_count - 1:
                continue
            print(json.dumps(portfolio_data, indent=4))
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Holdings')):
                portfolio_data['Flag'] = '5xFailed'
                portfolio_data['Flag Reason'] = 'Failed after 5 attempts'
                empty_records += 1
            processed += 1
            addToDB(f'etf_portfolio', portfolio_data)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb(f"ETF Portfolio Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(f'../../daily/etf/etf_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb(f"ETF Portfolio Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb(f"ETF Portfolio Processed", processed)
    recordStatsOnDb(f"ETF Portfolio Empty", empty_records)
