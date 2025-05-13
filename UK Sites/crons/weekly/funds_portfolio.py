from funds_other_tabs_scraper import Portfolio, recordStatsOnDb, input_file_path, threads_count, retry_count, addToDB
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import traceback

processed = 0
empty_records = 0

def processInThread(SecId):
    global processed
    global empty_records
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            portfolio = Portfolio(SecId)
            portfolio_data = portfolio.processEverything()
            if portfolio_data.get('Flag'):
                if portfolio_data.get('Flag') == 'Deleted':
                    addToDB('fund_portfolio', portfolio_data, True)
                else:
                    addToDB('fund_portfolio', portfolio_data)
                break
            # skip if no portfolio and not last retry
            if not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings') and i < retry_count - 1:
                continue
            print(json.dumps(portfolio_data, indent=4))
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings')):
                portfolio_data['Flag'] = '5xFailed'
                portfolio_data['Flag Reason'] = 'Failed after 5 attempts'
                empty_records += 1
            processed += 1
            addToDB('fund_portfolio', portfolio_data)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb("Funds Portfolio Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(input_file_path, 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=11) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("Funds Portfolio Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb("Funds Portfolio Processed", processed)
    recordStatsOnDb("Funds Portfolio Empty", empty_records)
