from invest_other_tabs_scraper import getPerformance, recordStatsOnDb, retry_count, addToDB
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
    for i in range(retry_count):
        try:
            performance = getPerformance(SecId, fund_type)
            if performance.get('Flag'):
                if performance.get('Flag') == 'Deleted':
                    addToDB(f'invest_performance', performance, True)
                else:
                    addToDB(f'invest_performance', performance)
                break
            # skip if no performance and not last retry
            if not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns') and i < retry_count - 1:
                continue
            print(json.dumps(performance, indent=4))
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns')):
                performance['Flag'] = '5xFailed'
                performance['Flag Reason'] = 'Failed after 5 attempts'
                empty_records += 1
            processed += 1
            addToDB(f'invest_performance', performance)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb(f"Invest Performance Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(f'../../daily/invest/invest_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb(f"Invest Performance Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb(f"Invest Performance Processed", processed)
    recordStatsOnDb(f"Invest Performance Empty", empty_records)
