from invest_other_tabs_scraper import getOverview, recordStatsOnDb, retry_count, addToDB
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
            overview = getOverview(SecId, fund_type)
            if overview.get('Flag'):
                if overview.get('Flag') == 'Deleted':
                    addToDB(f'invest_overview', overview, True)
                else:
                    addToDB(f'invest_overview', overview)
                break
            # skip if no overview and not last retry
            if not overview.get('keyStats') and not overview.get('Returns') and not overview.get('CategoryBenchMark') and i < retry_count - 1:
                continue
            print(json.dumps(overview, indent=4))
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not overview.get('keyStats') and not overview.get('Returns') and not overview.get('CategoryBenchMark')):
                overview['Flag'] = '5xFailed'
                overview['Flag Reason'] = 'Failed after 5 attempts'
                empty_records += 1
            processed += 1
            addToDB(f'invest_overview', overview)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')

if __name__ == '__main__':
    recordStatsOnDb(f"Invest Overview Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(f'../../daily/invest/invest_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb(f"Invest Overview Completed",
                    datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb(f"Invest Overview Processed", processed)
    recordStatsOnDb(f"Invest Overview Empty", empty_records)
