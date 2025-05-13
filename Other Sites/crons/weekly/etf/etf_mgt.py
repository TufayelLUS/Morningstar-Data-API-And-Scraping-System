from etf_other_tabs_scraper import getManagement, recordStatsOnDb, retry_count, addToDB
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
            management = getManagement(SecId, fund_type)
            if management.get('Flag'):
                if management.get('Flag') == 'Deleted':
                    addToDB(f'etf_management', management, True)
                else:
                    addToDB(f'etf_management', management)
                break
            # skip if no management and not last retry
            if not management.get('management') and not management.get('manager') and i < retry_count - 1:
                continue
            print(json.dumps(management, indent=4))
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not management.get('management') and not management.get('manager')):
                management['Flag'] = '5xFailed'
                management['Flag Reason'] = 'Failed after 5 attempts'
                empty_records += 1
            processed += 1
            addToDB(f'etf_management', management)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb(f"ETF Management Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(f'../../daily/etf/etf_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb(f"ETF Management Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb(f"ETF Management Processed", processed)
    recordStatsOnDb(f"ETF Management Empty", empty_records)
