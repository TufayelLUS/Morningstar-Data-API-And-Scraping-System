from life_other_tabs_scraper import getManagement, recordStatsOnDb, input_file_path, threads_count, retry_count, addToDB
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
            management = getManagement(SecId)
            if management.get('Flag'):
                if management.get('Flag') == 'Deleted':
                    addToDB('life_management', management, True)
                else:
                    addToDB('life_management', management)
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
            addToDB('life_management', management)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb("Life Management Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(input_file_path, 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("Life Management Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb("Life Management Processed", processed)
    recordStatsOnDb("Life Management Empty", empty_records)
