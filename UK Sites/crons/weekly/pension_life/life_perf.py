from life_other_tabs_scraper import getPerformance, recordStatsOnDb, input_file_path, threads_count, retry_count, addToDB
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
            performance = getPerformance(SecId)
            if performance.get('Flag'):
                if performance.get('Flag') == 'Deleted':
                    addToDB('life_performance', performance, True)
                else:
                    addToDB('life_performance', performance)
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
            addToDB('life_performance', performance)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb("Life Performance Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(input_file_path, 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("Life Performance Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb("Life Performance Processed", processed)
    recordStatsOnDb("Life Performance Empty", empty_records)
