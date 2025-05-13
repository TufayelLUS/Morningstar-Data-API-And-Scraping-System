from etf_other_tabs_scraper import getRiskNRating, recordStatsOnDb, input_file_path, threads_count, retry_count, addToDB
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
                empty_records += 1
            processed += 1
            addToDB('etf_risk_n_rating', risk_n_rating)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb("ETF Risk n Rating Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(input_file_path, 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("ETF Risk n Rating Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb("ETF Risk n Rating Processed", processed)
    recordStatsOnDb("ETF Risk n Rating Empty", empty_records)
