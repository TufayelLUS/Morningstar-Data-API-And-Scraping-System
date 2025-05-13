from etf_other_tabs_scraper import getDocuments, recordStatsOnDb, retry_count, addToDB
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import traceback

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
            documents = getDocuments(SecId, fund_type)
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
                empty_records += 1
            processed += 1
            addToDB('etf_documents', documents)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    recordStatsOnDb("ETF Docs Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(f'../../daily/etf/etf_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(processInThread, sec_ids)
    recordStatsOnDb("ETF Docs Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb("ETF Docs Processed", processed)
    recordStatsOnDb("ETF Docs Empty", empty_records)
