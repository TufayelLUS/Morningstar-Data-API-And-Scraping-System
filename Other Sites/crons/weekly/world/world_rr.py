from world_other_tabs_scraper import getRiskNRating, recordStatsOnDb, retry_count, addToDB
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import traceback
import sys
from mapping import world_api_map, world_fund_type_map

processed = 0
empty_records = 0
fund_type = None
country_code_db_name = None



def processInThreadWorld(SecId):
    global processed
    global empty_records
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            risk_n_rating = getRiskNRating(SecId)
            if risk_n_rating.get('Flag'):
                if risk_n_rating.get('Flag') == 'Deleted':
                    addToDB(f'{country_code_db_name}_risk_n_rating', risk_n_rating, True)
                else:
                    addToDB(f'{country_code_db_name}_risk_n_rating', risk_n_rating)
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
            addToDB(f'{country_code_db_name}_risk_n_rating', risk_n_rating)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} <country_code>")
        sys.exit(1)
    country_code = sys.argv[1].lower()
    try:
        threads_count = int(sys.argv[2])
    except:
        threads_count = 10
    if country_code not in world_fund_type_map:
        print("Invalid country code")
        sys.exit(1)
    fund_type = world_fund_type_map[country_code]
    country_code_db_name = world_api_map[country_code]
    recordStatsOnDb(f"World {country_code.upper()} Risk n Rating Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(f'../../daily/world/{country_code_db_name}_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        executor.map(processInThreadWorld, sec_ids)
    recordStatsOnDb(f"World {country_code.upper()} Risk n Rating Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb(f"World {country_code.upper()} Risk n Rating Processed", processed)
    recordStatsOnDb(f"World {country_code.upper()} Risk n Rating Empty", empty_records)
