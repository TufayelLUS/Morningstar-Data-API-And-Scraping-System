from world_other_tabs_scraper import getManagement, recordStatsOnDb, retry_count, addToDB
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
            management = getManagement(SecId)
            if management.get('Flag'):
                if management.get('Flag') == 'Deleted':
                    addToDB(f'{country_code_db_name}_management', management, True)
                else:
                    addToDB(f'{country_code_db_name}_management', management)
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
            addToDB(f'{country_code_db_name}_management', management)
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
    recordStatsOnDb(f"World {country_code.upper()} Management Started",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sec_ids = open(f'../../daily/world/{country_code_db_name}_sec_ids.txt', 'r').read().split('\n')
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        executor.map(processInThreadWorld, sec_ids)
    recordStatsOnDb(f"World {country_code.upper()} Management Completed",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    recordStatsOnDb(f"World {country_code.upper()} Management Processed", processed)
    recordStatsOnDb(f"World {country_code.upper()} Management Empty", empty_records)
