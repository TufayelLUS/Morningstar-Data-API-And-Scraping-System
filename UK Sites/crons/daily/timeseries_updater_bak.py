from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone
import traceback

mongo_connection_string = "mongodb://localhost:27017/"


def recordStatsOnDb(key, value):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db['stats']
    # get old stat if exist
    old_stat = collection.find_one()
    if old_stat:
        collection.update_one({}, {'$set': {key: value}})
    else:
        collection.insert_one({
            key: value
        })
    client.close()


def updateTimeseries(collection_screener_name, collection_chart_name, item_type):
    recordStatsOnDb(f'{item_type} Timeseries Price Update Started', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # Connect to MongoDB
    try:
        client = MongoClient(mongo_connection_string)
        db = client["morningstar"]

        item_screener_data = db[collection_screener_name]
        item_charts = db[collection_chart_name]

        # Fetch all SecId and GBRReturnD1 values
        screener_data = {doc["SecId"]: doc["GBRReturnD1"] for doc in item_screener_data.find(
            {"SecId": {"$exists": True}, "GBRReturnD1": {"$exists": True}})}

        # Get today's UTC 00:00 timestamp in milliseconds
        today_utc = int(datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

        # Prepare bulk update operations
        bulk_updates = []
        batch_size = 1000  # You can adjust the batch size as needed

        for sec_id, gbr_return in screener_data.items():
            # Append GBRReturnD1 at today's UTC 00:00
            new_entry = [today_utc, gbr_return]

            bulk_updates.append(
                UpdateOne(
                    {"SecId": sec_id},
                    {"$push": {"charts.$[].timeseries": new_entry}}  # Assuming 'charts' is an array of objects
                )
            )

            # Execute bulk update in batches
            if len(bulk_updates) >= batch_size:
                item_charts.bulk_write(bulk_updates)
                bulk_updates.clear()  # Clear the bulk updates list to free memory

        # Execute any remaining updates if there are any
        if bulk_updates:
            item_charts.bulk_write(bulk_updates)

        client.close()

        print("Bulk update complete.")
    except:
        print("Failed to update timeseries prices.")
        with open('errors.txt', 'a') as f:
            f.write("[{}] Exception: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), traceback.format_exc()) + '\n')
    recordStatsOnDb(f'{item_type} Timeseries Price Update Completed', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))



if __name__ == '__main__':
    updateTimeseries('fund_screener_data', 'fund_charts', 'Funds')
    updateTimeseries('life_screener_data', 'life_charts', 'Life')
    updateTimeseries('pension_screener_data', 'pension_charts', 'Pension')

