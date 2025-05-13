from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone
import traceback

mongo_connection_string = "mongodb://localhost:27017/"


def recordStatsOnDb(key, value):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db['stats']
    old_stat = collection.find_one()
    if old_stat:
        collection.update_one({}, {'$set': {key: value}})
    else:
        collection.insert_one({key: value})
    client.close()


def updateTimeseries(collection_screener_name, collection_chart_name, item_type):
    recordStatsOnDb(f'{item_type} Timeseries Price Update Started', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    try:
        client = MongoClient(mongo_connection_string)
        db = client["morningstar"]

        item_screener_data = db[collection_screener_name]
        item_charts = db[collection_chart_name]

        # Get today's UTC 00:00 timestamp in milliseconds
        today_utc = int(datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

        # First fetch all documents we need to update
        screener_data = {doc["SecId"]: doc["GBRReturnD1"] for doc in item_screener_data.find(
            {"SecId": {"$exists": True}, "GBRReturnD1": {"$exists": True}})}

        # Fetch all charts data for these SecIds
        charts_data = list(item_charts.find({"SecId": {"$in": list(screener_data.keys())}}))

        bulk_updates = []
        batch_size = 1000

        for chart_doc in charts_data:
            sec_id = chart_doc["SecId"]
            gbr_return = screener_data.get(sec_id, 0)  # Default to 0 if not found
            
            # Process each chart in the document
            updated_charts = []
            for chart in chart_doc.get("charts", []):
                timeseries = chart.get("timeseries", [])
                if not timeseries:
                    continue  # Skip if no timeseries data
                    
                # Get last value
                last_entry = timeseries[-1]
                last_value = last_entry[1]
                
                # Calculate new value with percentage increase
                new_value = last_value * (1 + gbr_return / 100)
                
                # Create new entry with today's timestamp
                new_entry = [today_utc, new_value]
                
                # Update the chart's timeseries
                updated_timeseries = timeseries + [new_entry]
                updated_chart = {**chart, "timeseries": updated_timeseries}
                updated_charts.append(updated_chart)
            
            if updated_charts:
                bulk_updates.append(
                    UpdateOne(
                        {"SecId": sec_id},
                        {"$set": {"charts": updated_charts}}
                    )
                )

            if len(bulk_updates) >= batch_size:
                item_charts.bulk_write(bulk_updates)
                bulk_updates.clear()

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