from pymongo import MongoClient
import csv

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["morningstar"]

def process_data(category):
    """
    Processes either 'etf' or 'fund' collections and generates a CSV file for missing SecIds.
    """
    # Collections
    overview_collection = db[f"{category}_overview"]
    charts_collection = db[f"{category}_charts"]

    # Fetch SecId and ISIN from overview where ISIN exists
    overview_data = {
        doc["SecId"]: doc["keyStats"]["ISIN"]
        for doc in overview_collection.find({"keyStats.ISIN": {"$exists": True, "$ne": None}}, {"SecId": 1, "keyStats.ISIN": 1})
    }

    # Fetch all SecId from charts collection
    charts_secids = {doc["SecId"] for doc in charts_collection.find({}, {"SecId": 1})}

    # Find SecIds missing in charts collection
    missing_data = [(secid, isin) for secid, isin in overview_data.items() if secid not in charts_secids]

    # Save to CSV
    csv_filename = f"missing_{category}_secids.csv"
    with open(csv_filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["SecId", "ISIN"])  # Header
        writer.writerows(missing_data)

    print(f"CSV file '{csv_filename}' created with {len(missing_data)} missing SecIds.")

# Process both ETF and Fund data
process_data("etf")
process_data("fund")
