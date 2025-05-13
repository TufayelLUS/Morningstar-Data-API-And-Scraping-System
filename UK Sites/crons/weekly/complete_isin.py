import csv
from pymongo import MongoClient

mongo_connection_string = "mongodb://localhost:27017/"

def generate_secid_isin_csv_fund():
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    
    # Define collections
    doc_col = db["fund_documents"]
    rating_col = db["fund_risk_n_rating"]
    screener_col = db["fund_screener_data"]
    charts_col = db["fund_charts"]
    overview_col = db["fund_overview"]
    
    # Get SecIds that satisfy non-empty conditions in various collections
    valid_secids_doc = set(doc_col.distinct("SecId", {"SecId": {"$ne": ""}, "documents": {"$exists": True, "$ne": []}}))
    valid_secids_rating = set(rating_col.distinct("SecId", {"SecId": {"$ne": ""}, "risk": {"$exists": True, "$ne": {}}}))
    valid_secids_screener = set(screener_col.distinct("SecId", {"SecId": {"$ne": ""}}))
    valid_secids_charts = set(charts_col.distinct("SecId", {"SecId": {"$ne": ""}}))
    
    # Find SecIds that are present in all four collections
    final_secids = valid_secids_doc & valid_secids_rating & valid_secids_screener & valid_secids_charts
    
    # Retrieve ISIN values for the final SecIds
    secid_isin_map = {}
    for doc in overview_col.find({"SecId": {"$in": list(final_secids)}}, {"SecId": 1, "keyStats.ISIN": 1}):
        secid_isin_map[doc["SecId"]] = doc.get("keyStats", {}).get("ISIN", "")
    
    # Write to CSV
    with open("secid_isin_fund.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["SecId", "ISIN"])
        for secid, isin in secid_isin_map.items():
            writer.writerow([secid, isin])
    
    client.close()
    print("CSV file generated: secid_isin_fund.csv")


def generate_secid_isin_csv_etf():
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    
    # Define collections
    doc_col = db["etf_documents"]
    rating_col = db["etf_risk_n_rating"]
    screener_col = db["etf_screener_data"]
    charts_col = db["etf_charts"]
    overview_col = db["etf_overview"]
    
    # Get SecIds that satisfy non-empty conditions in various collections
    valid_secids_doc = set(doc_col.distinct("SecId", {"SecId": {"$ne": ""}, "documents": {"$exists": True, "$ne": []}}))
    valid_secids_rating = set(rating_col.distinct("SecId", {"SecId": {"$ne": ""}, "risk": {"$exists": True, "$ne": {}}}))
    valid_secids_screener = set(screener_col.distinct("SecId", {"SecId": {"$ne": ""}}))
    valid_secids_charts = set(charts_col.distinct("SecId", {"SecId": {"$ne": ""}}))
    
    # Find SecIds that are present in all four collections
    final_secids = valid_secids_doc & valid_secids_rating & valid_secids_screener & valid_secids_charts
    
    # Retrieve ISIN values for the final SecIds
    secid_isin_map = {}
    for doc in overview_col.find({"SecId": {"$in": list(final_secids)}}, {"SecId": 1, "keyStats.ISIN": 1}):
        secid_isin_map[doc["SecId"]] = doc.get("keyStats", {}).get("ISIN", "")
    
    # Write to CSV
    with open("secid_isin_etf.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["SecId", "ISIN"])
        for secid, isin in secid_isin_map.items():
            writer.writerow([secid, isin])
    
    client.close()
    print("CSV file generated: secid_isin_etf.csv")

if __name__ == "__main__":
    generate_secid_isin_csv_fund()
    generate_secid_isin_csv_etf()