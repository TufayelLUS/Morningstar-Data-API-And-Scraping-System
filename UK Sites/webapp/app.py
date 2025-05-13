from fastapi import FastAPI, Request, Query
from fastapi.responses import StreamingResponse
from uvicorn import run
from pymongo import MongoClient
from bson import ObjectId
from bson.json_util import dumps
from typing import Optional
from io import StringIO
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed


mongo_connection_string = "mongodb://localhost:27017/"
app = FastAPI()


def connect_to_db():
    client = MongoClient(mongo_connection_string)
    db = client["morningstar"]
    return db


def close_db_connection(db):
    db.client.close()


def countEmptyDocuments(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents(
        {"documents": {"$size": 0}})
    total_documents = collection.estimated_document_count()
    return empty_documents, total_documents


def countEmptyManagement(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"management": {}, "otherShareClasses": {
        "$size": 0}, "manager": {"$size": 0}})
    total_documents = collection.estimated_document_count()
    return empty_documents, total_documents


def countEmptyOverview(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"keyStats": {}, "Sustainability": {
                                                 "$in": ["", None]}, "Returns": {}, "CategoryBenchMark": {"$size": 0}})
    total_documents = collection.estimated_document_count()
    return empty_documents, total_documents


def countEmptyPerformance(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"AnnualReturns": {"$size": 0}, "TrailingReturns": {
        "$size": 0}, "QuarterlyReturns": {"$size": 0}})
    total_documents = collection.estimated_document_count()
    return empty_documents, total_documents


def countEmptyPortfolio(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"Asset Allocation": {}, "Factor Profile": {
    }, "Style Measures": {}, "Exposure": {}, "Financial Metrics": {}, "Holdings": {}})
    total_documents = collection.estimated_document_count()
    return empty_documents, total_documents


def countEmptyRiskNRating(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents(
        {"rating": {"$size": 0}, "risk": {}})
    total_documents = collection.estimated_document_count()
    return empty_documents, total_documents


def countItemsAvailable(db, collection_name):
    collection = db[collection_name]
    total_documents = collection.estimated_document_count()
    return total_documents


def isAuthorizedUser(apiKey):
    db = connect_to_db()
    collection = db["users"]
    user = collection.find_one({"apiKey": apiKey})
    close_db_connection(db)
    return user is not None


def getStats():
    db = connect_to_db()
    collection = db["stats"]
    stats = collection.find_one({}, {"_id": 0})
    close_db_connection(db)
    if not stats:
        return {}
    # Function to parse date or assign lowest value if invalid

    def extract_date(value):
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except:
            return datetime.min  # Push non-date values to the bottom
    # Sort dictionary by values (dates first, non-dates last)
    sorted_stats = dict(
        sorted(stats.items(), key=lambda item: extract_date(item[1]), reverse=True))
    return sorted_stats


def countIntersectingSecIds(db, doc_col_name, rating_col_name, screener_col_name, charts_col_name):
    # Define collections
    doc_col = db[doc_col_name]
    rating_col = db[rating_col_name]
    screener_col = db[screener_col_name]
    charts_col = db[charts_col_name]
    # Get SecIds that satisfy non-empty conditions in fund_documents
    valid_secids_doc = set(doc_col.distinct("SecId", {
        "SecId": {"$ne": ""},
        "documents": {"$exists": True, "$ne": []}
    }))
    # Get SecIds that satisfy non-empty conditions in fund_risk_n_rating
    valid_secids_rating = set(rating_col.distinct("SecId", {
        "SecId": {"$ne": ""},
        "risk": {"$exists": True, "$ne": {}}
    }))
    # Get SecIds that exist in fund_screener
    valid_secids_screener = set(screener_col.distinct("SecId", {
        "SecId": {"$ne": ""}
    }))
    # Get SecIds that exist in fund_charts
    valid_secids_charts = set(charts_col.distinct("SecId", {
        "SecId": {"$ne": ""}
    }))
    # print("SecIds in fund_documents (non-empty):", len(valid_secids_doc))
    # print("SecIds in fund_risk_n_rating (non-empty):", len(valid_secids_rating))
    # print("SecIds in fund_screener:", len(valid_secids_screener))
    # print("SecIds in fund_charts:", len(valid_secids_charts))
    # Find SecIds that are present in all four collections
    final_secids = valid_secids_doc & valid_secids_rating & valid_secids_screener & valid_secids_charts
    # print("Total SecId count meeting all conditions:", len(final_secids))
    return len(final_secids)


def getLiveOld():
    fund_empty_documents, fund_total_documents = countEmptyDocuments(
        "fund_documents")
    etf_empty_documents, etf_total_documents = countEmptyDocuments(
        "etf_documents")
    pension_empty_documents, pension_total_documents = countEmptyDocuments(
        "pension_documents")
    life_empty_documents, life_total_documents = countEmptyDocuments(
        "life_documents")
    fund_risk_n_rating_empty_documents, fund_risk_n_rating_total_documents = countEmptyRiskNRating(
        "fund_risk_n_rating")
    etf_risk_n_rating_empty_documents, etf_risk_n_rating_total_documents = countEmptyRiskNRating(
        "etf_risk_n_rating")
    pension_risk_n_rating_empty_documents, pension_risk_n_rating_total_documents = countEmptyRiskNRating(
        "pension_risk_n_rating")
    life_risk_n_rating_empty_documents, life_risk_n_rating_total_documents = countEmptyRiskNRating(
        "life_risk_n_rating")
    fund_performance_empty_documents, fund_performance_total_documents = countEmptyPerformance(
        "fund_performance")
    etf_performance_empty_documents, etf_performance_total_documents = countEmptyPerformance(
        "etf_performance")
    pension_performance_empty_documents, pension_performance_total_documents = countEmptyPerformance(
        "pension_performance")
    life_performance_empty_documents, life_performance_total_documents = countEmptyPerformance(
        "life_performance")
    fund_portfolio_empty_documents, fund_portfolio_total_documents = countEmptyPortfolio(
        "fund_portfolio")
    etf_portfolio_empty_documents, etf_portfolio_total_documents = countEmptyPortfolio(
        "etf_portfolio")
    pension_portfolio_empty_documents, pension_portfolio_total_documents = countEmptyPortfolio(
        "pension_portfolio")
    life_portfolio_empty_documents, life_portfolio_total_documents = countEmptyPortfolio(
        "life_portfolio")
    fund_management_empty_documents, fund_management_total_documents = countEmptyManagement(
        "fund_management")
    etf_management_empty_documents, etf_management_total_documents = countEmptyManagement(
        "etf_management")
    pension_management_empty_documents, pension_management_total_documents = countEmptyManagement(
        "pension_management")
    life_management_empty_documents, life_management_total_documents = countEmptyManagement(
        "life_management")
    fund_overview_empty_documents, fund_overview_total_documents = countEmptyOverview(
        "fund_overview")
    etf_overview_empty_documents, etf_overview_total_documents = countEmptyOverview(
        "etf_overview")
    pension_overview_empty_documents, pension_overview_total_documents = countEmptyOverview(
        "pension_overview")
    life_overview_empty_documents, life_overview_total_documents = countEmptyOverview(
        "life_overview")
    fund_charts_total_documents = countItemsAvailable("fund_charts")
    etf_charts_total_documents = countItemsAvailable("etf_charts")
    pension_charts_total_documents = countItemsAvailable("pension_charts")
    life_charts_total_documents = countItemsAvailable("life_charts")
    fund_screener_total = countItemsAvailable("fund_screener_data")
    etf_screener_total = countItemsAvailable("etf_screener_data")
    pension_screener_total = countItemsAvailable("pension_screener_data")
    life_screener_total = countItemsAvailable("life_screener_data")
    try:
        fund_intersecting_secids = countIntersectingSecIds(
            'fund_documents', 'fund_risk_n_rating', 'fund_screener_data', 'fund_charts')
    except:
        fund_intersecting_secids = 'Please refresh again'
    try:
        etf_intersecting_secids = countIntersectingSecIds(
            'etf_documents', 'etf_risk_n_rating', 'etf_screener_data', 'etf_charts')
    except:
        etf_intersecting_secids = 'Please refresh again'
    try:
        pension_intersecting_secids = countIntersectingSecIds(
            'pension_documents', 'pension_risk_n_rating', 'pension_screener_data', 'pension_charts')
    except:
        pension_intersecting_secids = 'Please refresh again'
    try:
        life_intersecting_secids = countIntersectingSecIds(
            'life_documents', 'life_risk_n_rating', 'life_screener_data', 'life_charts')
    except:
        life_intersecting_secids = 'Please refresh again'
    statistics = {
        "complete_dataset_stats": {"fund": fund_intersecting_secids, "etf": etf_intersecting_secids, "pension": pension_intersecting_secids, "life": life_intersecting_secids},
        "fund_documents": {"empty": fund_empty_documents, "total": fund_total_documents},
        "etf_documents": {"empty": etf_empty_documents, "total": etf_total_documents},
        "pension_documents": {"empty": pension_empty_documents, "total": pension_total_documents},
        "life_documents": {"empty": life_empty_documents, "total": life_total_documents},
        "fund_risk_n_rating": {"empty": fund_risk_n_rating_empty_documents, "total": fund_risk_n_rating_total_documents},
        "etf_risk_n_rating": {"empty": etf_risk_n_rating_empty_documents, "total": etf_risk_n_rating_total_documents},
        "pension_risk_n_rating": {"empty": pension_risk_n_rating_empty_documents, "total": pension_risk_n_rating_total_documents},
        "life_risk_n_rating": {"empty": life_risk_n_rating_empty_documents, "total": life_risk_n_rating_total_documents},
        "fund_performance": {"empty": fund_performance_empty_documents, "total": fund_performance_total_documents},
        "etf_performance": {"empty": etf_performance_empty_documents, "total": etf_performance_total_documents},
        "pension_performance": {"empty": pension_performance_empty_documents, "total": pension_performance_total_documents},
        "life_performance": {"empty": life_performance_empty_documents, "total": life_performance_total_documents},
        "fund_portfolio": {"empty": fund_portfolio_empty_documents, "total": fund_portfolio_total_documents},
        "etf_portfolio": {"empty": etf_portfolio_empty_documents, "total": etf_portfolio_total_documents},
        "pension_portfolio": {"empty": pension_portfolio_empty_documents, "total": pension_portfolio_total_documents},
        "life_portfolio": {"empty": life_portfolio_empty_documents, "total": life_portfolio_total_documents},
        "fund_management": {"empty": fund_management_empty_documents, "total": fund_management_total_documents},
        "etf_management": {"empty": etf_management_empty_documents, "total": etf_management_total_documents},
        "pension_management": {"empty": pension_management_empty_documents, "total": pension_management_total_documents},
        "life_management": {"empty": life_management_empty_documents, "total": life_management_total_documents},
        "fund_overview": {"empty": fund_overview_empty_documents, "total": fund_overview_total_documents},
        "etf_overview": {"empty": etf_overview_empty_documents, "total": etf_overview_total_documents},
        "pension_overview": {"empty": pension_overview_empty_documents, "total": pension_overview_total_documents},
        "life_overview": {"empty": life_overview_empty_documents, "total": life_overview_total_documents},
        "fund_charts": {"total": fund_charts_total_documents},
        "etf_charts": {"total": etf_charts_total_documents},
        "pension_charts": {"total": pension_charts_total_documents},
        "life_charts": {"total": life_charts_total_documents},
        "fund_screener": {"total": fund_screener_total},
        "etf_screener": {"total": etf_screener_total},
        "pension_screener": {"total": pension_screener_total},
        "life_screener": {"total": life_screener_total}
    }
    return statistics


def getLive(db):
    results = {}

    def safe_call(key, fn, *args):
        try:
            return key, fn(db, *args)
        except:
            return key, 'Please refresh again'

    tasks = [
        ("fund_documents", countEmptyDocuments, "fund_documents"),
        ("etf_documents", countEmptyDocuments, "etf_documents"),
        ("pension_documents", countEmptyDocuments, "pension_documents"),
        ("life_documents", countEmptyDocuments, "life_documents"),

        ("fund_risk", countEmptyRiskNRating, "fund_risk_n_rating"),
        ("etf_risk", countEmptyRiskNRating, "etf_risk_n_rating"),
        ("pension_risk", countEmptyRiskNRating, "pension_risk_n_rating"),
        ("life_risk", countEmptyRiskNRating, "life_risk_n_rating"),

        ("fund_perf", countEmptyPerformance, "fund_performance"),
        ("etf_perf", countEmptyPerformance, "etf_performance"),
        ("pension_perf", countEmptyPerformance, "pension_performance"),
        ("life_perf", countEmptyPerformance, "life_performance"),

        ("fund_port", countEmptyPortfolio, "fund_portfolio"),
        ("etf_port", countEmptyPortfolio, "etf_portfolio"),
        ("pension_port", countEmptyPortfolio, "pension_portfolio"),
        ("life_port", countEmptyPortfolio, "life_portfolio"),

        ("fund_mgmt", countEmptyManagement, "fund_management"),
        ("etf_mgmt", countEmptyManagement, "etf_management"),
        ("pension_mgmt", countEmptyManagement, "pension_management"),
        ("life_mgmt", countEmptyManagement, "life_management"),

        ("fund_ov", countEmptyOverview, "fund_overview"),
        ("etf_ov", countEmptyOverview, "etf_overview"),
        ("pension_ov", countEmptyOverview, "pension_overview"),
        ("life_ov", countEmptyOverview, "life_overview"),

        ("fund_charts", countItemsAvailable, "fund_charts"),
        ("etf_charts", countItemsAvailable, "etf_charts"),
        ("pension_charts", countItemsAvailable, "pension_charts"),
        ("life_charts", countItemsAvailable, "life_charts"),

        ("fund_screener", countItemsAvailable, "fund_screener_data"),
        ("etf_screener", countItemsAvailable, "etf_screener_data"),
        ("pension_screener", countItemsAvailable, "pension_screener_data"),
        ("life_screener", countItemsAvailable, "life_screener_data"),

        ("fund_complete", countIntersectingSecIds, "fund_documents",
         "fund_risk_n_rating", "fund_screener_data", "fund_charts"),
        ("etf_complete", countIntersectingSecIds, "etf_documents",
         "etf_risk_n_rating", "etf_screener_data", "etf_charts"),
        ("pension_complete", countIntersectingSecIds, "pension_documents",
         "pension_risk_n_rating", "pension_screener_data", "pension_charts"),
        ("life_complete", countIntersectingSecIds, "life_documents",
         "life_risk_n_rating", "life_screener_data", "life_charts"),
    ]

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(safe_call, key, fn, *args)
                   for key, fn, *args in tasks]
        for future in as_completed(futures):
            key, value = future.result()
            results[key] = value

    statistics = {
        "complete_dataset_stats": {
            "fund": results["fund_complete"],
            "etf": results["etf_complete"],
            "pension": results["pension_complete"],
            "life": results["life_complete"]
        },
        "fund_documents": {"empty": results["fund_documents"][0], "total": results["fund_documents"][1]},
        "etf_documents": {"empty": results["etf_documents"][0], "total": results["etf_documents"][1]},
        "pension_documents": {"empty": results["pension_documents"][0], "total": results["pension_documents"][1]},
        "life_documents": {"empty": results["life_documents"][0], "total": results["life_documents"][1]},
        "fund_risk_n_rating": {"empty": results["fund_risk"][0], "total": results["fund_risk"][1]},
        "etf_risk_n_rating": {"empty": results["etf_risk"][0], "total": results["etf_risk"][1]},
        "pension_risk_n_rating": {"empty": results["pension_risk"][0], "total": results["pension_risk"][1]},
        "life_risk_n_rating": {"empty": results["life_risk"][0], "total": results["life_risk"][1]},
        "fund_performance": {"empty": results["fund_perf"][0], "total": results["fund_perf"][1]},
        "etf_performance": {"empty": results["etf_perf"][0], "total": results["etf_perf"][1]},
        "pension_performance": {"empty": results["pension_perf"][0], "total": results["pension_perf"][1]},
        "life_performance": {"empty": results["life_perf"][0], "total": results["life_perf"][1]},
        "fund_portfolio": {"empty": results["fund_port"][0], "total": results["fund_port"][1]},
        "etf_portfolio": {"empty": results["etf_port"][0], "total": results["etf_port"][1]},
        "pension_portfolio": {"empty": results["pension_port"][0], "total": results["pension_port"][1]},
        "life_portfolio": {"empty": results["life_port"][0], "total": results["life_port"][1]},
        "fund_management": {"empty": results["fund_mgmt"][0], "total": results["fund_mgmt"][1]},
        "etf_management": {"empty": results["etf_mgmt"][0], "total": results["etf_mgmt"][1]},
        "pension_management": {"empty": results["pension_mgmt"][0], "total": results["pension_mgmt"][1]},
        "life_management": {"empty": results["life_mgmt"][0], "total": results["life_mgmt"][1]},
        "fund_overview": {"empty": results["fund_ov"][0], "total": results["fund_ov"][1]},
        "etf_overview": {"empty": results["etf_ov"][0], "total": results["etf_ov"][1]},
        "pension_overview": {"empty": results["pension_ov"][0], "total": results["pension_ov"][1]},
        "life_overview": {"empty": results["life_ov"][0], "total": results["life_ov"][1]},
        "fund_charts": {"total": results["fund_charts"]},
        "etf_charts": {"total": results["etf_charts"]},
        "pension_charts": {"total": results["pension_charts"]},
        "life_charts": {"total": results["life_charts"]},
        "fund_screener": {"total": results["fund_screener"]},
        "etf_screener": {"total": results["etf_screener"]},
        "pension_screener": {"total": results["pension_screener"]},
        "life_screener": {"total": results["life_screener"]}
    }

    return statistics


def getScreenerData(collection_name, filter={}, last_id=None, limit=100):
    db = connect_to_db()
    collection = db[collection_name]
    query = filter.copy()
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}
    query.pop("Cursor", None)
    query.pop("Limit", None)
    total = collection.estimated_document_count()
    screenerData = collection.find(query).sort("_id", 1).limit(limit)
    result = []
    for doc in screenerData:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    new_last_id = result[-1]["_id"] if result else None
    close_db_connection(db)
    return {"Data": result, "Cursor": new_last_id, "Total": total}


def getDocuments(collection_name, filter={}, last_id=None, limit=100, Complete=False):
    db = connect_to_db()
    collection = db[collection_name]
    query = filter.copy()
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}
    query.pop("Cursor", None)
    query.pop("Limit", None)
    query.pop("Complete", None)
    total = collection.estimated_document_count()
    if Complete:
        query["documents"] = {"$not": {"$in": [[], {}]}}
    documents = collection.find(query).sort("_id", 1).limit(limit)
    result = []
    for doc in documents:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    new_last_id = result[-1]["_id"] if result else None
    close_db_connection(db)
    return {"Data": result, "Cursor": new_last_id, "Total": total}


def getRiskNRating(collection_name, filter={}, last_id=None, limit=100, Complete=False):
    db = connect_to_db()
    collection = db[collection_name]
    query = filter.copy()
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}
    query.pop("Cursor", None)
    query.pop("Limit", None)
    query.pop("Complete", None)
    total = collection.estimated_document_count()
    if Complete:
        query["rating"] = {"$not": {"$in": [[], {}]}}
        query["risk"] = {"$not": {"$in": [[], {}]}}
    riskNRating = collection.find(query).sort("_id", 1).limit(limit)
    result = []
    for doc in riskNRating:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    new_last_id = result[-1]["_id"] if result else None
    close_db_connection(db)
    return {"Data": result, "Cursor": new_last_id, "Total": total}


def getManagement(collection_name, filter={}, last_id=None, limit=100, Complete=False):
    db = connect_to_db()
    collection = db[collection_name]
    query = filter.copy()
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}
    query.pop("Cursor", None)
    query.pop("Limit", None)
    query.pop("Complete", None)
    total = collection.estimated_document_count()
    if Complete:
        query["management"] = {"$not": {"$in": [[], {}]}}
        query["manager"] = {"$not": {"$in": [[], {}]}}
        query["otherShareClasses"] = {"$not": {"$in": [[], {}]}}
    management = collection.find(query).sort("_id", 1).limit(limit)
    result = []
    for doc in management:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    new_last_id = result[-1]["_id"] if result else None
    close_db_connection(db)
    return {"Data": result, "Cursor": new_last_id, "Total": total}


def getPerformance(collection_name, filter={}, last_id=None, limit=100, Complete=False):
    db = connect_to_db()
    collection = db[collection_name]
    query = filter.copy()
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}
    query.pop("Cursor", None)
    query.pop("Limit", None)
    query.pop("Complete", None)
    total = collection.estimated_document_count()
    if Complete:
        query["AnnualReturns"] = {"$not": {"$in": [[], {}]}}
        query["TrailingReturns"] = {"$not": {"$in": [[], {}]}}
        query["QuarterlyReturns"] = {"$not": {"$in": [[], {}]}}
    performance = collection.find(query).sort("_id", 1).limit(limit)
    result = []
    for doc in performance:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    new_last_id = result[-1]["_id"] if result else None
    close_db_connection(db)
    return {"Data": result, "Cursor": new_last_id, "Total": total}


def getCharts(collection_name, filter={}, last_id=None, limit=100, Complete=False):
    db = connect_to_db()
    collection = db[collection_name]
    query = filter.copy()
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}
    query.pop("Cursor", None)
    query.pop("Limit", None)
    query.pop("Complete", None)
    total = collection.estimated_document_count()
    if Complete:
        query["charts"] = {"$not": {"$in": [[], {}]}}
    charts = collection.find(query).sort("_id", 1).limit(limit)
    result = []
    for doc in charts:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    new_last_id = result[-1]["_id"] if result else None
    close_db_connection(db)
    return {"Data": result, "Cursor": new_last_id, "Total": total}


def getOverview(collection_name, filter={}, last_id=None, limit=100, Complete=False):
    db = connect_to_db()
    collection = db[collection_name]
    query = filter.copy()
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}
    query.pop("Cursor", None)
    query.pop("Limit", None)
    query.pop("Complete", None)
    total = collection.estimated_document_count()
    if Complete:
        query["keyStats"] = {"$not": {"$in": [[], {}]}}
        query["Returns"] = {"$not": {"$in": [[], {}]}}
        query["CategoryBenchMark"] = {"$not": {"$in": [[], {}]}}
    overview = collection.find(query).sort("_id", 1).limit(limit)
    result = []
    for doc in overview:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    new_last_id = result[-1]["_id"] if result else None
    close_db_connection(db)
    return {"Data": result, "Cursor": new_last_id, "Total": total}


def getPortfolio(collection_name, filter={}, last_id=None, limit=100, Complete=False):
    db = connect_to_db()
    collection = db[collection_name]
    query = filter.copy()
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}
    query.pop("Cursor", None)
    query.pop("Limit", None)
    query.pop("Complete", None)
    total = collection.estimated_document_count()
    if Complete:
        query["Asset Allocation"] = {"$not": {"$in": [[], {}]}}
        query["Factor Profile"] = {"$not": {"$in": [[], {}]}}
        query["Style Measures"] = {"$not": {"$in": [[], {}]}}
        query["Exposure"] = {"$not": {"$in": [[], {}]}}
        query["Financial Metrics"] = {"$not": {"$in": [[], {}]}}
        query["Holdings"] = {"$not": {"$in": [[], {}]}}
    portfolio = collection.find(query).sort("_id", 1).limit(limit)
    result = []
    for doc in portfolio:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    new_last_id = result[-1]["_id"] if result else None
    close_db_connection(db)
    return {"Data": result, "Cursor": new_last_id, "Total": total}


# routing starts here
@app.get("/")
async def root():
    return {"message": "API is running"}


@app.get('/download')
async def download(request: Request, collection: str = Query(None), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    collection_name = collection
    filters.pop('apiKey', None)
    filters.pop('Cursor', None)
    filters.pop('Limit', None)
    filters.pop('Complete', None)
    filters.pop('collection', None)
    collections_available = [
        'fund_screener_data', 'etf_screener_data', 'pension_screener_data', 'life_screener_data',
        'fund_documents', 'etf_documents', 'pension_documents', 'life_documents',
        'fund_risk_n_rating', 'etf_risk_n_rating', 'pension_risk_n_rating', 'life_risk_n_rating',
        'fund_management', 'etf_management', 'pension_management', 'life_management',
        'fund_performance', 'etf_performance', 'pension_performance', 'life_performance',
        'fund_charts', 'etf_charts', 'pension_charts', 'life_charts',
        'fund_overview', 'etf_overview', 'pension_overview', 'life_overview',
        'fund_portfolio', 'etf_portfolio', 'pension_portfolio', 'life_portfolio'
    ]
    if not collection_name:
        return {"error": "collection parameter is required and must be one of: {}".format(", ".join(collections_available))}
    if collection_name not in collections_available:
        return {"error": "Invalid collection name provided. Valid options are: {}".format(", ".join(collections_available))}
    db = connect_to_db()
    collection = db[collection_name]
    complete_filters = {}
    if Complete:
        if collection_name == 'fund_documents' or collection_name == 'etf_documents':
            complete_filters['documents'] = {"$not": {"$in": [[], {}]}}
        elif collection_name == 'fund_risk_n_rating' or collection_name == 'etf_risk_n_rating':
            complete_filters["rating"] = {"$not": {"$in": [[], {}]}}
            complete_filters["risk"] = {"$not": {"$in": [[], {}]}}
        elif collection_name == 'fund_management' or collection_name == 'etf_management':
            complete_filters["management"] = {"$not": {"$in": [[], {}]}}
            complete_filters["manager"] = {"$not": {"$in": [[], {}]}}
            complete_filters["otherShareClasses"] = {"$not": {"$in": [[], {}]}}
        elif collection_name == 'fund_performance' or collection_name == 'etf_performance':
            complete_filters["AnnualReturns"] = {"$not": {"$in": [[], {}]}}
            complete_filters["TrailingReturns"] = {"$not": {"$in": [[], {}]}}
            complete_filters["QuarterlyReturns"] = {"$not": {"$in": [[], {}]}}
        elif collection_name == 'fund_overview' or collection_name == 'etf_overview':
            complete_filters["keyStats"] = {"$not": {"$in": [[], {}]}}
            complete_filters["Returns"] = {"$not": {"$in": [[], {}]}}
            complete_filters["CategoryBenchMark"] = {"$not": {"$in": [[], {}]}}
        elif collection_name == 'fund_portfolio' or collection_name == 'etf_portfolio':
            complete_filters["Asset Allocation"] = {"$not": {"$in": [[], {}]}}
            complete_filters["Factor Profile"] = {"$not": {"$in": [[], {}]}}
            complete_filters["Style Measures"] = {"$not": {"$in": [[], {}]}}
            complete_filters["Exposure"] = {"$not": {"$in": [[], {}]}}
            complete_filters["Financial Metrics"] = {"$not": {"$in": [[], {}]}}
            complete_filters["Holdings"] = {"$not": {"$in": [[], {}]}}
        filters.update(complete_filters)

    def data_generator():
        yield "["
        first = True
        # Exclude _id if not needed
        for doc in collection.find(filters, {"_id": 0}):
            if not first:
                yield ","
            # Convert BSON to JSON-safe format
            yield json.dumps(doc, default=str)
            first = False
        yield "]"
    return StreamingResponse(data_generator(), media_type="application/json",
                             headers={"Content-Disposition": f"attachment; filename={collection_name}.json"})


@app.get('/fund/screener', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def screener(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getScreenerData("fund_screener_data", filters, last_id, limit)


@app.get('/etf/screener', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def etf(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getScreenerData("etf_screener_data", filters, last_id, limit)


@app.get('/pension/screener', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def pension(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getScreenerData("pension_screener_data", filters, last_id, limit)


@app.get('/life/screener', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def life(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getScreenerData("life_screener_data", filters, last_id, limit)


@app.get('/fund/documents', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def documents(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getDocuments("fund_documents", filters, last_id, limit, Complete)


@app.get('/etf/documents', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def etf_documents(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getDocuments("etf_documents", filters, last_id, limit, Complete)


@app.get('/pension/documents', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def pension_documents(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getDocuments("pension_documents", filters, last_id, limit, Complete)


@app.get('/life/documents', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def life_documents(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getDocuments("life_documents", filters, last_id, limit, Complete)


@app.get('/fund/risk-n-rating', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def risk_n_rating(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getRiskNRating("fund_risk_n_rating", filters, last_id, limit, Complete)


@app.get('/etf/risk-n-rating', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def etf_risk_n_rating(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getRiskNRating("etf_risk_n_rating", filters, last_id, limit, Complete)


@app.get('/pension/risk-n-rating', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def pension_risk_n_rating(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getRiskNRating("pension_risk_n_rating", filters, last_id, limit, Complete)


@app.get('/life/risk-n-rating', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def life_risk_n_rating(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getRiskNRating("life_risk_n_rating", filters, last_id, limit, Complete)


@app.get('/fund/management', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def management(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getManagement("fund_management", filters, last_id, limit, Complete)


@app.get('/etf/management', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def etf_management(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getManagement("etf_management", filters, last_id, limit, Complete)


@app.get('/pension/management', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def pension_management(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getManagement("pension_management", filters, last_id, limit, Complete)


@app.get('/life/management', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def life_management(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getManagement("life_management", filters, last_id, limit, Complete)


@app.get('/fund/performance', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def performance(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPerformance("fund_performance", filters, last_id, limit, Complete)


@app.get('/etf/performance', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def etf_performance(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPerformance("etf_performance", filters, last_id, limit, Complete)


@app.get('/pension/performance', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def pension_performance(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPerformance("pension_performance", filters, last_id, limit, Complete)


@app.get('/life/performance', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def life_performance(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPerformance("life_performance", filters, last_id, limit, Complete)


@app.get('/fund/charts', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def charts(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getCharts("fund_charts", filters, last_id, limit, Complete)


@app.get('/etf/charts', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def etf_charts(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getCharts("etf_charts", filters, last_id, limit, Complete)


@app.get('/pension/charts', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def pension_charts(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getCharts("pension_charts", filters, last_id, limit, Complete)


@app.get('/life/charts', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def life_charts(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getCharts("life_charts", filters, last_id, limit, Complete)


@app.get('/fund/overview', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def overview(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getOverview("fund_overview", filters, last_id, limit, Complete)


@app.get('/etf/overview', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def etf_overview(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getOverview("etf_overview", filters, last_id, limit, Complete)


@app.get('/pension/overview', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def pension_overview(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getOverview("pension_overview", filters, last_id, limit, Complete)


@app.get('/life/overview', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def life_overview(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getOverview("life_overview", filters, last_id, limit, Complete)


@app.get('/fund/portfolio', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def portfolio(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPortfolio("fund_portfolio", filters, last_id, limit, Complete)


@app.get('/etf/portfolio', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def etf_portfolio(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPortfolio("etf_portfolio", filters, last_id, limit, Complete)


@app.get('/pension/portfolio', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def pension_portfolio(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPortfolio("pension_portfolio", filters, last_id, limit, Complete)


@app.get('/life/portfolio', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def life_portfolio(request: Request, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPortfolio("life_portfolio", filters, last_id, limit, Complete)


@app.get('/stats')
async def stats():
    return getStats()


@app.get('/stats/live')
async def live(apiKey: str):
    if not isAuthorizedUser(apiKey):
        return {"error": "Unauthorized apiKey provided"}
    db = connect_to_db()
    live_stats = getLive(db)
    close_db_connection(db)
    return live_stats


if __name__ == "__main__":
    run(app, host="0.0.0.0", port=8000)
