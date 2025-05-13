from fastapi import FastAPI, Request, Query
from fastapi.responses import StreamingResponse
from uvicorn import run
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
from bson.json_util import dumps
from typing import Optional
from enum import Enum
from io import StringIO
from datetime import datetime
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from crons.daily.world.mapping import world_api_map, world_fund_type_map
from crons.weekly.world.world_other_tabs_scraper import getPortfolio as getWorldPortfolioLive, getPerformance as getWorldPerformanceLive, getManagement as getWorldManagementLive, getOverview as getWorldOverviewLive, getRiskNRating as getWorldRiskNRatingLive
from crons.weekly.etf.etf_other_tabs_scraper import getPortfolio as getETFPortfolioLive, getPerformance as getETFPerformanceLive, getManagement as getETFManagementLive, getOverview as getETFOverviewLive, getRiskNRating as getETFRiskNRatingLive, getDocuments as getETFDocumentsLive
from crons.weekly.invest.invest_other_tabs_scraper import getPortfolio as getInvestPortfolioLive, getPerformance as getInvestPerformanceLive, getManagement as getInvestManagementLive, getOverview as getInvestOverviewLive, getRiskNRating as getInvestRiskNRatingLive, getDocuments as getInvestDocumentsLive
from crons.weekly.pension_life.pension_other_tabs_scraper import getPortfolio as getPensionPortfolioLive, getPerformance as getPensionPerformanceLive, getManagement as getPensionManagementLive, getOverview as getPensionOverviewLive, getRiskNRating as getPensionRiskNRatingLive, getDocuments as getPensionDocumentsLive
from crons.weekly.pension_life.life_other_tabs_scraper import getPortfolio as getLifePortfolioLive, getPerformance as getLifePerformanceLive, getManagement as getLifeManagementLive, getOverview as getLifeOverviewLive, getRiskNRating as getLifeRiskNRatingLive, getDocuments as getLifeDocumentsLive
from crons.weekly.equity.equity_other_tabs_scraper import getOverview as getEquityOverviewLive, getDocuments as getEquityDocumentsLive
from crons.daily.world.world_chart_daily import getChartText as getWorldChartsLive
from crons.daily.etf.etf_chart_daily import getChartText as getETFChartsLive
from crons.daily.invest.invest_chart_daily import getChartText as getInvestChartsLive
from crons.daily.pension_life.pension_life_chart_daily import getChartText as getPensionLifeChartsLive
from crons.daily.equity.equity_chart_daily import getChartText as getEquityChartsLive
from concurrent.futures import ThreadPoolExecutor


mongo_connection_string = "mongodb://localhost:27017/"
app = FastAPI()

class FundMode(str, Enum):
    equity = "equity"
    etf = "etf"
    invest = "invest"
    pension = "pension"
    life = "life"

class TabName(str, Enum):
    overview = "overview"
    charts = "charts"
    documents = "documents"
    screener = "screener"
    management = "management"
    portfolio = "portfolio"
    performance = "performance"
    risk_n_rating = "risk-n-rating"
    
class Countries(str, Enum):
    eu = "eu"
    gbr = "gbr"
    gbrofs = "gbrofs"
    aut = "aut"
    bel = "bel"
    dnk = "dnk"
    fin = "fin"
    fra = "fra"
    deu = "deu"
    irl = "irl"
    ita = "ita"
    nld = "nld"
    nor = "nor"
    prt = "prt"
    esp = "esp"
    swe = "swe"
    che = "che"

class TabNameWorld(str, Enum):
    overview = "overview"
    charts = "charts"
    management = "management"
    portfolio = "portfolio"
    performance = "performance"
    risk_n_rating = "risk-n-rating"

def connect_to_db():
    client = MongoClient(mongo_connection_string)
    db = client["morningstar"]
    return db


def close_db_connection(db):
    db.client.close()


def getFromDB(db, collection_name, filters):
    collection = db[collection_name]
    return collection.find(filters)


def countEmptyDocuments(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents(
        {"documents": {"$size": 0}})
    total_documents = collection.count_documents({})
    return empty_documents, total_documents


def countEmptyManagement(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"management": {}, "manager": {"$size": 0}})
    total_documents = collection.count_documents({})
    return empty_documents, total_documents


def countEmptyOverview(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"keyStats": {}, "Returns": {}, "CategoryBenchMark": {"$size": 0}})
    total_documents = collection.count_documents({})
    return empty_documents, total_documents


def countEmptyPerformance(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"AnnualReturns": {"$size": 0}, "TrailingReturns": {
                                      "$size": 0}, "QuarterlyReturns": {"$size": 0}})
    total_documents = collection.count_documents({})
    return empty_documents, total_documents


def countEmptyPortfolio(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"Asset Allocation": {}, "Style Measures": {}, "Exposure": {}, "Holdings": {}})
    total_documents = collection.count_documents({})
    return empty_documents, total_documents


def countEmptyRiskNRating(db, collection_name):
    collection = db[collection_name]
    empty_documents = collection.count_documents({"rating": {"$size": 0}, "risk": {}})
    total_documents = collection.count_documents({})
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
    sorted_stats = dict(sorted(stats.items(), key=lambda item: extract_date(item[1]), reverse=True))
    return sorted_stats


def countIntersectingSecIds(db, doc_col_name, rating_col_name, screener_col_name, charts_col_name):
    # Define collections
    doc_col = db[doc_col_name]
    rating_col = db[rating_col_name]
    screener_col = db[screener_col_name]
    charts_col = db[charts_col_name]
    # Get SecIds that satisfy non-empty conditions in world_eu_documents
    valid_secids_doc = set(doc_col.distinct("SecId", {
        "SecId": {"$ne": ""},
        "documents": {"$exists": True, "$ne": []}
    }))
    # Get SecIds that satisfy non-empty conditions in world_eu_risk_n_rating
    valid_secids_rating = set(rating_col.distinct("SecId", {
        "SecId": {"$ne": ""},
        "risk": {"$exists": True, "$ne": {}}
    }))
    # Get SecIds that exist in world_eu_screener
    valid_secids_screener = set(screener_col.distinct("SecId", {
        "SecId": {"$ne": ""}
    }))
    # Get SecIds that exist in world_eu_charts
    valid_secids_charts = set(charts_col.distinct("SecId", {
        "SecId": {"$ne": ""}
    }))
    # print("SecIds in world_eu_documents (non-empty):", len(valid_secids_doc))
    # print("SecIds in world_eu_risk_n_rating (non-empty):", len(valid_secids_rating))
    # print("SecIds in world_eu_screener:", len(valid_secids_screener))
    # print("SecIds in world_eu_charts:", len(valid_secids_charts))
    # Find SecIds that are present in all four collections
    final_secids = valid_secids_doc & valid_secids_rating & valid_secids_screener & valid_secids_charts
    # print("Total SecId count meeting all conditions:", len(final_secids))
    return len(final_secids)


def getLive():
    db = connect_to_db()
    world_eu_empty_documents, world_eu_total_documents = countEmptyDocuments(db, "world_eu_documents")
    world_us_empty_documents, world_us_total_documents = countEmptyDocuments(db, "world_us_documents")
    pension_empty_documents, pension_total_documents = countEmptyDocuments(db, "pension_documents")
    life_empty_documents, life_total_documents = countEmptyDocuments(db, "life_documents")
    world_eu_risk_n_rating_empty_documents, world_eu_risk_n_rating_total_documents = countEmptyRiskNRating(db, "world_eu_risk_n_rating")
    world_us_risk_n_rating_empty_documents, world_us_risk_n_rating_total_documents = countEmptyRiskNRating(db, "world_us_risk_n_rating")
    pension_risk_n_rating_empty_documents, pension_risk_n_rating_total_documents = countEmptyRiskNRating(db, "pension_risk_n_rating")
    life_risk_n_rating_empty_documents, life_risk_n_rating_total_documents = countEmptyRiskNRating(db, "life_risk_n_rating")
    world_eu_performance_empty_documents, world_eu_performance_total_documents = countEmptyPerformance(db, "world_eu_performance")
    world_us_performance_empty_documents, world_us_performance_total_documents = countEmptyPerformance(db, "world_us_performance")
    pension_performance_empty_documents, pension_performance_total_documents = countEmptyPerformance(db, "pension_performance")
    life_performance_empty_documents, life_performance_total_documents = countEmptyPerformance(db, "life_performance")
    world_eu_portfolio_empty_documents, world_eu_portfolio_total_documents = countEmptyPortfolio(db, "world_eu_portfolio")
    world_us_portfolio_empty_documents, world_us_portfolio_total_documents = countEmptyPortfolio(db, "world_us_portfolio")
    pension_portfolio_empty_documents, pension_portfolio_total_documents = countEmptyPortfolio(db, "pension_portfolio")
    life_portfolio_empty_documents, life_portfolio_total_documents = countEmptyPortfolio(db, "life_portfolio")
    world_eu_management_empty_documents, world_eu_management_total_documents = countEmptyManagement(db, "world_eu_management")
    world_us_management_empty_documents, world_us_management_total_documents = countEmptyManagement(db, "world_us_management")
    pension_management_empty_documents, pension_management_total_documents = countEmptyManagement(db, "pension_management")
    life_management_empty_documents, life_management_total_documents = countEmptyManagement(db, "life_management")
    world_eu_overview_empty_documents, world_eu_overview_total_documents = countEmptyOverview(db, "world_eu_overview")
    world_us_overview_empty_documents, world_us_overview_total_documents = countEmptyOverview(db, "world_us_overview")
    pension_overview_empty_documents, pension_overview_total_documents = countEmptyOverview(db, "pension_overview")
    life_overview_empty_documents, life_overview_total_documents = countEmptyOverview(db, "life_overview")
    world_eu_charts_total_documents = countItemsAvailable(db, "world_eu_charts")
    world_us_charts_total_documents = countItemsAvailable(db, "world_us_charts")
    pension_charts_total_documents = countItemsAvailable(db, "pension_charts")
    life_charts_total_documents = countItemsAvailable(db, "life_charts")
    world_eu_screener_total = countItemsAvailable(db, "world_eu_screener_data")
    world_us_screener_total = countItemsAvailable(db, "world_us_screener_data")
    pension_screener_total = countItemsAvailable(db, "pension_screener_data")
    life_screener_total = countItemsAvailable(db, "life_screener_data")
    try:
        world_eu_intersecting_secids = countIntersectingSecIds(db, 'world_eu_documents', 'world_eu_risk_n_rating', 'world_eu_screener_data', 'world_eu_charts')
    except:
        world_eu_intersecting_secids = 'Please refresh again'
    try:
        world_us_intersecting_secids = countIntersectingSecIds(db, 'world_us_documents', 'world_us_risk_n_rating', 'world_us_screener_data', 'world_us_charts')
    except:
        world_us_intersecting_secids = 'Please refresh again'
    try:
        pension_intersecting_secids = countIntersectingSecIds(db, 'pension_documents', 'pension_risk_n_rating', 'pension_screener_data', 'pension_charts')
    except:
        pension_intersecting_secids = 'Please refresh again'
    try:
        life_intersecting_secids = countIntersectingSecIds(db, 'life_documents', 'life_risk_n_rating', 'life_screener_data', 'life_charts')
    except:
        life_intersecting_secids = 'Please refresh again'
    close_db_connection(db)
    statistics = {
        "complete_dataset_stats": {"world_eu": world_eu_intersecting_secids, "world_us": world_us_intersecting_secids, "pension": pension_intersecting_secids, "life": life_intersecting_secids},
        "world_eu_documents": {"empty": world_eu_empty_documents, "total": world_eu_total_documents},
        "world_us_documents": {"empty": world_us_empty_documents, "total": world_us_total_documents},
        "pension_documents": {"empty": pension_empty_documents, "total": pension_total_documents},
        "life_documents": {"empty": life_empty_documents, "total": life_total_documents},
        "world_eu_risk_n_rating": {"empty": world_eu_risk_n_rating_empty_documents, "total": world_eu_risk_n_rating_total_documents},
        "world_us_risk_n_rating": {"empty": world_us_risk_n_rating_empty_documents, "total": world_us_risk_n_rating_total_documents},
        "pension_risk_n_rating": {"empty": pension_risk_n_rating_empty_documents, "total": pension_risk_n_rating_total_documents},
        "life_risk_n_rating": {"empty": life_risk_n_rating_empty_documents, "total": life_risk_n_rating_total_documents},
        "world_eu_performance": {"empty": world_eu_performance_empty_documents, "total": world_eu_performance_total_documents},
        "world_us_performance": {"empty": world_us_performance_empty_documents, "total": world_us_performance_total_documents},
        "pension_performance": {"empty": pension_performance_empty_documents, "total": pension_performance_total_documents},
        "life_performance": {"empty": life_performance_empty_documents, "total": life_performance_total_documents},
        "world_eu_portfolio": {"empty": world_eu_portfolio_empty_documents, "total": world_eu_portfolio_total_documents},
        "world_us_portfolio": {"empty": world_us_portfolio_empty_documents, "total": world_us_portfolio_total_documents},
        "pension_portfolio": {"empty": pension_portfolio_empty_documents, "total": pension_portfolio_total_documents},
        "life_portfolio": {"empty": life_portfolio_empty_documents, "total": life_portfolio_total_documents},
        "world_eu_management": {"empty": world_eu_management_empty_documents, "total": world_eu_management_total_documents},
        "world_us_management": {"empty": world_us_management_empty_documents, "total": world_us_management_total_documents},
        "pension_management": {"empty": pension_management_empty_documents, "total": pension_management_total_documents},
        "life_management": {"empty": life_management_empty_documents, "total": life_management_total_documents},
        "world_eu_overview": {"empty": world_eu_overview_empty_documents, "total": world_eu_overview_total_documents},
        "world_us_overview": {"empty": world_us_overview_empty_documents, "total": world_us_overview_total_documents},
        "pension_overview": {"empty": pension_overview_empty_documents, "total": pension_overview_total_documents},
        "life_overview": {"empty": life_overview_empty_documents, "total": life_overview_total_documents},
        "world_eu_charts": {"total": world_eu_charts_total_documents},
        "world_us_charts": {"total": world_us_charts_total_documents},
        "pension_charts": {"total": pension_charts_total_documents},
        "life_charts": {"total": life_charts_total_documents},
        "world_eu_screener": {"total": world_eu_screener_total},
        "world_us_screener": {"total": world_us_screener_total},
        "pension_screener": {"total": pension_screener_total},
        "life_screener": {"total": life_screener_total}
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
        'pension_screener_data', 'life_screener_data',
        'pension_documents', 'life_documents',
        'pension_risk_n_rating', 'life_risk_n_rating',
        'pension_management', 'life_management',
        'pension_performance', 'life_performance',
        'pension_charts', 'life_charts',
        'pension_overview', 'life_overview',
        'pension_portfolio', 'life_portfolio'
    ]
    # prepare world collection names from world map
    for value in world_api_map.values():
        collections_available.append(value + '_documents')
        collections_available.append(value + '_risk_n_rating')
        collections_available.append(value + '_management')
        collections_available.append(value + '_performance')
        collections_available.append(value + '_charts')
        collections_available.append(value + '_overview')
        collections_available.append(value + '_portfolio')
    if not collection_name:
        return {"error": "collection parameter is required and must be one of: {}".format(", ".join(collections_available))}
    if collection_name not in collections_available:
        return {"error": "Invalid collection name provided. Valid options are: {}".format(", ".join(collections_available))}
    db = connect_to_db()
    collection = db[collection_name]
    complete_filters = {}
    if Complete:
        if collection_name.startswith('world_'):
            if collection_name.endswith('_documents'):
                complete_filters['documents'] = {"$not": {"$in": [[], {}]}}
            elif collection_name.endswith('_risk_n_rating'):
                complete_filters["rating"] = {"$not": {"$in": [[], {}]}}
                complete_filters["risk"] = {"$not": {"$in": [[], {}]}}
            elif collection_name.endswith('_management'):
                complete_filters["management"] = {"$not": {"$in": [[], {}]}}
                complete_filters["manager"] = {"$not": {"$in": [[], {}]}}
            elif collection_name.endswith('_performance'):
                complete_filters["AnnualReturns"] = {"$not": {"$in": [[], {}]}}
                complete_filters["TrailingReturns"] = {"$not": {"$in": [[], {}]}}
                complete_filters["QuarterlyReturns"] = {"$not": {"$in": [[], {}]}}
            elif collection_name.endswith('_overview'):
                complete_filters["keyStats"] = {"$not": {"$in": [[], {}]}}
                complete_filters["Returns"] = {"$not": {"$in": [[], {}]}}
                complete_filters["CategoryBenchMark"] = {"$not": {"$in": [[], {}]}}
            elif collection_name.endswith('_portfolio'):
                complete_filters["Asset Allocation"] = {"$not": {"$in": [[], {}]}}
                complete_filters["Style Measures"] = {"$not": {"$in": [[], {}]}}
                complete_filters["Exposure"] = {"$not": {"$in": [[], {}]}}
                complete_filters["Holdings"] = {"$not": {"$in": [[], {}]}}
        filters.update(complete_filters)
    def data_generator():
        yield "["
        first = True
        for doc in collection.find(filters, {"_id": 0}):  # Exclude _id if not needed
            if not first:
                yield ","
            yield json.dumps(doc, default=str)  # Convert BSON to JSON-safe format
            first = False
        yield "]"
    return StreamingResponse(data_generator(), media_type="application/json",
                             headers={"Content-Disposition": f"attachment; filename={collection_name}.json"})



@app.get('/world/{country}/screener', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def screener(request: Request, country: Countries, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100)):
    country = country.lower()
    if country not in world_api_map:
        return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    collection_prefix = world_api_map[country]
    collection_name = f"{collection_prefix}_screener_data"
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getScreenerData(collection_name, filters, last_id, limit)



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


@app.get('/world/{country}/documents', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def documents(request: Request, country: Countries, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    return {"error": "World fund documents are available under screener endpoint"}
    # country = country.lower()
    # if country not in world_api_map:
    #     return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    # collection_prefix = world_api_map[country]
    # collection_name = f"{collection_prefix}_documents"
    # filters = dict(request.query_params)
    # if 'apiKey' not in filters:
    #     return {"error": "apiKey is required"}
    # if not isAuthorizedUser(filters['apiKey']):
    #     return {"error": "Unauthorized apiKey provided"}
    # del filters['apiKey']
    # last_id = Cursor
    # limit = Limit
    # return getDocuments(collection_name, filters, last_id, limit, Complete)



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



@app.get('/world/{country}/risk-n-rating', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def risk_n_rating(request: Request, country: Countries, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    country = country.lower()
    if country not in world_api_map:
        return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    collection_prefix = world_api_map[country]
    collection_name = f"{collection_prefix}_risk_n_rating"
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getRiskNRating(collection_name, filters, last_id, limit, Complete)



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



@app.get('/world/{country}/management', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def management(request: Request, country: Countries, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    country = country.lower()
    if country not in world_api_map:
        return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    collection_prefix = world_api_map[country]
    collection_name = f"{collection_prefix}_management"
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getManagement(collection_name, filters, last_id, limit, Complete)



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


@app.get('/world/{country}/performance', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def performance(request: Request, country: Countries, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    country = country.lower()
    if country not in world_api_map:
        return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    collection_prefix = world_api_map[country]
    collection_name = f"{collection_prefix}_performance"
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPerformance(collection_name, filters, last_id, limit, Complete)



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


@app.get('/world/{country}/charts', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def charts(request: Request, country: Countries, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    country = country.lower()
    if country not in world_api_map:
        return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    collection_prefix = world_api_map[country]
    collection_name = f"{collection_prefix}_charts"
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getCharts(collection_name, filters, last_id, limit, Complete)



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


@app.get('/world/{country}/overview', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def overview(request: Request, country: Countries, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    country = country.lower()
    if country not in world_api_map:
        return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    collection_prefix = world_api_map[country]
    collection_name = f"{collection_prefix}_overview"
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getOverview(collection_name, filters, last_id, limit, Complete)



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


@app.get('/world/{country}/portfolio', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def portfolio(request: Request, country: Countries, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    country = country.lower()
    if country not in world_api_map:
        return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    collection_prefix = world_api_map[country]
    collection_name = f"{collection_prefix}_portfolio"
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    last_id = Cursor
    limit = Limit
    return getPortfolio(collection_name, filters, last_id, limit, Complete)


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


@app.get('/stats/live')
async def live(apiKey: str):
    if not isAuthorizedUser(apiKey):
        return {"error": "Unauthorized apiKey provided"}
    return getLive()


@app.get('/stats')
async def stats():
    return getStats()


@app.get('/world/{country}/{tab_name}/live', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def portfolio_live(request: Request, country: Countries, tab_name: TabNameWorld, SecId: str):
    known_tabs = ['overview', 'portfolio', 'performance', 'management', 'charts', 'risk-n-rating']
    tab_name = tab_name.lower()
    if tab_name not in known_tabs:
        return {"error": "Invalid tab name provided and must any of {}".format(", ".join(known_tabs))}
    country = country.lower()
    if country not in world_api_map:
        return {"error": "Invalid country code provided and must any of {}".format(", ".join(world_api_map.keys()))}
    collection_prefix = world_api_map[country]
    collection_name = f"{collection_prefix}_{tab_name}"
    fund_type = world_fund_type_map[country]
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    if SecId.strip() == "":
        return {"error": "SecId is empty, please provide valid SecId"}
    sec_ids = SecId.split(',')
    if len(sec_ids) > 10:
        return {"error": "Too many SecIds provided. Maximum allowed is 10."}
    db = connect_to_db()
    collection = db[collection_name]
    if tab_name == 'portfolio':
        with ThreadPoolExecutor(max_workers=5) as executor:
            result = executor.map(getWorldPortfolioLive, sec_ids)
            result = list(result)
            ops = [
                UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
            ]
            if ops:
                collection.bulk_write(ops)
        close_db_connection(db)
        return {"Data": result, "Cursor": "", "Total": len(result)}
    elif tab_name == 'performance':
        with ThreadPoolExecutor(max_workers=5) as executor:
            result = executor.map(getWorldPerformanceLive, sec_ids)
            result = list(result)
            ops = [
                UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
            ]
            if ops:
                collection.bulk_write(ops)
        close_db_connection(db)
        return {"Data": result, "Cursor": "", "Total": len(result)}
    elif tab_name == 'management':
        with ThreadPoolExecutor(max_workers=5) as executor:
            result = executor.map(getWorldManagementLive, sec_ids)
            result = list(result)
            ops = [
                UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
            ]
            if ops:
                collection.bulk_write(ops)
        close_db_connection(db)
        return {"Data": result, "Cursor": "", "Total": len(result)}
    elif tab_name == 'overview':
        with ThreadPoolExecutor(max_workers=5) as executor:
            result = executor.map(getWorldOverviewLive, sec_ids)
            result = list(result)
            ops = [
                UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
            ]
            if ops:
                collection.bulk_write(ops)
        close_db_connection(db)
        return {"Data": result, "Cursor": "", "Total": len(result)}
    elif tab_name == 'charts':
        with ThreadPoolExecutor(max_workers=5) as executor:
            result = executor.map(getWorldChartsLive, sec_ids, [fund_type] * len(sec_ids))
            result = list(result)
            ops = [
                UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
            ]
            if ops:
                collection.bulk_write(ops)
        close_db_connection(db)
        return {"Data": result, "Cursor": "", "Total": len(result)}
    elif tab_name == 'risk-n-rating':
        with ThreadPoolExecutor(max_workers=5) as executor:
            result = executor.map(getWorldRiskNRatingLive, sec_ids)
            result = list(result)
            ops = [
                UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
            ]
            if ops:
                collection.bulk_write(ops)
        close_db_connection(db)
        return {"Data": result, "Cursor": "", "Total": len(result)}
    else:
        close_db_connection(db)
        return {"error": "Feature not available for {}".format(tab_name)}


@app.get('/{fund_mode}/{tab_name}', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
@app.get('/{fund_mode}/{tab_name}/{securityToken}/live', responses={200: {"description": "Successful Response", "content": {"application/json": {"example": {"Data": [], "Cursor": "", "Total": 0}}}}})
# get SecId as param
async def dynamic_handler(request: Request, fund_mode: FundMode, tab_name: TabName, securityToken: Optional[str] = None, Cursor: Optional[str] = Query(None), Limit: Optional[int] = Query(100), Complete: Optional[bool] = Query(False)):
    known_tabs = ['overview', 'charts', 'documents', 'screener', 'management', 'portfolio', 'performance', 'risk-n-rating']
    known_tabs_equity = ['overview', 'charts', 'documents', 'screener']
    fund_modes = ['equity', 'etf', 'invest', 'pension', 'life']
    tab_name = tab_name.lower()
    fund_mode = fund_mode.lower()
    if fund_mode not in fund_modes:
        return {"error": "Invalid fund mode provided and must any of {}".format(", ".join(fund_modes))}
    if fund_mode == 'equity':
        if tab_name not in known_tabs_equity:
            return {"error": "Invalid tab name provided and must any of {}".format(", ".join(known_tabs_equity))}
    else:
        if tab_name not in known_tabs:
            return {"error": "Invalid tab name provided and must any of {}".format(", ".join(known_tabs))}
    collection_name = f"{fund_mode}_{tab_name}"
    filters = dict(request.query_params)
    if 'apiKey' not in filters:
        return {"error": "apiKey is required"}
    if not isAuthorizedUser(filters['apiKey']):
        return {"error": "Unauthorized apiKey provided"}
    del filters['apiKey']
    if securityToken:
        if 'SecId' not in filters:
            return {"error": "SecId is required"}
        if tab_name == 'screener':
            return {"error": "Feature not available for screener"}
        if filters['SecId'].strip() == "":
            return {"error": "SecId is empty, please provide valid SecId"}
        db = connect_to_db()
        collection = db[collection_name]
        if securityToken:
            if fund_mode == 'equity':
                if tab_name == 'overview':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getEquityOverviewLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'charts':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getEquityChartsLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'documents':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getEquityDocumentsLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
            elif fund_mode == 'etf':
                if tab_name == 'overview':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getETFOverviewLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'charts':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getETFChartsLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'documents':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getETFDocumentsLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'management':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getETFManagementLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'portfolio':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getETFPortfolioLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'performance':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getETFPerformanceLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'risk-n-rating':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getETFRiskNRatingLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
            elif fund_mode == 'invest':
                if tab_name == 'overview':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getInvestOverviewLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'charts':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getInvestChartsLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'documents':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getInvestDocumentsLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'management':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getInvestManagementLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'portfolio':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getInvestPortfolioLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'performance':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getInvestPerformanceLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'risk-n-rating':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getInvestRiskNRatingLive, sec_ids, [securityToken] * len(sec_ids))
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
            elif fund_mode == 'pension':
                if tab_name == 'overview':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getPensionOverviewLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'charts':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getPensionLifeChartsLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'documents':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getPensionDocumentsLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'management':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getPensionManagementLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'portfolio':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getPensionPortfolioLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'performance':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getPensionPerformanceLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'risk-n-rating':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getPensionRiskNRatingLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
            elif fund_mode == 'life':
                if tab_name == 'overview':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getLifeOverviewLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'charts':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getPensionLifeChartsLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'documents':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getLifeDocumentsLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'management':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getLifeManagementLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'portfolio':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getLifePortfolioLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'performance':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getLifePerformanceLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
                elif tab_name == 'risk-n-rating':
                    sec_ids = filters['SecId'].split(',')
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        result = executor.map(getLifeRiskNRatingLive, sec_ids)
                        result = list(result)
                        ops = [
                            UpdateOne({"SecId": doc["SecId"]}, {"$set": doc}) for doc in result if isinstance(doc, dict) and "SecId" in doc
                        ]
                        if ops:
                            collection.bulk_write(ops)
                    close_db_connection(db)
                    return {"Data": result, "Cursor": "", "Total": len(result)}
        else:
            return {"error": "securityToken is required"}
    else:
        last_id = Cursor
        limit = Limit
        if tab_name == 'overview':
            return getOverview(collection_name, filters, last_id, limit, Complete)
        elif tab_name == 'charts':
            return getCharts(collection_name, filters, last_id, limit, Complete)
        elif tab_name == 'documents':
            return getDocuments(collection_name, filters, last_id, limit, Complete)
        elif tab_name == 'screener':
            return getScreenerData(collection_name + "_data", filters, last_id, limit)
        elif tab_name == 'risk-n-rating':
            collection_name = collection_name.replace("-", "_")
            return getRiskNRating(collection_name, filters, last_id, limit, Complete)
        elif tab_name == 'management':
            return getManagement(collection_name, filters, last_id, limit, Complete)
        elif tab_name == 'portfolio':
            return getPortfolio(collection_name, filters, last_id, limit, Complete)
        elif tab_name == 'performance':
            return getPerformance(collection_name, filters, last_id, limit, Complete)
        else:
            return {"error": "Feature not available for {}".format(tab_name)}
    

@app.get('/tokens')
async def securityTokens():
    '''
    <option selected="selected" value="E0EXG$XASE">American Stock Exchange</option>
			<option value="E0EXG$XMCE">Bolsa de Madrid  </option>
			<option value="E0EXG$XMEX">Bolsa Mexicana de Valores</option>
			<option value="E0EXG$XMIL">Borsa Italiana</option>
			<option value="E0EXG$XBSP">Bahia Sergipe Alagoas Stock Exchange</option>
			<option value="E0EXG$XFRA">Deutsche Boerse</option>
			<option value="E0EXG$XLIS">EuroNext Lisbon</option>
			<option value="E0EXG$XHKG">Hong Kong Stock Exchange</option>
			<option value="E0EXG$XDUB">Irish Stock Exchange</option>
			<option value="E0EXG$XKRX">Korea Exchange</option>
			<option value="E0EXG$XLON">Equity - London Stock Exchange</option>
			<option value="E0EXG$XLUX">Luxembourg Stock Exchange</option>
			<option value="E0EXG$XNAS">NASDAQ</option>
			<option value="E0EXG$XSTO">Stockholm Stock Exchange</option>
			<option value="E0EXG$XNZE">New Zealand Exchange Limited</option>
			<option value="E0EXG$XNYS">NYSE</option>
			<option value="E0EXG$EUNX">Euronext</option>
			<option value="E0EXG$XAMS">Euronext Amsterdam</option>
			<option value="E0EXG$XBRU">Euronext Brussels</option>
			<option value="E0EXG$XPAR">Euronext Paris</option>
			<option value="E0EXG$XCSE">Copenhagen Stock Exchange</option>
			<option value="E0EXG$XHEL">Helsinki Stock Exchange</option>
			<option value="E0EXG$XICE">Iceland Stock Exchange</option>
			<option value="E0EXG$XOSL">Oslo Stock Exchange</option>
			<option value="E0EXG$XSHG">Shanghai Stock Exchange</option>
			<option value="E0EXG$XSHE">Shenzhen Stock Exchange</option>
			<option value="E0EXG$XSES">Singapore Exchange</option>
			<option value="E0EXG$XSWX">SIX Swiss Exchange</option>
			<option value="E0EXG$XTAI">Taiwan Stock Exchange</option>
			<option value="E0EXG$XBKK">Stock Exchange of Thailand</option>
			<option value="E0EXG$XTKS">Tokyo Stock Exchange</option>
			<option value="E0EXG$XTSE">Toronto Stock Exchange</option>
			<option value="E0EXG$XTSX">TSX Venture Exchange</option>
			<option value="E0EXG$XWAR">Warsaw Stock Exchange</option>
			<option value="E0EXG$XWBO">Wiener Börse</option>
			<option value="E0EXG$XETR">Xetra</option>
			<option value="E0EXG$XJSE">Johannesburg Stock Exchange</option>
            '''
    all_tokens = {
        "world": world_fund_type_map.copy(),
        "etf": {
            "Europe ETF" :'ETEUR$$ALL',
            "ARCA": 'ETEXG$ARCX'
        },
        "invest": {
            "uk": "FCGBR$$ALL",
            "worldwide": "CEWWE$$ALL"
        },
        "equity": {
            "American Stock Exchange": "E0EXG$XASE",
            "Bolsa de Madrid": "E0EXG$XMCE",
            "Bolsa Mexicana de Valores": "E0EXG$XMEX",
            "Borsa Italiana": "E0EXG$XMIL",
            "Bahia Sergipe Alagoas Stock Exchange": "E0EXG$XBSP",
            "Deutsche Boerse": "E0EXG$XFRA",
            "EuroNext Lisbon": "E0EXG$XLIS",
            "Hong Kong Stock Exchange": "E0EXG$XHKG",
            "Irish Stock Exchange": "E0EXG$XDUB",
            "Korea Exchange": "E0EXG$XKRX",
            "Equity - London Stock Exchange": "E0EXG$XLON",
            "Luxembourg Stock Exchange": "E0EXG$XLUX",
            "NASDAQ": "E0EXG$XNAS",
            "Stockholm Stock Exchange": "E0EXG$XSTO",
            "New Zealand Exchange Limited": "E0EXG$XNZE",
            "NYSE": "E0EXG$XNYS",
            "Euronext": "E0EXG$EUNX",
            "Euronext Amsterdam": "E0EXG$XAMS",
            "Euronext Brussels": "E0EXG$XBRU",
            "Euronext Paris": "E0EXG$XPAR",
            "Copenhagen Stock Exchange": "E0EXG$XCSE",
            "Helsinki Stock Exchange": "E0EXG$XHEL",
            "Iceland Stock Exchange": "E0EXG$XICE",
            "Oslo Stock Exchange": "E0EXG$XOSL",
            "Shanghai Stock Exchange": "E0EXG$XSHG",
            "Shenzhen Stock Exchange": "E0EXG$XSHE",
            "Singapore Exchange": "E0EXG$XSES",
            "SIX Swiss Exchange": "E0EXG$XSWX",
            "Taiwan Stock Exchange": "E0EXG$XTAI",
            "Stock Exchange of Thailand": "E0EXG$XBKK",
            "Tokyo Stock Exchange": "E0EXG$XTKS",
            "Toronto Stock Exchange": "E0EXG$XTSE",
            "TSX Venture Exchange": "E0EXG$XTSX",
            "Warsaw Stock Exchange": "E0EXG$XWAR",
            "Wiener Börse": "E0EXG$XWBO",
            "Xetra": "E0EXG$XETR",
            "Johannesburg Stock Exchange": "E0EXG$XJSE"
        },
        "pension": {
            "pension": "SAGBR$$PSA"
        },
        "life": {
            "life": "SAGBR$$LSA"
        }
    }
    return all_tokens


if __name__ == "__main__":
    run(app, host="0.0.0.0", port=8000)
