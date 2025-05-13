from funds_other_tabs_scraper import mongo_connection_string, addToDB, threads_count, retry_count, getManagement as fund_getManagement, getOverview as fund_getOverview, getPerformance as fund_getPerformance, Portfolio as fund_Portfolio, getRiskNRating as fund_getRiskNRating, getDocuments as fund_getDocuments
from etf_other_tabs_scraper import getManagement as etf_getManagement, getOverview as etf_getOverview, getPerformance as etf_getPerformance, Portfolio as etf_Portfolio, getRiskNRating as etf_getRiskNRating, getDocuments as etf_getDocuments
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
import json
import traceback
import sys
from datetime import datetime


# find empty records on all collections and re-run all funds_other_tabs_scraper


def listEmptyDocuments(collection_name):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection_name]
    empty_documents = collection.find(
        {"documents": {"$size": 0}}, {"SecId": 1, "_id": 0})
    empty_documents = [doc["SecId"] for doc in empty_documents]
    client.close()
    return empty_documents


def listEmptyManagement(collection_name):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection_name]
    empty_documents = collection.find({"management": {}, "otherShareClasses": {"$size": 0}, "manager": {"$size": 0}}, {"SecId": 1, "_id": 0})
    empty_documents = [doc["SecId"] for doc in empty_documents]
    client.close()
    return empty_documents


def listEmptyOverview(collection_name):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection_name]
    empty_documents = collection.find({"keyStats": {}, "Sustainability": {"$in": ["", None]}, "Returns": {}, "CategoryBenchMark": {"$size": 0}}, {"SecId": 1, "_id": 0})
    empty_documents = [doc["SecId"] for doc in empty_documents]
    client.close()
    return empty_documents


def listEmptyPerformance(collection_name):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection_name]
    empty_documents = collection.find({"AnnualReturns": {"$size": 0}, "TrailingReturns": {
                                      "$size": 0}, "QuarterlyReturns": {"$size": 0}}, {"SecId": 1, "_id": 0})
    empty_documents = [doc["SecId"] for doc in empty_documents]
    client.close()
    return empty_documents


def listEmptyPortfolio(collection_name):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection_name]
    empty_documents = collection.find({"Asset Allocation": {}, "Factor Profile": {}, "Style Measures": {
    }, "Exposure": {}, "Financial Metrics": {}, "Holdings": {}}, {"SecId": 1, "_id": 0})
    empty_documents = [doc["SecId"] for doc in empty_documents]
    client.close()
    return empty_documents


def listEmptyRiskNRating(collection_name):
    client = MongoClient(mongo_connection_string)
    db = client['morningstar']
    collection = db[collection_name]
    empty_documents = collection.find(
        {"rating": {"$size": 0}, "risk": {}}, {"SecId": 1, "_id": 0})
    empty_documents = [doc["SecId"] for doc in empty_documents]
    client.close()
    return empty_documents


def getEtfDocumentsInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            documents = etf_getDocuments(SecId)
            if documents.get('Flag'):
                if documents.get('Flag') == 'Deleted':
                    addToDB('etf_documents', documents, True)
                else:
                    addToDB('etf_documents', documents)
                break
            # skip if no documents and not last retry
            if not documents.get('documents') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and not documents.get('documents'):
                documents['Flag'] = '5xFailed'
                documents['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(documents, indent=4))
            addToDB('etf_documents', documents)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getFundDocumentsInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            documents = fund_getDocuments(SecId)
            if documents.get('Flag'):
                if documents.get('Flag') == 'Deleted':
                    addToDB('fund_documents', documents, True)
                else:
                    addToDB('fund_documents', documents)
                break
            # skip if no documents and not last retry
            if not documents.get('documents') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and not documents.get('documents'):
                documents['Flag'] = '5xFailed'
                documents['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(documents, indent=4))
            addToDB('fund_documents', documents)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getEtfManagementInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            management = etf_getManagement(SecId)
            if management.get('Flag'):
                if management.get('Flag') == 'Deleted':
                    addToDB('etf_management', management, True)
                else:
                    addToDB('etf_management', management)
                break
            # skip if no management and not last retry
            if not management.get('management') and not management.get('otherShareClasses') and not management.get('manager') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not management.get('management') and not management.get('otherShareClasses') and not management.get('manager')):
                management['Flag'] = '5xFailed'
                management['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(management, indent=4))
            addToDB('etf_management', management)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getFundManagementInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            management = fund_getManagement(SecId)
            if management.get('Flag'):
                if management.get('Flag') == 'Deleted':
                    addToDB('fund_management', management, True)
                else:
                    addToDB('fund_management', management)
                break
            # skip if no management and not last retry
            if not management.get('management') and not management.get('otherShareClasses') and not management.get('manager') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not management.get('management') and not management.get('otherShareClasses') and not management.get('manager')):
                management['Flag'] = '5xFailed'
                management['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(management, indent=4))
            addToDB('fund_management', management)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getEtfOverviewInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            overview = etf_getOverview(SecId)
            if overview.get('Flag'):
                if overview.get('Flag') == 'Deleted':
                    addToDB('etf_overview', overview, True)
                else:
                    addToDB('etf_overview', overview)
                break
            # skip if no overview and not last retry
            if not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark')):
                overview['Flag'] = '5xFailed'
                overview['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(overview, indent=4))
            addToDB('etf_overview', overview)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getFundOverviewInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            overview = fund_getOverview(SecId)
            if overview.get('Flag'):
                if overview.get('Flag') == 'Deleted':
                    addToDB('fund_overview', overview, True)
                else:
                    addToDB('fund_overview', overview)
                break
            # skip if no overview and not last retry
            if not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not overview.get('keyStats') and not overview.get('Sustainability') and not overview.get('Returns') and not overview.get('CategoryBenchMark')):
                overview['Flag'] = '5xFailed'
                overview['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(overview, indent=4))
            addToDB('fund_overview', overview)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getEtfPerformanceInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            performance = etf_getPerformance(SecId)
            if performance.get('Flag'):
                if performance.get('Flag') == 'Deleted':
                    addToDB('etf_performance', performance, True)
                else:
                    addToDB('etf_performance', performance)
                break
            # skip if no performance and not last retry
            if not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns')):
                performance['Flag'] = '5xFailed'
                performance['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(performance, indent=4))
            addToDB('etf_performance', performance)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getFundPerformanceInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            performance = fund_getPerformance(SecId)
            if performance.get('Flag'):
                if performance.get('Flag') == 'Deleted':
                    addToDB('fund_performance', performance, True)
                else:
                    addToDB('fund_performance', performance)
                break
            # skip if no performance and not last retry
            if not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not performance.get('AnnualReturns') and not performance.get('TrailingReturns') and not performance.get('QuarterlyReturns')):
                performance['Flag'] = '5xFailed'
                performance['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(performance, indent=4))
            addToDB('fund_performance', performance)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getEtfPortfolioInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            portfolio = etf_Portfolio(SecId)
            portfolio_data = portfolio.processEverything()
            if portfolio_data.get('Flag'):
                if portfolio_data.get('Flag') == 'Deleted':
                    addToDB('etf_portfolio', portfolio_data, True)
                else:
                    addToDB('etf_portfolio', portfolio_data)
                break
            # skip if no portfolio and not last retry
            if not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings')):
                portfolio_data['Flag'] = '5xFailed'
                portfolio_data['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(portfolio_data, indent=4))
            addToDB('etf_portfolio', portfolio_data)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getFundPortfolioInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            portfolio = fund_Portfolio(SecId)
            portfolio_data = portfolio.processEverything()
            if portfolio_data.get('Flag'):
                if portfolio_data.get('Flag') == 'Deleted':
                    addToDB('fund_portfolio', portfolio_data, True)
                else:
                    addToDB('fund_portfolio', portfolio_data)
                break
            # skip if no portfolio and not last retry
            if not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not portfolio_data.get('Asset Allocation') and not portfolio_data.get('Factor Profile') and not portfolio_data.get('Style Measures') and not portfolio_data.get('Exposure') and not portfolio_data.get('Financial Metrics') and not portfolio_data.get('Holdings')):
                portfolio_data['Flag'] = '5xFailed'
                portfolio_data['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(portfolio_data, indent=4))
            addToDB('fund_portfolio', portfolio_data)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getEtfRiskNRatingInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            risk_n_rating = etf_getRiskNRating(SecId)
            if risk_n_rating.get('Flag'):
                if risk_n_rating.get('Flag') == 'Deleted':
                    addToDB('etf_risk_n_rating', risk_n_rating, True)
                else:
                    addToDB('etf_risk_n_rating', risk_n_rating)
                break
            # skip if no risk n rating and not last retry
            if not risk_n_rating.get('rating') and not risk_n_rating.get('risk') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not risk_n_rating.get('rating') and not risk_n_rating.get('risk')):
                risk_n_rating['Flag'] = '5xFailed'
                risk_n_rating['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(risk_n_rating, indent=4))
            addToDB('etf_risk_n_rating', risk_n_rating)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


def getFundRiskNRatingInThread(SecId):
    print("Processing {}".format(SecId))
    for i in range(retry_count):
        try:
            risk_n_rating = fund_getRiskNRating(SecId)
            if risk_n_rating.get('Flag'):
                if risk_n_rating.get('Flag') == 'Deleted':
                    addToDB('fund_risk_n_rating', risk_n_rating, True)
                else:
                    addToDB('fund_risk_n_rating', risk_n_rating)
                break
            # skip if no risk n rating and not last retry
            if not risk_n_rating.get('rating') and not risk_n_rating.get('risk') and i < retry_count - 1:
                continue
            # check in the last loop if we have missing
            if i == retry_count - 1 and (not risk_n_rating.get('rating') and not risk_n_rating.get('risk')):
                risk_n_rating['Flag'] = '5xFailed'
                risk_n_rating['Flag Reason'] = 'Failed after 5 attempts'
            print(json.dumps(risk_n_rating, indent=4))
            addToDB('fund_risk_n_rating', risk_n_rating)
            break
        except:
            with open('errors.txt', 'a') as f:
                f.write("[{}] SecId: {}\nException: {}".format(datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), SecId, traceback.format_exc()) + '\n')


if __name__ == '__main__':
    fund_empty_documents = listEmptyDocuments('fund_documents')
    print("Fund Empty Documents: {}".format(len(fund_empty_documents)))
    etf_empty_documents = listEmptyDocuments('etf_documents')
    print("ETF Empty Documents: {}".format(len(etf_empty_documents)))
    fund_empty_management = listEmptyManagement('fund_management')
    print("Fund Empty Management: {}".format(len(fund_empty_management)))
    etf_empty_management = listEmptyManagement('etf_management')
    print("ETF Empty Management: {}".format(len(etf_empty_management)))
    fund_empty_overview = listEmptyOverview('fund_overview')
    print("Fund Empty Overview: {}".format(len(fund_empty_overview)))
    etf_empty_overview = listEmptyOverview('etf_overview')
    print("ETF Empty Overview: {}".format(len(etf_empty_overview)))
    fund_empty_performance = listEmptyPerformance('fund_performance')
    print("Fund Empty Performance: {}".format(len(fund_empty_performance)))
    etf_empty_performance = listEmptyPerformance('etf_performance')
    print("ETF Empty Performance: {}".format(len(etf_empty_performance)))
    fund_empty_portfolio = listEmptyPortfolio('fund_portfolio')
    print("Fund Empty Portfolio: {}".format(len(fund_empty_portfolio)))
    etf_empty_portfolio = listEmptyPortfolio('etf_portfolio')
    print("ETF Empty Portfolio: {}".format(len(etf_empty_portfolio)))
    fund_empty_risk_n_rating = listEmptyRiskNRating('fund_risk_n_rating')
    print("Fund Empty Risk N Rating: {}".format(len(fund_empty_risk_n_rating)))
    etf_empty_risk_n_rating = listEmptyRiskNRating('etf_risk_n_rating')
    print("ETF Empty Risk N Rating: {}".format(len(etf_empty_risk_n_rating)))
    if len(sys.argv) == 2 and (sys.argv[1] == '-info' or sys.argv[1] == '--info'):
        exit(0)
    else:
        # documents
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getFundDocumentsInThread, fund_empty_documents)
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getEtfDocumentsInThread, etf_empty_documents)
        # management
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getEtfManagementInThread, etf_empty_management)
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getFundManagementInThread, fund_empty_management)
        # overview
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getFundOverviewInThread, fund_empty_overview)
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getEtfOverviewInThread, etf_empty_overview)
        # performance
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getEtfPerformanceInThread, etf_empty_performance)
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getFundPerformanceInThread, fund_empty_performance)
        # risk n rating
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getEtfRiskNRatingInThread, etf_empty_risk_n_rating)
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getFundRiskNRatingInThread, fund_empty_risk_n_rating)
        # portfolio
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getEtfPortfolioInThread, etf_empty_portfolio)
        with ThreadPoolExecutor(max_workers=threads_count) as executor:
            executor.map(getFundPortfolioInThread, fund_empty_portfolio)
        
