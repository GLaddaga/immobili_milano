import json
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import time
import requests
import pickle
import os
import logging

def extract_listing_rawdata(listing_soup):
    json_webpage = json.loads(listing_soup.find("script", id = "__NEXT_DATA__").text)
    listing_data = pd.json_normalize({"title": json_webpage["props"]["pageProps"]["detailData"]["realEstate"]["title"], 
                   "id.listing": json_webpage["props"]["pageProps"]["detailData"]["realEstate"]["properties"][0]["id"],
                   **json_webpage["props"]["pageProps"]["detailData"]["realEstate"]["properties"][0],
                   **json_webpage["props"]["pageProps"]["detailData"]["realEstate"]["price"], 
                   **json_webpage["props"]["pageProps"]["detailData"]["realEstate"]["typology"], 
                   "projectlike": json_webpage["props"]["pageProps"]["detailData"]["realEstate"]["isProjectLike"]})
    return listing_data

def listings_data_extraction(i, listings_urls, listings_rawdata_backup, errors):
    listings_rawdata = listings_rawdata_backup
    logging.info("Download progress:")
    start = i
    for url in tqdm(listings_urls):
        time.sleep(0.1)
        listing_response = requests.get(url)
        status = listing_response.status_code
        if status == 200:
            listing_soup = BeautifulSoup(listing_response.text, features = "html.parser")
            listing_rawdata = extract_listing_rawdata(listing_soup)
            listings_rawdata.append(listing_rawdata)
        else:
            errors[id] = status
            logging.error(" ".join(["Status code", str(status), "at url", url]))
        if i % 100 == 0:
            with open("./data/backup/immobiliare_it_data.pkl", "wb") as checkpoint:
                pickle.dump({"logprocess": {"i": i, "errors": errors}, "rawdata": listings_rawdata}, checkpoint)
        i += 1
    return listings_rawdata
            
def listings_data():
    logging.info("Listings data scraping...")
    try:
        with open("./data/backup/immobiliare_it_data.pkl", "rb") as checkpoint:
            container = pickle.load(checkpoint)
    except FileNotFoundError:
        container = {"logprocess": {"i": 0, "errors": {}}, "rawdata": []}
    i = container["logprocess"]["i"]
    errors = container["logprocess"]["errors"]
    listings_rawdata_backup = container["rawdata"]
    listings_urls = pd.read_csv("./data/raw/listings_urls.csv")["URL"].tolist()[i:]
    listings_rawdata = listings_data_extraction(i, listings_urls, listings_rawdata_backup, errors)
    df = pd.concat(listings_rawdata)
    df.to_csv("./data/raw/annunci.csv", index = False, sep = ";")
    logging.info("Data saved in \"raw\" folder.")
    os.remove("./data/backup/immobiliare_it_data.pkl")
    logging.info("Process backup removed successfully.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    listings_data()
