from tqdm import tqdm
import requests
import random
import time
import json
import pandas as pd
import numpy as np
from utilities import utilities
import logging

def get_listings_urls(areas, user_agents):
    listings_urls = []
    for area in tqdm(areas): 
        catalogue_area = "".join(["https://www.immobiliare.it/api-next/search-list/real-estates/?fkRegione=lom&idProvincia=MI&idComune=8042&idMZona[0]=",
                            area,
                            "&idNazione=IT&idContratto=1&idCategoria=1&criterio=rilevanza&noAste=1&__lang=it&pag=",
                            str(1),
                            "&paramsCount=3&path=/vendita-case/milano/"])
        json_start = json.loads(requests.get(catalogue_area).text)
        n_pages = json_start["maxPages"]
        listings_urls.extend([listing["seo"]["url"] for listing in json_start["results"]])
        for i in range(2, n_pages + 1):
            time.sleep(0.1)
            headers = {'User-Agent': random.choice(user_agents)}
            catalogue_i = requests.get("".join(["https://www.immobiliare.it/api-next/search-list/real-estates/?fkRegione=lom&idProvincia=MI&idComune=8042&idMZona[0]=",
                                                                    area,
                                                                    "&idNazione=IT&idContratto=1&idCategoria=1&criterio=rilevanza&noAste=1&__lang=it&pag=",
                                                                    str(i),
                                                                    "&paramsCount=3&path=/vendita-case/milano/"]), 
                                                                    headers = headers)
            if catalogue_i.status_code == 200:
                try:
                    json_catalogue_i = json.loads(catalogue_i.text)
                    listings_urls_i = [listing["seo"]["url"] for listing in json_catalogue_i["results"]]
                    listings_urls.extend(listings_urls_i)
                except json.decoder.JSONDecodeError:
                    logging.error(" ".join(["JSON Decode Error at area", area, "page", str(i), "!"]))
            else:
                logging.warning(" ".join(["Status code ", str(catalogue_i.status_code), "at area", area, "page", str(i), "!"]))
    return listings_urls

def listings_urls(user_agents):
    logging.info("Retrieving listings urls...")
    areas = ['10066', '10292', '10065', '10064', '10072', '10071', '10068', '10293', '10320', '10319', '10294', '10317', '10295', '10316', '10070', '10069',
             '10059', '10057', '10321', '10067', '10056', '10055', '10054', '10296', '10318', '10060', '10061', '10053', '10047', '10049', '10050', '10046']
    listings_urls = get_listings_urls(areas, user_agents)
    listings_urls_df = pd.DataFrame.from_dict({"ID": [url.split("/")[-2] for url in listings_urls], "URL": listings_urls})
    logging.info("Listings urls retrieved!")
    listings_urls_df.drop_duplicates(subset=["ID"], keep = 'first', inplace = True)
    listings_urls_df.to_csv("./data/raw/listings_urls.csv", index = False)
    logging.info("Listing urls data available in \"raw\" folder.\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    listings_urls(utilities.list_user_agents())