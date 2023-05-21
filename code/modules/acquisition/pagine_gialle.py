import random
from tqdm import tqdm
import time
import requests
from bs4 import BeautifulSoup
import json
import numpy as np
import pandas as pd
import logging
from utilities import utilities

# Funzione per ottenere la lista degli URL di un certo tipo di esercizi commerciali presenti in un certo territorio
def get_urls_byservice(service, city, user_agents):
  logging.info("Retrieving urls...")
  service = service.lower()
  location = city.lower()
  start_url = "/".join(["https://www.paginegialle.it/ricerca", service.lower(), location.lower()])
  services_urls = []
  i = 1
  scrape = True
  while scrape and i:
    url = "".join([start_url, "/p-", str(i)])
    time.sleep(random.randint(1, 2))
    headers = {'User-Agent': random.choice(user_agents)}
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
      soup = BeautifulSoup(response.text, features = "html.parser")
      services_urls.extend([spoonful.text for spoonful in soup.findAll("div", {"style": "display: none;"})])
      i += 1
    else:
      scrape = False
  logging.info("Urls retrieved!")
  return services_urls

# Funzione per estrarre i dati presenti negli URL di una lista di esercizi commerciali
def get_services_data(services_urls, user_agents):
  logging.info("Retrieving services data...")
  services_dict = {"name": [], "category": [], "city": [], "region": [], "postcode": [], "address": [], "latitude": [], "longitude": []}
  for url in tqdm(services_urls):
    time.sleep(random.randint(1, 2))
    headers = {'User-Agent': random.choice(user_agents)}
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
      soup = BeautifulSoup(response.text, features = "html.parser")
      json_data = json.loads(soup.find("script", {"type": "application/ld+json"}).text)
      services_dict["name"].append(json_data["name"])
      features = soup.findAll("div", {"data-tr": "scheda_azienda__info-categorie"})
      if len(features) >= 1:
        category = features[-1].find("p").text.strip().replace("\t", "").replace("\n", "").replace(".", "")
      else:
        category = ""
      services_dict["category"].append(category)
      services_dict["city"].append(json_data["address"]["addressLocality"])
      services_dict["region"].append(json_data["address"]["addressRegion"])
      services_dict["postcode"].append(json_data["address"]["postalCode"])
      services_dict["address"].append(json_data["address"]["streetAddress"])
      services_dict["latitude"].append(json_data["geo"]["latitude"])
      services_dict["longitude"].append(json_data["geo"]["longitude"])
  logging.info("Services data retrieved!")
  return pd.DataFrame.from_dict(services_dict)

def supermarkets_data(user_agents):
    logging.info("Retrieving services data from Pagine Gialle...")
    logging.info("Services: supermercati")
    supermarkets_urls = get_urls_byservice("supermercati", city = "Milano", user_agents = user_agents)
    supermarkets_data = get_services_data(services_urls = supermarkets_urls, user_agents = user_agents)
    supermarkets_data = supermarkets_data[~supermarkets_data["category"].str.contains("Abbigliamento")]
    supermarkets_data.to_csv("./data/raw/supermercati.csv", sep = ";", index = False)
    logging.info("Services data available in \"raw\" folder.\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    supermarkets_data(utilities.list_user_agents())