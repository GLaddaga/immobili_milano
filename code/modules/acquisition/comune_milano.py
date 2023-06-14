import requests
import logging
import logging.handlers
import json
import pandas as pd

def get_mi_datasets(datasets):
    for name, id in datasets.items():
        response_start = requests.get(f"https://dati.comune.milano.it/api/3/action/datastore_search?resource_id={id}&limit=1")
        if response_start.status_code == 200:
            limit = json.loads(response_start.text)["result"]["total"]
            response = requests.get(f"https://dati.comune.milano.it/api/3/action/datastore_search?resource_id={id}&limit={limit}")
            if response.status_code == 200:
                (pd.json_normalize(json.loads(response.text)["result"]["records"])).to_csv(f"./data/raw/{name}.csv", sep = ";", index = False)
            else:
                logging.error(f"Errore {response.status_code} nel download del dataset {name}.")            
        else:
            logging.error(f"Errore {response_start.status_code} nell'inizializzazione del download del dataset {name}.")
    logging.info("Download datasets da Open Data - Comune di Milano terminato.")

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO, 
                        format = "%(asctime)s - %(levelname)s - %(message)s",
                        handlers = [
                            logging.StreamHandler(),
                            logging.FileHandler("./data/log/logprocess.log", mode = "a")
                        ])
    datasets = {
        "underground_stops": "0f4d4d05-b379-45a4-9a10-412a34708484",
        "surface_stops": "2a52d51d-66fe-480b-a101-983aa2f6cbc3",
        "surface_lines": "67bbf039-a22b-4ee3-b32e-31735bf1354d",
        "train_stations": "8f3fd2c6-004b-4e23-8d51-852714ce8230",
        "nurseries": "372d6b0e-af5b-4dad-a9c3-4fa0318a7dc8",
        "preschools": "85ccb4f0-c892-48d7-b5fb-9558d4c4d041",
        "primary_schools": "3110f679-4613-47b8-9c7a-3100b9516670",
        "middle_schools": "a78a8e44-76cc-438f-878b-918d72135f77",
        "high_schools": "20e2f522-3112-4fa3-989c-0cc77a85cfc5",
        "universities": "22d3ec0d-b67a-410e-8004-69da64e2a416",
        "addresses": "533b4e63-3d78-4bb5-aeb4-6c5f648f7f21"
    }
    get_mi_datasets(datasets)