import requests
import logging
import logging.handlers
import json
import pandas as pd


def get_mi_datasets(datasets):
    for name, id in datasets.items():
        response = requests.get(f"https://dati.comune.milano.it/api/3/action/datastore_search?resource_id={id}")
        if response.status_code == 200:
            try:
                pd.json_normalize(json.loads(response.text)["result"]["records"]).to_csv(f"./data/raw/{name}.csv", sep = ";", index = False)
            except KeyError:
                logging.ERROR(f"Errore nella lettura del JSON relativo al dataset {name}.")

        else:
            logging.ERROR(f"Errore {response.status_code} nel download del dataset {name}.")
    logging.INFO("Download datasets da Open Data - Comune di Milano terminato.")



if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO(), 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers = [
                            logging.StreamHandler(),
                            logging.FileHandler("./log/logprocess.log", mode = "a")
                        ])
    # TODO: Inserire gli id dei diversi dataflows
    datasets = {
    "fermate_metro": "",
    "fermate_superficie": "",
    "linee_superficie": "",
    "scuole_infanzia": "",
    "scuole_elementari": "",
    "scuole_medie": "",
    "scuole_superiori": ""
    }
    get_mi_datasets(datasets)