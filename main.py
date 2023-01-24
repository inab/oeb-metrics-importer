import requests as re
import json
import os
from utils import get_url, connect_db, push_entry, save_entry


def import_data():
    # 1. connect to DB/get output files
    STORAGE_MODE = os.getenv('STORAGE_MODE', 'db')

    if STORAGE_MODE =='db':
        ALAMBIQUE = os.getenv('ALAMBIQUE', 'alambique') 
        alambique = connect_db(ALAMBIQUE)

    else:
        OUTPUT_PATH = os.getenv('OUTPUT_PATH', './data/opeb_tools.json')

    # 2. Get metrics metadata from OPEB
    URL_OPEB_METRICS = os.getenv('URL_OPEB_METRICS', 'https://openebench.bsc.es/monitor/metrics/')
    content_decoded = get_url(URL_OPEB_METRICS)

    if content_decoded:
        log = {'names':[],
           'n_ok':0,
           'errors': []}
        
        for inst_dict in content_decoded:
             # 3. Add data source to each entry
            inst_dict['@data_source'] = 'opeb_metrics'

            # 4. output/push to db tools metadata
            if STORAGE_MODE=='db':
                log = push_entry(inst_dict, alambique, log)
            else:
                log = save_entry(inst_dict, OUTPUT_PATH, log)

if __name__ == "__main__":
    import_data()