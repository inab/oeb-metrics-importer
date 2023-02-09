import requests as re
import json
import os
import logging
import argparse
from dotenv import load_dotenv
from utils import get_url, connect_db, push_entry, save_entry


def import_data():
    # 0.1 Set up logging
    parser = argparse.ArgumentParser(
        description="Importer of OpenEBench tools from OpenEBench Tool API"
    )
    parser.add_argument(
        "--loglevel", "-l",
        help=("Set the logging level"),
        default="INFO",
    )
    args = parser.parse_args()
    numeric_level = getattr(logging, args.loglevel.upper())

    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')

    # 0.2 Load .env
    load_dotenv()

    # 1. connect to DB/get output files
    logging.info('Connecting to database')
    STORAGE_MODE = os.getenv('STORAGE_MODE', 'db')

    if STORAGE_MODE =='db':
        alambique = connect_db()

    else:
        OUTPUT_PATH = os.getenv('OUTPUT_PATH', './data/opeb_tools.json')

    # 2. Get metrics metadata from OPEB
    logging.info('Downloading OPEB metrics entries')
    URL_OPEB_METRICS = os.getenv('URL_OPEB_METRICS', 'https://openebench.bsc.es/monitor/metrics/')
    logging.info(f'OpenEBench metrics URL:{URL_OPEB_METRICS}')
    content_decoded = get_url(URL_OPEB_METRICS)

    if content_decoded:
        log = {'names':[],
           'n_ok':0,
           'errors': []}
        
        logging.info(f'Processing {len(content_decoded)} OPEB metrics entries')
        n=0
        landmarks = {str(int((len(content_decoded)/10)*i)): f"{i*10}%" for i in range(0,11)} # 10% landmarks for logging

        for inst_dict in content_decoded:
            if str(n) in landmarks.keys():
                logging.info(f'{n}/{len(content_decoded)} ({landmarks[str(n)]}) instances pushed to database\r')
                n+=1

             # 3. Add data source to each entry
            inst_dict['@data_source'] = 'opeb_metrics'

            # 4. output/push to db tools metadata
            if STORAGE_MODE=='db':
                log = push_entry(inst_dict, alambique, log)
            else:
                log = save_entry(inst_dict, OUTPUT_PATH, log)
    
    else:
        logging.info('No content to process. Exiting.')

if __name__ == "__main__":
    import_data()