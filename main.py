import requests as re
import os
import logging
import argparse
import sys
from utils import get_url, connect_db, push_entry, push_publication, add_metadata_to_entry, add_metadata_to_publication


def get_meta_from_opeb():
    '''
    '''
    logging.info('downloading OPEB metrics entries')
    URL_OPEB_METRICS = os.getenv('URL_OPEB_METRICS', 'https://openebench.bsc.es/monitor/metrics/')
    logging.info(f'openEBench metrics URL:{URL_OPEB_METRICS}')
    content_decoded = get_url(URL_OPEB_METRICS)
    return content_decoded

def get_url_from_oeb(id: str):
    '''
    For an @id, return the URL using the oeb monitoring api
    form: https://openebench.bsc.es/monitor/tool/biotools:aurocch:1.1.0.0/lib/www.mathworks.com
    data to fetch: https://openebench.bsc.es/monitor/metrics/biotools:aurocch:1.1.0.0/lib/www.mathworks.com
    '''
    url_data = id.replace('/metrics/', '/tool/')
    data = get_url(url_data)

    if data:
        if data.get('web'):
            return data['web'].get('homepage')

    return None

def process_web_metrics(inst_dict: dict):
    '''
    '''
    # extract website metrics
    data = inst_dict.get('project')
    data = data.get('website') if data else None
    if data:
        url = get_url_from_oeb(inst_dict.get('@id'))

    if data and url:
        # add metadata
        entry = {
            'data' : data,
            '@data_source'  : 'opeb_metrics',
            '_id': url
        }

        # update metadata and push
        webMetrics = connect_db('webMetrics')
        document_w_metadata = add_metadata_to_entry(url, entry, webMetrics)
        push_entry(document_w_metadata, webMetrics)

    return


def process_tool_publications(inst_dict: dict):
    '''
    push to alambque tool publications with ids, title and year
    '''
    id_ = inst_dict.get('@id')
    if len(id_.split('/'))>6:
        main = id_.split('/')[5]
        if ':' in main: 
            name = main.split(':')[1]
            if len(main.split(':'))>2:
                version = main.split(':')[2]
            else:
                version = None
        else:
            name = main
            version = None

        type_ = id_.split('/')[6]

        identifier = f"opeb_metrics/{name}/{type_}/{version}"
        inst_dict['name'] = name
        inst_dict['@type'] = type_
        inst_dict['version'] = version 

    else:
        name = id_.split('/')[5]
        inst_dict['name'] = name
        identifier = f"opeb_metrics/{name}//"
    
    entry = {
        'data' : inst_dict,
        '@data_source'  : 'opeb_metrics',
        '_id': identifier
    }

    # update metadata and push
    alambique = connect_db('alambique')
    document_w_metadata = add_metadata_to_entry(identifier, entry, alambique)
    push_entry(document_w_metadata, alambique)
    

def process_publications(inst_dict: dict):
    '''
    '''
    # extract publications
    if inst_dict.get('project'):
        if inst_dict['project'].get('publications'):
            publications = inst_dict['project']['publications']
            for pub in publications:
                if pub.get('entries'):
                    # add metadata
                    item = pub['entries'][0]
                    doi = item.get('doi')
                    pmid = item.get('pmid')
                    pmcid = item.get('pmcid')
                    if doi or pmid or pmcid:
                        entry = {
                            'data' : item,
                            '@data_source'  : 'opeb_metrics',
                            '@doi': item.get('doi'),
                            '@pmid': item.get('pmid'),
                            '@pmcid': item.get('pmcid')
                        }

                        # update metadata and push
                        publications = connect_db('publications')
                        document_w_metadata = add_metadata_to_publication(item.get('doi'), item.get('pmid'), item.get('pmcid'), entry, publications)
                        push_publication(document_w_metadata, publications)
    
    return


def import_data():
    try:
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

        logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)        


        logging.info("state_importation - 1")
        # 2. Get metrics metadata from OPEB
        content_decoded = get_meta_from_opeb()

        if content_decoded:

            logging.info(f'Processing {len(content_decoded)} OPEB metrics entries')

            for inst_dict in content_decoded:

                if inst_dict:

                    # extract website metrics
                    #process_web_metrics(inst_dict)

                    # extract publications
                    #process_publications(inst_dict)

                    # extract tool publications
                    process_tool_publications(inst_dict)
                
        else:
            logging.exception("Exception occurred")
            logging.error('error - crucial_object_empty')
            logging.error('No content to processed. content_decoded is empty. Exiting...')
            logging.info("state_importation - 2")
            exit(1) 
        
    except Exception as e:
        logging.exception("Exception occurred")
        logging.error(f'error - {type(e).__name__}')
        logging.info("state_importation - 2")
        exit(1)

    else:
        logging.info("state_importation - 0")

        
    
if __name__ == "__main__":
    import_data()