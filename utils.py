import os
import logging
import json
import requests
from pymongo import MongoClient
from pymongo.collection import Collection
from datetime import datetime

def create_metadata_url(identifier: str, alambique:Collection):
    '''
    This function first checks if the entry is already in the database.
    If the entry is in the database, it creates a metadata dictionary with the 
    following fields:
        - "@last_updated_at" : current_date
        - "@updated_by" : task_run_id
    If the entry is not in the database, in addition the the previos fields,it:
    adds the following:
        - "@url": identifier
        - "@created_at" : current_date
        - "@created_by" : task_run_id
    The metadata is returned.
    '''
    # Current timestamp
    current_date = datetime.utcnow()
    # Commit url
    CI_PROJECT_NAMESPACE = os.getenv("CI_PROJECT_NAMESPACE")
    CI_PROJECT_NAME = os.getenv("CI_PROJECT_NAME")
    CI_COMMIT_SHA = os.getenv("CI_COMMIT_SHA")
    commit_url = f"https://gitlab.bsc.es/{CI_PROJECT_NAMESPACE}/{CI_PROJECT_NAME}/-/commit/{CI_COMMIT_SHA}"
    # Prepare the metadata to add or update
    metadata = {
        "_id": identifier,
        "@last_updated_at": current_date,
        "@updated_by": commit_url,
        "@updated_logs": os.getenv("CI_PIPELINE_URL")
    }
    
    # Check if the entry exists in the database
    existing_entry = alambique.find_one({"_id": identifier})
    
    if not existing_entry:
        # This entry is new, so add additional creation metadata
        metadata.update({
            "_id": identifier,
            "@created_at": current_date,
            "@created_by": commit_url,
            "@created_logs": os.getenv("CI_PIPELINE_URL")
        })
        
    # Return the entry with the new fields
    return metadata


def build_query(doi: str,pmid: str, pmcid: str):
    '''
    This function builds a query to check if a given entry is already in the database.
    '''
    # Dynamically build the query based on non-None identifiers
    query_conditions = []
    if doi is not None:
        query_conditions.append({'doi': doi})
    if pmid is not None:
        query_conditions.append({'pmid': pmid})
    if pmcid is not None:
        query_conditions.append({'pmcid': pmcid})

    # Construct the query using the $or operator, only if there are valid conditions
    if query_conditions:
        query = {'$or': query_conditions}
    else:
        query = {}

    return query


def create_metadata_publication(doi: str,pmid: str, pmcid: str, collection:Collection):
    '''
    This function first checks if the entry is already in the database.
    If the entry is in the database, it creates a metadata dictionary with the 
    following fields:
        - "@last_updated_at" : current_date
        - "@updated_by" : task_run_id
    If the entry is not in the database, in addition the the previos fields,it:
    adds the following:
        - "@doi": doi
        - "@pmid": pmid
        - "@pmcid": pmcid
        - "@created_at" : current_date
        - "@created_by" : task_run_id
    The metadata is returned.
    '''
    # Current timestamp
    current_date = datetime.utcnow()
    # Commit url
    CI_PROJECT_NAMESPACE = os.getenv("CI_PROJECT_NAMESPACE")
    CI_PROJECT_NAME = os.getenv("CI_PROJECT_NAME")
    CI_COMMIT_SHA = os.getenv("CI_COMMIT_SHA")
    commit_url = f"https://gitlab.bsc.es/{CI_PROJECT_NAMESPACE}/{CI_PROJECT_NAME}/-/commit/{CI_COMMIT_SHA}"
    # Prepare the metadata to add or update
    metadata = {
        "@last_updated_at": current_date,
        "@updated_by": commit_url,
        "@updated_logs": os.getenv("CI_PIPELINE_URL")
    }
    
    # Check if the entry exists in the database
    query = build_query(doi, pmid, pmcid)
    existing_entry = collection.find(query)

    if not existing_entry:
        # This entry is new, so add additional creation metadata
        metadata.update({
            "@created_at": current_date,
            "@created_by": commit_url,
            "@created_logs": os.getenv("CI_PIPELINE_URL"),
            "@doi": doi,
            "@pmid": pmid,
            "@pmcid": pmcid
        })
        
    # Return the entry with the new fields
    return metadata

def add_metadata_to_entry(identifier: str, entry: dict, alambique:Collection):
    '''
    This function adds metadata regarding update and returns it.
        {
            "_id": "toolshed/trimal/cmd/1.4",
            "@created_at": "2023-01-01T00:00:00Z", 
            "@created_by": ObjectId("integration_20240210103607"),
            "@last_updated_at": "2023-02-01T12:00:00Z",
            "@updated_by": ObjectId("integration_20240214103607"),
            "data": {
                "id": "trimal",
                "version": "1.4",
                ...
            }
    '''
    document_w_metadata = create_metadata_url(identifier, alambique)
    document_w_metadata.update(entry)

    return document_w_metadata

def add_metadata_to_publication(doi: str, pmid: str, pmcid: str, entry: dict, collection:Collection):
    '''
    This function adds metadata regarding update and returns it.
        {
            "@doi": "10.1093/bioinformatics/bty1057",
            "@pmid": "29220451",
            "@pmcid": "PMC5764149",
            "@created_at": "2023-01-01T00:00:00Z", 
            "@created_by": ObjectId("integration_20240210103607"),
            "@last_updated_at": "2023-02-01T12:00:00Z",
            "@updated_by": ObjectId("integration_20240214103607"),
            "data": {
            }
    '''
    document_w_metadata = create_metadata_publication(doi, pmid, pmcid, collection)
    document_w_metadata.update(entry)

    return document_w_metadata



def clean_date_field(tool:dict):
    if 'about' in tool['data'].keys():
        # date objects cause trouble and are prescindable
        tool['data']['about'].pop('date', None)
    return tool

def clean_date_field(tool:dict):
    if 'about' in tool['data'].keys():
        # date objects cause trouble and are prescindable
        tool['data']['about'].pop('date', None)
    return tool


def push_entry(tool:dict, collection: Collection):
    '''Push tool to collection.

    tool: dictionary. Must have at least an '@id' key.
    collection: collection where the tool will be pushed.
    log : {'errors':[], 'n_ok':0, 'n_err':0, 'n_total':len(insts)}
    '''    
    try:
        # if the entry already exists, update it
        if collection.find_one({"_id": tool['_id']}):
            result = collection.replace_one({"_id": tool['_id']}, tool , upsert=True)
        # if the entry does not exist, insert it
        else:
            inset_new_entry(tool, collection)
        
    except Exception as e:
        logging.warning(f"error - {type(e).__name__} - {e}")

    else:
        logging.info(f"pushed_to_db_ok - {tool['_id']}")
    finally:
        return


    
def push_publication(tool:dict, collection: Collection):
    '''Push tool to collection.

    tool: dictionary. Must have at least an '@id' key.
    collection: collection where the tool will be pushed.
    log : {'errors':[], 'n_ok':0, 'n_err':0, 'n_total':len(insts)}
    '''    
    try:
        doi = tool.get('@doi')
        pmid = tool.get('@pmid')
        pmcid = tool.get('@pmcid')
        # if the entry already exists, update it
        query = build_query(doi, pmid, pmcid)
        existing_entry = collection.find(query)

        if existing_entry:
            result = collection.replace_one({"@doi": tool['@doi']}, tool , upsert=True)
            result = collection.replace_one({"@pmid": tool['@pmid']}, tool , upsert=True)
            result = collection.replace_one({"@pmcid": tool['@pmcid']}, tool , upsert=True)

        # if the entry does not exist, insert it
        else:
            inset_new_entry(tool, collection)

    except Exception as e:
        logging.warning(f"error - {type(e).__name__} - {e}")

    else:
        logging.info(f"pushed_to_db_ok - {tool['@doi']}, {tool['@pmid']}, {tool['@pmcid']}")
    finally:
        return
        
def inset_new_entry(entry: dict, collection: Collection):
    '''Inserts a new entry in the collection.

    entry: dictionary. Must have at least an '_id' key.
    collection: collection where the entry will be inserted.
    '''
    try:
        collection.insert_one(entry)
    except Exception as e:
        logging.warning(f"error - {type(e).__name__} - {e}")
    else:
        logging.info(f"inserted_to_db_ok")
    finally:
        return


def connect_db(collection_name: str):
    '''Connect to MongoDB and return the database and collection objects.

    '''
    # variables come from .env file
    mongoHost = os.getenv('HOST', default='localhost')
    mongoPort = os.getenv('PORT', default='27017')
    mongoUser = os.getenv('USER')
    mongoPass = os.getenv('PASS')
    mongoAuthSrc = os.getenv('AUTH_SRC', default='admin')
    mongoDb = os.getenv('DB', default='oeb-research-software')

    collections = {
        'alambique': os.getenv('ALAMBIQUE', default='alambique'),
        'publications': os.getenv('PUBLICATIONS', default='publications'), 
        'webMetrics': os.getenv('WEB_METRICS', default='webMetrics')
    }
    collection_name = collections.get(collection_name)

    print(f"Connecting to {collection_name} collection.")
    if collection_name is None:
        logging.error(f"Collection {collection_name} not found.")
        return None

    # Connect to MongoDB
    mongoClient = MongoClient(
        host=mongoHost,
        port=int(mongoPort),
        username=mongoUser,
        password=mongoPass,
        authSource=mongoAuthSrc,
    )
    db = mongoClient[mongoDb]
    alambique = db[collection_name]

    return alambique


def connect_db_local(collection_name: str):
    '''Connect to MongoDB and return the database and collection objects.

    '''
    # Connect to MongoDB
    mongoClient = MongoClient('localhost', 27017)
    db = mongoClient['oeb-research-software']
    alambique = db[collection_name]

    return alambique


# initializing session
session = requests.Session()
headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"}

def get_url(url, verb=False):
    '''
    Takes and url as an input and returns a json response
    '''
    try:
        re = session.get(url, headers=headers, timeout=(10, 30))
    except Exception as e:
        logging.warning(f"error with {url} - html_request")
        logging.warning(e)
        return None
        
    else:
        if re.status_code == 200:
            content_decoded = decode_json(re)
            return(content_decoded)
        else:
            logging.warning(f"error with {url} - html_request")
            return None

def decode_json(json_res):
    '''
    Decodes a json response
    '''
    try:
        content_decoded=json.loads(json_res.text)
    except Exception as e:
        logging.warning(f"error with NA - json_decode")
        logging.warning('Impossible to decode the json. Please, check URL_OPEB_METRICS')
        logging.error(e)
        return None
    else:
        return(content_decoded)
