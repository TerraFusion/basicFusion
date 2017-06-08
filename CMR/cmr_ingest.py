import sys
from xml.etree import ElementTree
from lxml import etree
import requests
import json

def get_token(cred_file):
    url = "https://cmr.uat.earthdata.nasa.gov/legacy-services/rest/tokens"
    headers = {"Content-Type" : "application/xml"}
    res = requests.post(url, headers = headers, data = open(cred_file, 'rb'))
    tree = ElementTree.fromstring(res.content)
    token = tree.find('id')
    print('Token: ' + token.text)
    return token.text
    
def test_collection_validation(ingest_file, token):
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/collection/"
    headers = {"Content-Type" : "application/echo10+xml", "Accept" : "application/xml", "Echo-Token" : token}
    res = requests.post(url, headers = headers, data = open(ingest_file, 'rb'))
    print('collection validation response: ' + res.content)
    print('Status code: ' + str(res.status_code))
    return res.status_code   

def test_collection_ingestion(ingest_file, token):
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/collections/"
    headers = {"Content-Type" : "application/echo10+xml", "Echo-Token" : token}
    res = requests.put(url, headers = headers, data = open(ingest_file, 'rb'))
    print('collection ingestion response: ' + res.content)

#Separate function for update because native-id is needed for update
def test_collection_update(ingest_file, token):
    #Extract native-id
    search_url = "https://cmr.uat.earthdata.nasa.gov/search/collections.umm_json_v1_4"
    headers = {"Echo-Token" : token}
    tree = ElementTree.parse(ingest_file)
    ds_id = tree.find("DataSetId")
    params = {'DatasetId' : ds_id.text}
    res = requests.get(search_url, headers=headers, params=params)
    data = json.loads(res.text)
    native_id = data['items'][0]['meta']['native-id']
   
    #Send update put request
    update_url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/collections/" + native_id
    headers = {"Content-Type" : "application/echo10+xml", "Echo-Token" : token}
    res = requests.put(update_url, headers = headers, data = open(ingest_file, 'rb'))
    print('collection update response: ' + res.content)
    print('Status code: ' + str(res.status_code))

def test_granule_validation(ingest_file, token):
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/granule/"
    headers = {"Content-Type" : "application/echo10+xml", "Accept" : "application/xml", "Echo-Token" : token}
    res = requests.post(url, headers = headers, data = open(ingest_file, 'rb'))
    print('granule validation response: ' + res.content)
    print('Status code: ' + str(res.status_code))
    return res.status_code

def test_granule_ingestion(ingest_file, token):
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/granules/"
    headers = {"Content-Type" : "application/echo10+xml", "Echo-Token" : token}
    res = requests.put(url, headers = headers, data = open(ingest_file, 'rb'))
    print('granule ingestion response: ' + res.content)
    print('Status code: ' + str(res.status_code))
    return res.status_code

#---------------------------------------------------------------------------------------------------
def main() :
#---------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------
#Usage:
    #1. Use -c/-g to indicate whether you are inputting a collection or a granule
    #2. Use -u/-i to indicate whether you are ingesting the piece of metadata for the first time, or you are updating
#---------------------------------------------------------------------------------------------------

    if len(sys.argv) < 5 :
      print("usage: python cmr_ingest.py ingest_file.xml credential_file.xml doctype[-c/-g] update/ingest[-u, -i]")
      sys.exit(1)    
    ingest_file = sys.argv[1]
    cred_file = sys.argv[2]
    is_collection = True if sys.argv[3] == "-c" else False
    is_update = True if sys.argv[4] == "-u" else False        
    token = get_token(cred_file)
    if(is_collection):
        status = test_collection_validation(ingest_file, token)
        if(status == 200):
            if(is_update):
                test_collection_update(ingest_file, token)
            else:
                test_collection_ingestion(ingest_file, token)
        else:
            print("Validation for collection failed")
    else:
        status = test_granule_validation(ingest_file, token)
        if(status == 200):
            if(is_update):
                pass
            else:
                test_granule_ingestion(ingest_file, token)

#---------------------------------------------------------------------------------------------------
if __name__ == '__main__':
#---------------------------------------------------------------------------------------------------
   main()
