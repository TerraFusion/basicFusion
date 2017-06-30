import sys
import requests
import json
from xml.etree import ElementTree

#Get native id from short name, needed for granule ingestion
def sn_native_helper(ingest_file, token):
    """
    A helper to get native id for granule ingestion from the CMR server
    
    :param ingest_file: file path of the file to be ingested
    :param token: token for CMR API calls
    :returns native_id: native id needed for granule ingestion
    """
    search_url = "https://cmr.uat.earthdata.nasa.gov/search/collections.umm_json_v1_4"
    headers = {"Echo-Token" : token}
    tree = ElementTree.parse(ingest_file)
    sn = tree.find("Collection/ShortName")
    params = {'short_name' : sn.text}
    res = requests.get(search_url, headers=headers, params=params)
    data = json.loads(res.text)
    native_id = data['items'][0]['meta']['native-id']
    return native_id

def get_token(cred_file):
    """
    Get API token from CMR server, using UAT server
    
    :param cred_file: file path of the credentials xml file
    :returns token.text: the token needed for subsequent API calls
    """
    url = "https://cmr.uat.earthdata.nasa.gov/legacy-services/rest/tokens"
    headers = {"Content-Type" : "application/xml"}
    res = requests.post(url, headers = headers, data = open(cred_file, 'rb'))
    tree = ElementTree.fromstring(res.content)
    token = tree.find('id')
    return token.text
    
def test_collection_validation(ingest_file, token):
    """
    Validates collection xml file for the ECHO10 specification by submitting the file to the CMR server
    
    :param ingest_file: file path of the file to be ingested
    :param token: token for CMR API calls
    :returns res.status_code: status code to indicate the result of the validation process
    """
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/collection/"
    headers = {"Content-Type" : "application/echo10+xml", "Accept" : "application/xml", "Echo-Token" : token}
    res = requests.post(url, headers = headers, data = open(ingest_file, 'rb'))
    print('collection validation response: ' + res.content)
    print('Status code: ' + str(res.status_code))
    return res.status_code   

def test_collection_ingestion(ingest_file, token):
    """
    Ingests collection xml file to the CMR server
    
    :param ingest_file: file path of the file to be ingested
    :param token: token for CMR API calls
    """
    tree = ElementTree.parse(ingest_file)
    dsi = tree.find("DataSetId")
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/collections/"
    url += dsi.text
    headers = {"Content-Type" : "application/echo10+xml", "Echo-Token" : token}
    res = requests.put(url, headers = headers, data = open(ingest_file, 'rb'))
    print('collection ingestion response: ' + res.content)
    print('Status code: ' + str(res.status_code))

#Separate function for update because native-id is needed for update
def test_collection_update(ingest_file, token):
    """
    Updates collection metadata that already exists in the CMR server, native_id is extracted by an additional API call
    
    :param ingest_file: file path of the file to be ingested
    :param token: token for CMR API calls
    """
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
    """
    Validates granule xml file for the ECHO10 specification by submitting the file to the CMR server
    
    :param ingest_file: file path of the file to be ingested
    :param token: token for CMR API calls
    :returns res.status_code: status code to indicate the result of the validation process
    """
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/granule/"
    headers = {"Content-Type" : "application/echo10+xml", "Accept" : "application/xml", "Echo-Token" : token}
    res = requests.post(url, headers = headers, data = open(ingest_file, 'rb'))
    print('granule validation response: ' + res.content)
    print('Status code: ' + str(res.status_code))
    return res.status_code

def test_granule_ingestion(ingest_file, token):
    """
    Ingests granule xml file to the CMR server, there's no need for an extra function for updating, since native_id 
    of its parent is always needed for ingestion
    
    :param ingest_file: file path of the file to be ingested
    :param token: token for CMR API calls
    """
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/granules/"
    url += sn_native_helper(ingest_file, token)
    headers = {"Content-Type" : "application/echo10+xml", "Echo-Token" : token}
    res = requests.put(url, headers = headers, data = open(ingest_file, 'rb'))
    print('granule ingestion response: ' + res.content)
    print('Status code: ' + str(res.status_code))
    return res.status_code

#---------------------------------------------------------------------------------------------------
def main() :
#---------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------
    """
    Validates and ingest metadata file into the CMR file
    
    Usage:
        #1. Use -c/-g to indicate whether you are inputting a collection or a granule
        #2. Use -u/-i to indicate whether you are ingesting the piece of metadata for the first time, or you are updating
    """
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
            test_granule_ingestion(ingest_file, token)

#---------------------------------------------------------------------------------------------------
if __name__ == '__main__':
#---------------------------------------------------------------------------------------------------
   main()
