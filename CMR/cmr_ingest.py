import sys
from xml.etree import ElementTree
import requests

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
    print('collection response content: ' + res.content)

def test_granule_validation(ingest_file, token):
    url = "https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/granule/"
    headers = {"Content-Type" : "application/echo10+xml", "Accept" : "application/xml", "Echo-Token" : token}
    res = requests.post(url, headers = headers, data = open(ingest_file, 'rb'))
    print('granule response content: ' + res.content)

#---------------------------------------------------------------------------------------------------
def main() :
#---------------------------------------------------------------------------------------------------
    if len(sys.argv) < 4 :
      print("usage: python cmr_ingest.py ingest_file.xml credential_file.xml doctype[-c/-g]")
      sys.exit(1)
    ingest_file = sys.argv[1]
    cred_file = sys.argv[2]
    is_collection = True if sys.argv[3] == "-c" else False
    token = get_token(cred_file)
    if(is_collection):
        test_collection_validation(ingest_file, token)
    else:
        test_granule_validation(ingest_file, token)

#---------------------------------------------------------------------------------------------------
if __name__ == '__main__':
#---------------------------------------------------------------------------------------------------
   main()
