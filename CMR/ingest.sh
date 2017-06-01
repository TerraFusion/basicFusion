# Generate token first.
curl -X POST --header "Content-Type: application/xml" -d @mytokengenerator.xml https://cmr.uat.earthdata.nasa.gov/legacy-services/rest/tokens

# Test collection validation.
curl -v -XPOST -H "Content-Type: application/echo10+xml" -H "Accept: application/xml" -H "Echo-Token: C2E53E41-0158-E741-7CA5-5AA019525DA2" -d @sample_echo10.xml https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/collection/sample_echo10

# Test granule validation.
curl -v -XPOST -H "Content-Type: application/echo10+xml" -H "Accept: application/xml" -H "Echo-Token: C2E53E41-0158-E741-7CA5-5AA019525DA2" -d @InputGranules.xml https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/granule/InputGranules
