# Generate token first.
curl -X POST --header "Content-Type: application/xml" -d @mytokengenerator.xml https://cmr.uat.earthdata.nasa.gov/legacy-services/rest/tokens

# Test collection validation.
curl -v -XPOST -H "Content-Type: application/echo10+xml" -H "Accept: application/xml" -H "Echo-Token: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX" -d @sample_echo10.xml https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/collection/sample_echo10

# Test granule validation.
curl -v -XPOST -H "Content-Type: application/echo10+xml" -H "Accept: application/xml" -H "Echo-Token: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX" -d @InputGranules.xml https://cmr.uat.earthdata.nasa.gov/ingest/providers/FUSION/validate/granule/InputGranules

# Validate locally.
# https://raw.githubusercontent.com/nasa/Common-Metadata-Repository/8b81dbeb8ed08418cb9dcaec53cd3487a96a85da/umm-lib/resources/schema/echo10/Granule.xsd
# https://raw.githubusercontent.com/nasa/Common-Metadata-Repository/8b81dbeb8ed08418cb9dcaec53cd3487a96a85da/umm-lib/resources/schema/echo10/MetadataCommon.xsd
xmllint --schema Granule.xsd --noout InputGranules_generated.xml