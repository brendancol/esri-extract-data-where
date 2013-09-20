esri-extract-data-where
=======================

##Summary
Added query capability for data extract (ArcGIS Server 9.3 - 10.2)

Small modification on Esri Extract Data tool (Server Tools) to:
1. Remove Clip
2. Use SelectByAttribute to filter output

##Publish Service:
1. Create a map document(mxd) with layers intended for export.
2. Add ExtractDataWhere toolbox to mxd and run the model.
3. Grab result from Geoprocessing > Results and publish as geoprocessing service

Each layer from the "layers" parameter must have a corresponding where clause in the "Where Clauses" parameter.  The "Where Clauses" parameter should be a semi-colon delimited string.
