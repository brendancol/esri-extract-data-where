esri-extract-data-where
=======================

Added query capability for data extract.  I've been testing on ArcGIS Server 10.1, but this should have no problem on 9.3 - 10.2.

###Small modification on Esri Extract Data tool (Server Tools) to:
    * Removed Clip
    * Used SelectByAttribute to filter output
    * Added input parameter for semi-colon delimited string of where clauses which correspond to input layer names.

###Setup:
    1. Create a map document(mxd) with layers intended for export.
    2. Add ExtractDataWhere toolbox to mxd and run the model.
    3. Grab result from Geoprocessing > Results and publish as geoprocessing service

###Usage (In Progress):
Request must contains the following parameters:
    * Layers in TODO: add example
    * Output Format
    * Where Clauses: Each layer from the "layers" parameter must have a corresponding where clause in the "Where Clauses" parameter.  The "Where Clauses" parameter should be a semi-colon delimited string.
