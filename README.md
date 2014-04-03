esri-extract-data-where
=======================

Intended as geoprocessing service to generate zip file of data based on attribute query.  I've been testing on ArcGIS Server 10.1+.

## Publishing Instructions: ##
You need to first run the extract_data_where tool in ArcMap or ArcCatalog and publish the result using the share as -> geoprocessing service.  Here is a list of example parameter values:

### Layers to Clip (JSON Object): ###
*Value:* [{"layer": "EDSBDIGITIZER.DBO.SBEdit_13_14", "where": "leaid = '1200390'", "name": "custom_made_name_1"}]

### Feature Format: ###
*Value:* Shapefile - SHP - .shp

### Raster Format: ###
*Value:* ESRI GRID - GRID

### Spatial Reference: ###
*Description:* Optional parameter to convert output to different coordinate system.  Right now only WGS84 is available.
*Value:* WGS_1984

### Zip File Name: ###
*Description:* The name of the output zip file
*Value:* school_boundaries_export

### Export Data Workspace: ###
*Description:* Parent container for all data to be exported.  If you are using SDE, then this should be the **full path** to the SDE connection file.
*Value:* browse to workspace

Set the scratchWorkspace environment variable from the *Environments...* tab to the *scratch* folder next to the extract toolbox.


## Feature Format: ##
*Constants:* 'Shapefile - SHP - .shp', 'File Geodatabase - GDB - .gdb', 'Excel File - XLS - .xls'
