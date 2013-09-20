import arcgisscripting
import json
import os
import sys
import traceback
import zipfile
import re

gp = arcgisscripting.create(9.3)

class LicenseError(Exception):
    pass

def setUpCoordSystemEnvironment(coordinateSystem, customCoordSystemFolder):
    # get the correct spatial reference and set it into the environment
    # so that the data will get projected when clip runs
    # if it is a number, assume we have a WKID and set it directly in
    # else, find the file in the Coordinate System directory
    if coordinateSystem.lower() == "same as input" or coordinateSystem == "":
        return "same as input"

    if coordinateSystem.strip().isalnum() and customCoordSystemFolder == "":
        try:
            gp.OutputCoordinateSystem = coordinateSystem.strip()
        except:
            #Message "Coordinate System WKID %s is not valid.  Output Coordinate System will be the same as the input layer's Coordinate System"
            gp.AddWarning(get_ID_message(86131) % (coordinateSystem))
            coordinateSystem = "same as input"
            gp.OutputCoordinateSystem = None
            pass
        return coordinateSystem

    found = False
    # Search custom folder if specified
    if customCoordSystemFolder != "":
        found, coordinateSystemPath = getPRJFile(coordinateSystem, customCoordSystemFolder)

    # Search to see if we can find the spatial reference
    if not found:
        srList = gp.ListSpatialReferences("*/%s" % coordinateSystem)
        if srList:
            coordinateSystemPath = os.path.join(os.path.join(gp.getinstallinfo()["InstallDir"], "Coordinate Systems"), srList[0]) + ".prj"
            found = True

    if found:
        gp.OutputCoordinateSystem = coordinateSystemPath
        return coordinateSystemPath
    else:
        #Message "Couldn't find the specified projection file %s.  Output Coordinate System will be the same as the input layer's Coordinate System."
        gp.AddWarning(get_ID_message(86132) % coordinateSystem)
        return "same as input"

def getPRJFile(inputCoordSysString, prjDir):
    inputCoordSysString += ".prj"
    # walk through the dirs to find the prj file
    if os.path.exists(prjDir):
        for x in os.walk(prjDir):
            if inputCoordSysString in x[2]:
                return True, os.path.join(x[0], inputCoordSysString)
    else:
        return False, ""

    # if we got to here then it didn't find the prj file
    return False, ""

def zipUpFolder(folder, outZipFile):
    # zip the data
    try:
        zip = zipfile.ZipFile(outZipFile, 'w', zipfile.ZIP_DEFLATED)
        zipws(str(folder), zip, "CONTENTS_ONLY")
        zip.close()
    except RuntimeError:
        # Delete zip file if exists
        if os.path.exists(outZipFile):
            os.unlink(outZipFile)
        zip = zipfile.ZipFile(outZipFile, 'w', zipfile.ZIP_STORED)
        zipws(str(folder), zip, "CONTENTS_ONLY")
        zip.close()
        #Message"  Unable to compress zip file contents."
        gp.AddWarning(get_ID_message(86133))

def zipws(path, zip, keep):
    path = os.path.normpath(path)
    # os.walk visits every subdirectory, returning a 3-tuple
    #  of directory name, subdirectories in it, and filenames
    #  in it.
    for (dirpath, dirnames, filenames) in os.walk(path):
        # Iterate over every filename
        for file in filenames:
            # Ignore .lock files
            if not file.endswith('.lock'):
                #gp.AddMessage("Adding %s..." % os.path.join(path, dirpath, file))
                try:
                    if keep:
                        zip.write(os.path.join(dirpath, file),
                        os.path.join(os.path.basename(path), os.path.join(dirpath, file)[len(path)+len(os.sep):]))
                    else:
                        zip.write(os.path.join(dirpath, file),
                        os.path.join(dirpath[len(path):], file))

                except Exception as e:
                    #Message "    Error adding %s: %s"
                    gp.AddWarning(get_ID_message(86134) % (file, e[0]))
    return None

def createFolderInScratch(folderName):
    # create the folders necessary for the job
    folderPath = gp.CreateUniqueName(folderName, gp.scratchworkspace)
    gp.CreateFolder_management(gp.scratchworkspace, os.path.basename(folderPath))
    return folderPath

def getTempLocationPath(folderPath, format):
    # make sure there is a location to write to for gdb and mdb
    if format == "mdb":
        MDBPath = os.path.join(folderPath, "data.mdb")
        if not gp.exists(MDBPath):
            gp.CreatePersonalGDB_management(folderPath, "data")
        return MDBPath
    elif format == "gdb":
        GDBPath = os.path.join(folderPath, "data.gdb")
        if not gp.exists(GDBPath):
            gp.CreateFileGDB_management(folderPath, "data")
        return GDBPath
    else:
        return folderPath

def makeOutputPath(raster, inLayerName, convert, formatList, zipFolderPath, scratchFolderPath):
    outFormat = formatList[1].lower()

    # if we are going to convert to an esri format on the clip, put the output in the zipfolder
    # else put it in the scratch folder in a gdb
    if convert:
        outwkspc = getTempLocationPath(zipFolderPath, outFormat)
    else:
        outwkspc = getTempLocationPath(scratchFolderPath, "gdb")

    if inLayerName.find("\\"):
        inLayerName = inLayerName.split("\\")[-1]

    # make sure there are no spaces in the out raster name and make sure its less than 13 chars
    if outFormat == "grid":
        if len(inLayerName) > 12:
            inLayerName = inLayerName[:12]
        if inLayerName.find(" ") > -1:
            inLayerName = inLayerName.replace(" ", "_")

    # make the output path
    tmpName = os.path.basename(gp.createuniquename(inLayerName, outwkspc))
    tmpName = gp.validatetablename(tmpName, outwkspc)

    # do some extension housekeeping.
    # Raster formats and shp always need to put the extension at the end
    if raster or outFormat == "shp":
        if outFormat != "gdb" and outFormat != "mdb" and outFormat != "grid":
            tmpName = tmpName + formatList[2].lower()

    outputpath = os.path.join(outwkspc, tmpName)

    return tmpName, outputpath

def clipRaster(lyr, rasterFormat, zipFolderPath, scratchFolderPath):
    # get the path and a validated name for the output
    layerName, outputpath = makeOutputPath(True, lyr, True, rasterFormat, zipFolderPath, scratchFolderPath)
    # do the clip
    try:
        gp.CopyRaster_management(lyr, outputpath)
        #Message "  clipped %s..."
        gp.AddIDMessage("INFORMATIVE", 86135, lyr)
    except:
        errmsg = gp.getmessages(2)
        #Message "  failed to clip layer %s..."
        gp.AddWarning(get_ID_message(86136) % lyr)
        if errmsg.lower().find("error 000446") > -1:
        #Message"  Output file format with specified pixel type or number of bands or colormap is not supported.\n  Refer to the 'Technical specifications for raster dataset formats' help section in Desktop Help.\n  http://webhelp.esri.com/arcgisdesktop/9.3/index.cfm?TopicName=Technical_specifications_for_raster_dataset_formats"
        #Shorted as "Output file format with specified pixel type or number of bands or colormap is not supported"
            gp.AddWarning(get_ID_message(86137))

        elif errmsg.lower().find("error 000445"):
            #Message "  Extension is invalid for the output raster format.  Please verify that the format you have specified is valid."
            gp.AddWarning(get_ID_message(86138))
        else:
            gp.AddWarning(gp.GetMessages(2))
        pass

def clipFeatures(lyr, featureFormat, zipFolderPath, scratchFolderPath, convertFeaturesDuringClip, where=None, name='output_features'):
    global haveDataInterop
    cleanUpFeatureLayer = False

    # get the path and a validated name for the output
    layerName, outputpath = makeOutputPath(False, lyr, convertFeaturesDuringClip, featureFormat, zipFolderPath, scratchFolderPath)

    if where:
        feature_layer = layerName
        gp.MakeFeatureLayer_management(lyr, feature_layer)
        cleanUpFeatureLayer = True
        gp.SelectLayerByAttribute_management(feature_layer, "NEW_SELECTION", where)

        count = int(arcpy.GetCount_management(feature_layer).getOutput(0))
        if count == 0:
            gp.AddWarning("Where clause yielded no records ::  Layer=%s; Clause=%s" % (feature_layer, where))
            return
    else:
        feature_layer = lyr

    try:

        # do the clip
        gp.CopyFeatures(feature_layer, outputpath)
        #Message "  clipped %s..."
        gp.AddIDMessage("INFORMATIVE", 86135, feature_layer)

        # if format needs data interop, convert with data interop
        if not convertFeaturesDuringClip:
            # get path to zip
            outputinzip = os.path.join(zipFolderPath, layerName + featureFormat[2])
            if featureFormat[2].lower() in [".dxf", ".dwg", ".dgn"]:
                #Message "..using export to cad.."
                gp.AddWarning(get_ID_message(86139))
                gp.ExportCAD_conversion(outputpath, featureFormat[1], outputinzip)
            else:
                if not haveDataInterop:
                    raise LicenseError
                    
                diFormatString = "%s,%s" % (featureFormat[1], outputinzip)
                # run quick export
                gp.quickexport_interop(outputpath, diFormatString)

    except LicenseError:
        #Message "  failed to export to %s.  The requested formats require the Data Interoperability extension.  This extension is currently unavailable."
        gp.AddWarning(get_ID_message(86140) % featureFormat[1])
        pass

    except:
        errorstring = gp.GetMessages(2)
        if errorstring.lower().find("failed to execute (quickexport)") > -1:
            #Message "  failed to export layer %s with Quick Export.  Please verify that the format you have specified is valid."
            gp.AddWarning(get_ID_message(86141) % feature_layer)

        elif errorstring.lower().find("failed to execute (clip)") > -1:
            #Message "  failed to clip layer %s...
            gp.AddWarning(get_ID_message(86142) % feature_layer)
        else:
            gp.AddWarning(get_ID_message(86142) % feature_layer)
            gp.AddWarning(gp.GetMessages(2))
        pass

    finally:
        if cleanUpFeatureLayer:
            arcpy.Delete_management(feature_layer)


def clipAndConvert(layersToWhereClause, featureFormat, rasterFormat, coordinateSystem):
    try:

        # for certain output formats we don't need to use Data Interop to do the conversion
        convertFeaturesDuringClip = False
        if featureFormat[1].lower() in ["gdb", "mdb", "shp"]:
            convertFeaturesDuringClip = True

        # get a scratch folder for temp data and a zip folder to hold
        # the final data we want to zip and send
        zipFolderPath = createFolderInScratch("zipfolder")
        scratchFolderPath = createFolderInScratch("scratchfolder")

        # loop through the list of layers recieved
        for lyr, where in layersToWhereClause.items():
            # temporary stop gap measure to counteract bug
            if lyr.find(" ") > -1:
                lyr = lyr.replace("'", "")

            describe = gp.describe(lyr)
            dataType = describe.DataType.lower()

            # make sure we are dealing with features or raster and not some other layer type (group, tin, etc)
            if dataType in ["featurelayer", "rasterlayer"]:
                # if the coordinate system is the same as the input
                # set the environment to the coord sys of the layer being clipped
                # may not be necessary, but is a failsafe.
                if coordinateSystem.lower() == "same as input":
                    sr = describe.spatialreference
                    if sr != None:
                        gp.outputcoordinatesystem = sr

                # raster branch
                if dataType == "rasterlayer":
                    clipRaster(lyr, rasterFormat, zipFolderPath, scratchFolderPath)

                # feature branch
                else:
                    clipFeatures(lyr, featureFormat, zipFolderPath, scratchFolderPath, convertFeaturesDuringClip, where=where)
            else:
                #Message "  Cannot clip layer: %s.  This tool does not clip layers of type: %s..."
                gp.AddWarning(get_ID_message(86143) % (lyr, dataType))
        return zipFolderPath

    except:
        errstring = get_ID_message(86144)#"Failure in clipAndConvert..\n"
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
                str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"
        errstring += pymsg
        raise Exception, errstring

def get_ID_message(ID):
    return re.sub("%1|%2", "%s", gp.GetIDMessage(ID))

if __name__ == '__main__':
    try:
        layers = gp.getparameterastext(0).split(";")
        inputFeatureFormat = gp.getparameterastext(1)
        inputRasterFormat = gp.getparameterastext(2)
        coordinateSystem = gp.getparameterastext(3)
        customCoordSystemFolder = gp.getparameterastext(4)
        outputZipFile = gp.getparameterastext(5).replace("\\",os.sep)

        # input where clauses
        where_clauses_text = gp.getparameterastext(6)

        # use '*' or '1=1' to get all features from all layers
        if not where_clauses_text or where_clauses_text in ['1=1', '*']:
            where_clauses = [None] * len(layers)

        # use 'all:' keyword to apply where clause to all layers in service
        elif 'all:' in where_clauses_text:
            where = where_clauses_text.split(':')[1]
            where_clauses = [where] * len(layers)

        # supply one where clause for each layer in corresponding order
        else:
            where_clauses = filter(None, where_clauses.split(";"))

        if len(layers) != len(where_clauses):
            msg = 'You must supply the same number of layers as where clauses or "*"'
            gp.AddWarning(msg)
            raise ValueError('You must supply the same number of layers as where clauses or "*"')

        layers_to_where_clause = dict(zip(layers, where_clauses))

        if gp.CheckExtension("DataInteroperability") == "Available":
            gp.CheckOutExtension("DataInteroperability")
            haveDataInterop = True
        else:
            haveDataInterop = False
        # Do a little internal validation.
        # Expecting "long name - short name - extension
        # If no format is specified, send features to GDB.
        if inputFeatureFormat == "":
            featureFormat = ["File Geodatabase", "GDB", ".gdb"]
        else:
            #featureFormat = inputFeatureFormat.split(" - ")
            featureFormat = map(lambda x: x.strip(), inputFeatureFormat.split("-"))
            if len(featureFormat) < 3:
                featureFormat.append("")

        # If no format is specified, send rasters to GRID.
        # Expecting "long name - short name - extension
        if inputRasterFormat == "":
            rasterFormat = ["ESRI GRID", "GRID", ""]
        else:
            #rasterFormat = inputRasterFormat.split(" - ")
            rasterFormat = map(lambda x: x.strip(), inputRasterFormat.split("-"))
            if len(rasterFormat) < 3:
                rasterFormat.append("")

        coordinateSystem = setUpCoordSystemEnvironment(coordinateSystem, customCoordSystemFolder)

        # Do this so the tool works even when the scratch isn't set or if it is set to gdb/mdb/sde
        if gp.scratchworkspace is None or os.path.exists(str(gp.scratchworkspace)) is False:
            gp.scratchworkspace = gp.getsystemenvironment("TEMP")
        else:
            swd = gp.describe(gp.scratchworkspace)
            wsid = swd.workspacefactoryprogid
            if wsid == 'esriDataSourcesGDB.FileGDBWorkspaceFactory.1' or\
               wsid == 'esriDataSourcesGDB.AccessWorkspaceFactory.1' or\
               wsid == 'esriDataSourcesGDB.SdeWorkspaceFactory.1':
                gp.scratchworkspace = gp.getsystemenvironment("TEMP")

        # clip and convert the layers and get the path to the folder we want to zip
        zipFolder = clipAndConvert(layers_to_where_clause, featureFormat, rasterFormat, coordinateSystem)

        # zip the folder
        zipUpFolder(zipFolder, outputZipFile)

    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
                str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"
        gp.AddError(pymsg)