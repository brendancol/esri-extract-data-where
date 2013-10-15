import arcpy
import json
import os
import sys
import traceback
import zipfile
import re
import urllib
import urlparse
import unittest

class LicenseError(Exception):
	pass

#Total hack...remove after
SERVER_VIRTUAL_DIRECTORIES = r'Server virtual directory not set review top of export script'
SCRATCH_FOLDER = os.path.join(os.path.dirname(__file__), 'scratch')
PROJECTIONS_DIRECTORY = os.path.join(os.path.dirname(__file__), 'projections')
TEST_DATA_FOLDER = os.path.join(os.path.dirname(__file__), 'test_data')
TEST_DATA_GDB = os.path.join(TEST_DATA_FOLDER, 'test_data.gdb')
TEST_DATA_SHP = os.path.join(TEST_DATA_FOLDER, 'test_data_shp')

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
		arcpy.AddWarning("Unable to compress zip file contents.")

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
				#arcpy.AddMessage("Adding %s..." % os.path.join(path, dirpath, file))
				try:
					if keep:
						zip.write(os.path.join(dirpath, file),
						os.path.join(os.path.basename(path), os.path.join(dirpath, file)[len(path)+len(os.sep):]))
					else:
						zip.write(os.path.join(dirpath, file),
						os.path.join(dirpath[len(path):], file))

				except Exception as e:
					#Message "    Error adding %s: %s"
					arcpy.AddWarning(get_ID_message(86134) % (file, e[0]))
	return None

def createFolderInScratch(folderName):
	# create the folders necessary for the job
	folderPath = arcpy.CreateUniqueName(folderName, arcpy.env.scratchWorkspace)
	arcpy.CreateFolder_management(arcpy.env.scratchWorkspace, os.path.basename(folderPath))
	return folderPath

def getTempLocationPath(folderPath, format, ):
	# make sure there is a location to write to for gdb and mdb
	if format == "mdb":
		MDBPath = os.path.join(folderPath, "data.mdb")
		if not arcpy.Exists(MDBPath):
			arcpy.CreatePersonalGDB_management(folderPath, "data")
		return MDBPath
	elif format == "gdb":
		GDBPath = os.path.join(folderPath, "data.gdb")
		if not arcpy.Exists(GDBPath):
			arcpy.CreateFileGDB_management(folderPath, "data")
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
	tmpName = os.path.basename(arcpy.CreateUniqueName(inLayerName, outwkspc))
	tmpName = arcpy.ValidateTableName(tmpName, outwkspc)

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
		arcpy.CopyRaster_management(lyr, outputpath)
		#Message "  clipped %s..."
		arcpy.AddIDMessage("INFORMATIVE", 86135, lyr)
	except:
		errmsg = arcpy.getmessages(2)
		#Message "  failed to clip layer %s..."
		arcpy.AddWarning(get_ID_message(86136) % lyr)
		if errmsg.lower().find("error 000446") > -1:
		#Message"  Output file format with specified pixel type or number of bands or colormap is not supported.\n  Refer to the 'Technical specifications for raster dataset formats' help section in Desktop Help.\n  http://webhelp.esri.com/arcgisdesktop/9.3/index.cfm?TopicName=Technical_specifications_for_raster_dataset_formats"
		#Shorted as "Output file format with specified pixel type or number of bands or colormap is not supported"
			arcpy.AddWarning(get_ID_message(86137))

		elif errmsg.lower().find("error 000445"):
			#Message "  Extension is invalid for the output raster format.  Please verify that the format you have specified is valid."
			arcpy.AddWarning(get_ID_message(86138))
		else:
			arcpy.AddWarning(arcpy.GetMessages(2))
		pass

def clipFeatures(lyr, featureFormat, zipFolderPath, scratchFolderPath, convertFeaturesDuringClip, where=None, name='output_features', projection=None):
	global haveDataInterop
	cleanUpFeatureLayer = False
	# get the path and a validated name for the output
	layerName, outputpath = makeOutputPath(False, lyr, convertFeaturesDuringClip, featureFormat, zipFolderPath, scratchFolderPath)
	arcpy.AddMessage("Starting layer: %s where: %s" % (lyr, where))
	feature_layer = layerName
	
	cleanUpFeatureLayer = True

	try:
		arcpy.MakeFeatureLayer_management(lyr, feature_layer)
		arcpy.SelectLayerByAttribute_management(feature_layer, "NEW_SELECTION", where)
		count = int(arcpy.GetCount_management(feature_layer).getOutput(0))
		
	except:
		arcpy.AddWarning("Select Attributes Error ::  Layer=%s; Clause=%s" % (feature_layer, where))
		arcpy.AddWarning(arcpy.GetMessages(2))
		return

	if count == 0:
		arcpy.AddWarning("Where clause yielded no records ::  Layer=%s; Clause=%s" % (feature_layer, where))
		return

	try:

		if projection:
			arcpy.AddMessage('Ready to project: feature_layer=%s; outputpath=%s' % (feature_layer, outputpath))
			out_coordinate_system = os.path.join(PROJECTIONS_DIRECTORY, projection + '.prj')
			arcpy.Project_management(feature_layer, outputpath, out_coordinate_system)
			arcpy.AddMessage('Project Complete')
		else:
			arcpy.AddMessage('Ready to copy: feature_layer=%s; outputpath=%s' % (feature_layer, outputpath))
			arcpy.CopyFeatures_management(feature_layer, outputpath)
			arcpy.AddMessage('Copy Complete')

		# if format needs data interop, convert with data interop
		if not convertFeaturesDuringClip:
			# get path to zip
			outputinzip = os.path.join(zipFolderPath, layerName + featureFormat[2])
			if featureFormat[2].lower() in [".dxf", ".dwg", ".dgn"]:
				#Message "..using export to cad.."
				arcpy.AddWarning(get_ID_message(86139))
				arcpy.ExportCAD_conversion(outputpath, featureFormat[1], outputinzip)
			else:
				if not haveDataInterop:
					raise LicenseError
					
				diFormatString = "%s,%s" % (featureFormat[1], outputinzip)
				# run quick export
				arcpy.quickexport_interop(outputpath, diFormatString)

	except LicenseError:
		#Message "  failed to export to %s.  The requested formats require the Data Interoperability extension.  This extension is currently unavailable."
		arcpy.AddWarning(get_ID_message(86140) % featureFormat[1])
		pass

	except:
		errorstring = arcpy.GetMessages(2)
		arcpy.AddWarning(arcpy.GetMessages(0) + arcpy.GetMessages(1) + errorstring  + ' ERROR :: ' + feature_layer)

	finally:
		if cleanUpFeatureLayer and arcpy.Exists(feature_layer):
			arcpy.Delete_management(feature_layer)


def clipAndConvert(layersToWhereClause, featureFormat, rasterFormat, projection=None):
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

			describe = arcpy.Describe(lyr)
			dataType = describe.DataType.lower()

			# make sure we are dealing with features or raster and not some other layer type (group, tin, etc)
			if dataType in ["featurelayer", "rasterlayer", "featureclass"]:
				# if the coordinate system is the same as the input
				# set the environment to the coord sys of the layer being clipped
				# may not be necessary, but is a failsafe.

				# raster branch
				if dataType == "rasterlayer":
					clipRaster(lyr, rasterFormat, zipFolderPath, scratchFolderPath)

				# feature branch
				else:
					clipFeatures(lyr, featureFormat, zipFolderPath, scratchFolderPath, convertFeaturesDuringClip, where=where, projection=projection)
			else:
				#Message "  Cannot clip layer: %s.  This tool does not clip layers of type: %s..."
				arcpy.AddWarning(get_ID_message(86143) % (lyr, dataType))
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
	return re.sub("%1|%2", "%s", arcpy.GetIDMessage(ID))

def get_results_virtual_path(resultsFilePath):
	file_url = urlparse.urljoin('file:', urllib.pathname2url(resultsFilePath))
	if 'directories' in file_url:
		return SERVER_VIRTUAL_DIRECTORIES + file_url.split(r'directories')[1]
	else:
		return file_url

def run_export(params):
	try:

		layers = params.get_full_layer_pathes()
		params.result_file = os.path.join(arcpy.env.scratchWorkspace, params.zipfile_name)
		params.virtual_result_file = get_results_virtual_path(params.result_file)

		if len(layers) != len(params.where_clauses):
			msg = 'You must supply the same number of layers as where clauses or "*"'
			arcpy.AddWarning(msg)
			raise ValueError('You must supply the same number of layers as where clauses or "*"')

		layers_to_where_clause = dict(zip(layers, params.where_clauses))

		if arcpy.CheckExtension("DataInteroperability") == "Available":
			arcpy.CheckOutExtension("DataInteroperability")
			haveDataInterop = True
		else:
			haveDataInterop = False
		# Do a little internal validation.
		# Expecting "long name - short name - extension
		# If no format is specified, send features to GDB.
		if not params.input_feature_format:
			featureFormat = ["File Geodatabase", "GDB", ".gdb"]
		else:
			#featureFormat = inputFeatureFormat.split(" - ")
			featureFormat = map(lambda x: x.strip(), params.input_feature_format.split("-"))
			if len(featureFormat) < 3:
				featureFormat.append("")

		# If no format is specified, send rasters to GRID.
		# Expecting "long name - short name - extension
		if not params.input_raster_format:
			rasterFormat = ["ESRI GRID", "GRID", ""]
		else:
			#rasterFormat = inputRasterFormat.split(" - ")
			rasterFormat = map(lambda x: x.strip(), params.input_raster_format.split("-"))
			if len(rasterFormat) < 3:
				rasterFormat.append("")

		# Do this so the tool works even when the scratch isn't set or if it is set to gdb/mdb/sde
		if arcpy.env.scratchWorkspace is None or os.path.exists(str(arcpy.env.scratchWorkspace)) is False:
			raise "Scratch workspace is None or doesn't exists"
		else:
			swd = arcpy.Describe(arcpy.env.scratchWorkspace)
			wsid = swd.workspacefactoryprogid
			if wsid == 'esriDataSourcesGDB.FileGDBWorkspaceFactory.1' or\
			   wsid == 'esriDataSourcesGDB.AccessWorkspaceFactory.1' or\
			   wsid == 'esriDataSourcesGDB.SdeWorkspaceFactory.1':
				arcpy.env.scratchWorkspace = arcpy.getsystemenvironment("TEMP")
				
		arcpy.AddMessage("Scratch Workspace: %s" % arcpy.env.scratchWorkspace)

		# clip and convert the layers and get the path to the folder we want to zip
		zipFolder = clipAndConvert(layers_to_where_clause, featureFormat, rasterFormat, projection=params.output_projection)

		# zip the folder
		zipUpFolder(zipFolder, params.result_file)

	except:
		tb = sys.exc_info()[2]
		tbinfo = traceback.format_tb(tb)[0]
		pymsg = "ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
				str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"
		arcpy.AddError(pymsg)


class ToolParameters(object):
	'''value object for storing export tool parameters'''

	def __init__(self):
		self.layers = None
		self.input_feature_format = None
		self.input_raster_format = None
		self.output_projection = None
		self.where_clauses = None
		self.zipfile_name = None
		self.export_source_directory = None
		self.zipfile_path = None

		self.valid_projections = ['WGS_1984']

	def from_arc(self):
		self.layers = arcpy.GetParameterAsText(0).split(";")
		self.input_feature_format = arcpy.GetParameterAsText(1)
		self.input_raster_format = arcpy.GetParameterAsText(2)
		self.output_projection = arcpy.GetParameterAsText(3)
		self.zipfile_name = arcpy.GetParameterAsText(4)

		if not self.zipfile_name.endswith('.zip'):
			self.zipfile_name += '.zip'

		# input where clauses
		where_clauses_text = arcpy.GetParameterAsText(6)
		arcpy.AddMessage("Workspace: %s" % arcpy.workspace)
		arcpy.SetParameterAsText(7, output_zip)
		export_source_directory = arcpy.GetParameterAsText(8)

		# use '*' or '1=1' to get all features from all layers
		if not where_clauses_text or where_clauses_text in ['1=1', '*']:
			self.where_clauses = [None] * len(self.layers)

		# use 'all:' keyword to apply where clause to all layers in service
		elif 'all:' in where_clauses_text:
			where = where_clauses_text.split(':')[1]
			self.where_clauses = [where] * len(self.layers)

		# supply one where clause for each layer in corresponding order
		else:
			self.where_clauses = filter(None, where_clauses_text.split(";"))

		run_export(params)

	def get_full_layer_pathes(self):
		return [os.path.join(self.export_source_directory, l) for l in self.layers]

class ExtractDataWhereTests(unittest.TestCase):
	def setUp(self):
		arcpy.env.scratchWorkspace = SCRATCH_FOLDER

		self.params = ToolParameters()
		self.params.layers = ['export_test_1', 'export_test_2']
		self.params.input_feature_format = 'File Geodatabase - GDB - .gdb'
		self.params.input_raster_format = None
		self.params.output_projection = None
		self.params.where_clauses = ["OBJECTID < 5","OBJECTID >= 5 AND OBJECTID < 10"]
		self.params.zipfile_name = 'test_export_where'
		self.params.export_source_directory = TEST_DATA_GDB
		self.params.result_file = None

	def tearDown(self):
		pass

	def test_export_shapefile(self):
		self.params.input_feature_format = 'Shapefile - SHP - .shp'
		self.params.zipfile_name = 'test_export_shapefile'
		run_export(self.params)
		self.assertTrue(os.path.exists(self.params.result_file))

	def test_export_file_geodatabase(self):
		self.params.input_feature_format = 'File Geodatabase - GDB - .gdb'
		self.params.zipfile_name = 'test_export_file_geodatabase'
		run_export(self.params)
		self.assertTrue(os.path.exists(self.params.result_file))

	def test_export_wgs84_shapefile(self):
		self.params.input_feature_format = 'Shapefile - SHP - .shp'
		self.params.output_projection = 'WGS_1984'
		self.params.zipfile_name = 'test_export_wgs84_shapefile'
		run_export(self.params)

		self.assertTrue(os.path.exists(self.params.result_file))

	def test_export_wgs84_file_geodatabase(self):
		self.params.input_feature_format = 'File Geodatabase - GDB - .gdb'
		self.params.output_projection = 'WGS_1984'
		self.params.zipfile_name = 'test_export_wgs84_file_geodatabase'
		run_export(self.params)

		self.assertTrue(os.path.exists(self.params.result_file))

if __name__ == '__main__':
	if arcpy.GetParameterAsText(0):
		params = ToolParameters()
		params.from_arc()
		run_export(params)
	else:
		unittest.main()