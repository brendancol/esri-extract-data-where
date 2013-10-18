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

from extract_data_config import SCRATCH_FOLDER, TEST_DATA_GDB, TEST_DATA_SHP, PROJECTIONS_FOLDER, VALID_PROJECTION_ALIASES

class LicenseError(Exception):
	pass

def create_zipfile(folder, outZipFile):
	# zip the data
	try:
		if not outZipFile.endswith('.zip'):
			outZipFile += '.zip'

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

def create_folder_in_scratch(folderName):
	# create the folders necessary for the job
	folderPath = arcpy.CreateUniqueName(folderName, arcpy.env.scratchWorkspace)
	arcpy.CreateFolder_management(arcpy.env.scratchWorkspace, os.path.basename(folderPath))
	return folderPath

def get_temp_location_path(folderPath, format, outputDataFolderName='data'):
	# make sure there is a location to write to for gdb and mdb
	if format == "mdb":
		MDBPath = os.path.join(folderPath, outputDataFolderName + ".mdb")
		if not arcpy.Exists(MDBPath):
			arcpy.CreatePersonalGDB_management(folderPath, outputDataFolderName)
		return MDBPath
	elif format == "gdb":
		GDBPath = os.path.join(folderPath, outputDataFolderName + ".gdb")
		if not arcpy.Exists(GDBPath):
			arcpy.CreateFileGDB_management(folderPath, outputDataFolderName)
		return GDBPath
	else:
		return folderPath

def make_output_path(raster, inLayerName, outLayerName, convert, formatList, zipFolderPath, scratchFolderPath, outputDataFolderName='data'):
	outFormat = formatList[1].lower()

	if convert:
		outwkspc = get_temp_location_path(zipFolderPath, outFormat, outputDataFolderName=outputDataFolderName)
	else:
		outwkspc = get_temp_location_path(scratchFolderPath, "gdb", outputDataFolderName=outputDataFolderName)

	if inLayerName.find("\\"):
		inLayerName = inLayerName.split("\\")[-1]

	# make sure there are no spaces in the out raster name and make sure its less than 13 chars
	if outFormat == "grid":
		if len(inLayerName) > 12:
			inLayerName = inLayerName[:12]
		if inLayerName.find(" ") > -1:
			inLayerName = inLayerName.replace(" ", "_")

	# make the output path
	tmpName = os.path.basename(arcpy.CreateUniqueName(outLayerName, outwkspc))
	tmpName = arcpy.ValidateTableName(tmpName, outwkspc)

	# do some extension housekeeping.
	# Raster formats and shp always need to put the extension at the end
	if raster or outFormat == "shp":
		if outFormat != "gdb" and outFormat != "mdb" and outFormat != "grid":
			tmpName += formatList[2].lower()

	outputpath = os.path.join(outwkspc, tmpName)

	return tmpName, outputpath

def clipRaster(lyr, rasterFormat, zipFolderPath, scratchFolderPath):
	# get the path and a validated name for the output
	layerName, outputpath = make_output_path(True, lyr, True, rasterFormat, zipFolderPath, scratchFolderPath)
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

def clipFeatures(params, job, convertFeaturesDuringClip=True):
	global haveDataInterop
	cleanUpFeatureLayer = False
	# get the path and a validated name for the output
	layerName, outputpath = make_output_path(False, job['layer'], job['name'], convertFeaturesDuringClip, params.input_feature_format, params.zip_folder_path, params.scratch_folder_path, outputDataFolderName=params.output_folder_name)
	arcpy.AddMessage("Starting layer: %s where: %s" % (job['layer'], job['where']))
	feature_layer = layerName
	
	cleanUpFeatureLayer = True

	try:
		arcpy.MakeFeatureLayer_management(job['layer'], feature_layer)
		arcpy.SelectLayerByAttribute_management(feature_layer, "NEW_SELECTION", job['where'])
		count = int(arcpy.GetCount_management(feature_layer).getOutput(0))
		
	except:
		arcpy.AddWarning("Select Attributes Error ::  Layer=%s; Clause=%s" % (feature_layer, job['where']))
		arcpy.AddWarning(arcpy.GetMessages(2))
		return

	if count == 0:
		arcpy.AddWarning("Where clause yielded no records ::  Layer=%s; Clause=%s" % (feature_layer, job['where']))
		return

	try:

		if params.output_projection and params.output_projection in VALID_PROJECTION_ALIASES.keys():
			arcpy.AddMessage('Ready to project: feature_layer=%s; outputpath=%s' % (feature_layer, outputpath))
			out_coordinate_system = os.path.join(PROJECTIONS_FOLDER, VALID_PROJECTION_ALIASES[params.output_projection])
			arcpy.Project_management(feature_layer, outputpath, out_coordinate_system)
		else:
			arcpy.AddMessage('Ready to copy: feature_layer=%s; outputpath=%s' % (feature_layer, outputpath))
			arcpy.CopyFeatures_management(feature_layer, outputpath)

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
		raise

	finally:
		if cleanUpFeatureLayer and arcpy.Exists(feature_layer):
			arcpy.Delete_management(feature_layer)


def clipAndConvert(params):
	try:
		params.zip_folder_path = create_folder_in_scratch(params.output_folder_name)
		params.scratch_folder_path = create_folder_in_scratch("scratchfolder")

		for job in params.export_jobs:
			describe = arcpy.Describe(job['layer'])
			dataType = describe.DataType.lower()

			if dataType in ["featurelayer", "rasterlayer", "featureclass"]:
				if dataType == "rasterlayer":
					clipRaster(job['layer'], params.input_raster_format, params.zip_folder_path, params.scratch_folder_path)
				else:
					clipFeatures(params, job)
			else:
				arcpy.AddWarning(get_ID_message(86143) % (source_path, dataType))

		return params.zip_folder_path 

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
		params.commit_properties()

		if arcpy.CheckExtension("DataInteroperability") == "Available":
			arcpy.CheckOutExtension("DataInteroperability")
			haveDataInterop = True
		else:
			haveDataInterop = False

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

		zipFolder = clipAndConvert(params)
		create_zipfile(zipFolder, params.result_file)

		return params.result_file

	except:
		tb = sys.exc_info()[2]
		tbinfo = traceback.format_tb(tb)[0]
		pymsg = "ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
				str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"
		arcpy.AddError(pymsg)

def arcgis_parameter_bootstrap():

	params = ToolParameters()

	raw_layers = arcpy.GetParameterAsText(0)
	params.load_layers(raw_layers)

	raw_input_feature_format = arcpy.GetParameterAsText(1)
	params.load_input_feature_format(raw_input_feature_format)

	raw_input_raster_format = arcpy.GetParameterAsText(2)
	params.load_input_raster_format(raw_input_raster_format)

	params.output_projection = arcpy.GetParameterAsText(3)

	raw_zipfile_name = arcpy.GetParameterAsText(4)
	params.load_zip_file_name(raw_zipfile_name)

	params.output_folder_name = arcpy.GetParameterAsText(5)
	params.export_source_directory = arcpy.GetParameterAsText(6)

	return params

class ToolParameters(object):
	'''value object for storing export tool parameters'''

	def __init__(self):
		self.layers = None
		self.output_names = None
		self.input_feature_format = None
		self.input_raster_format = None
		self.output_projection = None
		self.where_clauses = None
		self.zipfile_name = None
		self.output_folder_name = None
		self.export_source_directory = None
		self.zipfile_path = None
		self.export_jobs = []
		self.projection_directory = None

		self.valid_projections = ['WGS_1984']

	def load_layers(self, rawInput):
		layer_json = json.loads(rawInput)
		if isinstance(layer_json, list) and isinstance(layer_json[0], dict):
			self.export_jobs = layer_json
		else:
			raise ValueError('Invalid layers property')

	def load_input_feature_format(self, raw_input_feature_format=None):
		if not raw_input_feature_format:
			self.input_feature_format = ["File Geodatabase", "GDB", ".gdb"]
		else:
			self.input_feature_format = map(lambda x: x.strip(), raw_input_feature_format.split("-"))
			if len(self.input_feature_format) < 3:
				self.input_feature_format.append("")

	def load_input_raster_format(self, raw_input_raster_format=None):
		if not raw_input_raster_format:
			self.input_raster_format = ["ESRI GRID", "GRID", ""]
		else:
			self.input_raster_format = map(lambda x: x.strip(), raw_input_raster_format.split("-"))
			if len(self.input_raster_format) < 3:
				self.input_raster_format.append("")

	def load_zip_file_name(self, raw_zip_file_name):
		if not raw_zip_file_name.endswith('.zip'):
			self.zipfile_name = raw_zip_file_name + '.zip'
		else:
			self.zipfile_name = raw_zip_file_name

	def commit_properties(self):
		self.result_file = os.path.join(arcpy.env.scratchWorkspace, self.zipfile_name)
		self.virtual_result_file = get_results_virtual_path(self.result_file) #TODO: FIX VIRTUAL DIRECTORY HACK

		for job in self.export_jobs:
			job['layer'] = os.path.join(self.export_source_directory, job['layer'])

class Tests(unittest.TestCase):
	'''
	python -m unittest ExtractDataWhere.Tests.test_export_shp
	'''
	def setUp(self):
		arcpy.env.scratchWorkspace = SCRATCH_FOLDER

		self.params = ToolParameters()
		self.params.load_layers(self.create_mock_jobs_json())
		self.params.load_input_feature_format('File Geodatabase - GDB - .gdb')
		self.params.input_raster_format = None
		self.params.output_projection = None
		self.params.load_zip_file_name('test_export_where')
		self.params.output_folder_name = 'spatial_data_export'
		self.params.export_source_directory = TEST_DATA_GDB
		self.params.result_file = None

		print '= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ='

	def tearDown(self):
		self.params = None

	def test_export_shp(self):
		function_name = sys._getframe().f_code.co_name
		self.params.load_zip_file_name(function_name)
		self.params.output_folder_name  = function_name

		self.params.load_input_feature_format('Shapefile - SHP - .shp')
		
		run_export(self.params)
		self.assertTrue(os.path.exists(self.params.result_file))

	def test_export_fgdb(self):
		function_name = sys._getframe().f_code.co_name
		self.params.load_zip_file_name(function_name)
		self.params.output_folder_name  = function_name

		self.params.load_input_feature_format('File Geodatabase - GDB - .gdb')
		run_export(self.params)
		self.assertTrue(os.path.exists(self.params.result_file))

	def test_export_wgs84_shp(self):
		function_name = sys._getframe().f_code.co_name
		self.params.load_zip_file_name(function_name)
		self.params.output_folder_name  = function_name

		self.params.load_input_feature_format('Shapefile - SHP - .shp')
		self.params.output_projection = 'WGS_1984'
		run_export(self.params)
		self.assertTrue(os.path.exists(self.params.result_file))

	def test_export_wgs84_fgdb(self):
		function_name = sys._getframe().f_code.co_name
		self.params.load_zip_file_name(function_name)
		self.params.output_folder_name  = function_name

		self.params.load_input_feature_format('File Geodatabase - GDB - .gdb')
		self.params.output_projection = 'WGS_1984'
		run_export(self.params)
		self.assertTrue(os.path.exists(self.params.result_file))

	def test_setting_output_directory_name(self):
		function_name = sys._getframe().f_code.co_name
		self.params.load_zip_file_name(function_name)
		self.params.output_folder_name  = function_name

		self.params.input_feature_format = 'File Geodatabase - GDB - .gdb'
		self.params.output_folder_name = 'TESTING_SETTING_DIRECTORY'
		run_export(self.params)
		self.assertTrue(os.path.exists(self.params.result_file))

	def create_mock_jobs_json(self):
		input_jobs = []

		export_1 = {}
		export_1['layer'] = 'export_test_1'
		export_1['name'] = 'custom_made_name_1'
		export_1['where'] = 'OBJECTID < 5'
		input_jobs.append(export_1)

		export_2 = {}
		export_2['layer'] = 'export_test_2'
		export_2['name'] = 'custom_made_name_2'
		export_2['where'] = 'OBJECTID >= 5 AND OBJECTID < 10'
		input_jobs.append(export_2)

		print json.dumps(input_jobs)
		return json.dumps(input_jobs)

if __name__ == '__main__':
	if arcpy.GetParameterAsText(0):
		params = arcgis_parameter_bootstrap()
		params.result_file = run_export(params)
		arcpy.SetParameterAsText(7, virtual_result_file)
	else:
		unittest.main()