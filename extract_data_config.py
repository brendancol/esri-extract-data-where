import os

SERVER_VIRTUAL_DIRECTORIES = r'Server virtual directory not set review top of export script'
SCRATCH_FOLDER = os.path.join(os.path.dirname(__file__), 'scratch')

# test constants
TEST_DATA_FOLDER = os.path.join(os.path.dirname(__file__), 'test_data')
TEST_DATA_GDB = os.path.join(TEST_DATA_FOLDER, 'test_data.gdb')
TEST_DATA_SHP = os.path.join(TEST_DATA_FOLDER, 'test_data_shp')

# project info
VALID_PROJECTION_ALIASES = {}
VALID_PROJECTION_ALIASES['WGS_1984'] = 'WGS_1984.prj'
VALID_PROJECTION_ALIASES['WGS1984'] = 'WGS_1984.prj'
VALID_PROJECTION_ALIASES['WGS84'] = 'WGS_1984.prj'
VALID_PROJECTION_ALIASES['4326'] = 'WGS_1984.prj'
VALID_PROJECTION_ALIASES['geographic'] = 'WGS_1984.prj'

PROJECTIONS_FOLDER = os.path.join(os.path.dirname(__file__), 'projections')