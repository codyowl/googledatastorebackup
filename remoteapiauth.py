import sys
import os
import sqlite3
import csv
import getpass

#for resolving path issues
if 'google' in sys.modules:
	del sys.modules['google']

sys.path.append(os.path.join('/usr/local/google_appengine'))
sys.path.append(os.path.join('/usr/local/google_appengine', 'lib', 'yaml', 'lib'))
sys.path.append(os.path.join('/usr/local/google_appengine', 'lib', 'fancy_urllib'))

os.chdir("/home/sampath/google_appengine/")

import appcfg

from google.appengine.ext.remote_api import remote_api_stub
from google.appengine.ext import db
from google.appengine.ext.db.metadata import Kind
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore

USERNAME = raw_input('Enter your Google app engine Username :')
PASSWORD = getpass.getpass('Enter your Google app engine password :')

APPLICATION_ID = raw_input('Enter your Applicaton id :')
APPLICATION_ID_SITE = APPLICATION_ID + '.appspot.com'

def auth_func():
	return (USERNAME, PASSWORD)

remote_api_stub.ConfigureRemoteApi(None, '/_ah/remote_api', auth_func, APPLICATION_ID_SITE)

kind_result = []

kinds = Kind.all()

for values in kinds:
	kind_result.append(str(values.kind_name))

#truncating to remove the builtin kinds
truncated_kind_result = [i for i in kind_result if i[:1] != '_']

# print truncated_kind_result

print "###############################################"

print "Your application authenticated"



for KIND in truncated_kind_result:
	FILE_NAME = raw_input("Enter the filename to dump the datastore data :")
	shell_command = "python appcfg.py download_data --kind='%s' --url=https://%s.appspot.com/_ah/remote_api --filename='%s.db'" %(KIND, APPLICATION_ID, FILE_NAME)
	os.system(shell_command)

DATABASE_NAME = FILE_NAME + '.db'

connection = sqlite3.connect(DATABASE_NAME, isolation_level=None)

cursor = connection.cursor()

cursor.execute('SELECT * FROM result')

#getting column names
column_name = [i[0] for i in cursor.description]

column_name = tuple(column_name[:-1])

column_name = ','.join(map(str, column_name))

query = "select %s from result" %(column_name)

cursor.execute(query)

result = []

for unused_entity, entity in cursor:
	entity_proto = entity_pb.EntityProto(contents=entity)
	entity = datastore.Entity._FromPb(entity_proto)
	result.append(entity) 

keys = result[1].keys()

print "###################################"
print " creating the csv file "

with open('%s.csv'%FILE_NAME, 'wb') as outputfile:
	dict_writer = csv.DictWriter(outputfile, keys, extrasaction='ignore')
	dict_writer.writeheader()
	dict_writer.writerows(result)

print " csv file '%s' has been created" % FILE_NAME	





