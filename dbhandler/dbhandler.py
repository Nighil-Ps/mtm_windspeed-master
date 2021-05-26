import sys
import os
import json


import MySQLdb as sql

try:
        sys.path.insert( 0, '/usr/lib/python2.7/dist-packages')
        sys.path.insert( 0, '/usr/lib/python3.5/dist-packages')
except:
        print ("Invalid path")

def getSqlDetails():
	#cwd = os.getcwd()
	cwd = os.path.dirname(os.path.realpath(__file__))
	#Read from json
	data = json.load(open(cwd + '/richie.json'))
	return [data['host'], data['uname'], data['pass'], data['db']]



def connecttodb():
	host,uname,passwd,db = getSqlDetails();
	while True:
		try:			
			dbconn = sql.connect( host = host, user = uname, passwd = passwd, db = db, port=3306)	
			cur = dbconn.cursor()			
			break
		except sql.Error as e:
			try:
				print ("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
				#pass
			except IndexError:
				print ("MySQL Error: %s" % str(e))
				#pass
			continue
	return [dbconn, cur]


def executeQ(q):
	dbconn, cur = connecttodb()
	try:
		cur.execute(q)						
		data = cur.fetchall()
		dbconn.commit()
		closedb(dbconn, cur)
		return data
	except sql.Error as e:
		try:
			print ("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
			#pass
		except IndexError:
			print ("MySQL Error: %s" % str(e))
			#pass

def closedb(dbconn, cur):
	dbconn.close()
	cur.close()



if __name__ == "__main__":
	pass
