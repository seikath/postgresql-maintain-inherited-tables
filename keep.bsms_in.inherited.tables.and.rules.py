# ecoding=utf8

# 2013-07-27.00.46.56
############################################################################################
# cd /opt/proceed.bsms/psycopg2-2.5.1-1
# git remote add origin ssh://git@bitbucket.org/seikath/aegir.bsms.psycopg2-2.5.1-1.git
# git push -u origin --all   # to push up the repo for the first time
############################################################################################
# OR start from scratch
# mkdir /path/to/your/project
# cd /path/to/your/project
# git init
# git remote add origin ssh://git@bitbucket.org/seikath/aegir.bsms.psycopg2-2.5.1-1.git
# git add *
# git commit -m "psycopg2-2.5.1-1.git"
############################################################################################
# root@aegir.voicecom.bg:[Sat Jul 27 02:02:04]:[/opt/proceed.bsms/psycopg2-2.5.1-1]$ git push -u origin --all
# Counting objects: 3, done.
# Delta compression using up to 2 threads.
# Compressing objects: 100% (2/2), done.
# Writing objects: 100% (3/3), 439 bytes, done.
# Total 3 (delta 0), reused 0 (delta 0)
# To ssh://git@bitbucket.org/seikath/aegir.bsms.psycopg2-2.5.1-1.git
# * [new branch]      master -> master
# Branch master set up to track remote branch master from origin.
############################################################################################
## git checkout -b lean_db_conection
## git push origin lean_db_conection
#  https://drupal.org/node/1066342
# http://initd.org/psycopg/docs/module.html#exceptions
# http://initd.org/psycopg/docs/usage.html

# The basic connection parameters are:

# dbname – the database name (only in the dsn string)
# database – the database name (only as keyword argument)
# user – user name used to authenticate
# password – password used to authenticate
# host – database host address (defaults to UNIX socket if not provided)
# port – connection port number (defaults to 5432 if not provided)

import psycopg2
import psycopg2.extras
import sys
from datetime import datetime
import ConfigParser
import os
os.environ['PGCONNECT_TIMEOUT'] = '5'

#################################################
### CONFIG START 
config = ConfigParser.SafeConfigParser()
# get the config at the db.config file in the same directory
#print os.path.basename(os.path.dirname(os.path.abspath(__file__)))
# config.read(os.path.dirname(os.path.abspath(__file__))+'.c')
config_file=os.path.dirname(os.path.abspath(__file__))+'/'+os.path.basename(os.path.dirname(os.path.abspath(__file__)))+'.cfg'

#print config_file

try:
	config.read(config_file)
	config_aegir = dict(config.items("db-aegir-local"))
	weeks_to_deactivate = config.getint('rule-conf','weeks_to_deactivate')
	weeks_to_activate = config.getint('rule-conf','weeks_to_activate')
	table_name_base = config.get('rule-conf','table_name_base')
	vc_debug = config.getboolean('rule-conf','vc_debug')
	#print config_aegir
except ConfigParser.Error, e:
	print "["+str(datetime.now())+"] : " + "Error : %s" % (e)
	sys.exit(1)
	

try:
    conn_aegir = psycopg2.connect(**config_aegir)
    conn_aegir.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
except:
    print "I am unable to connect:: %s !!".format(config_aegir)
    sys.exit(1)

# sys.exit (0)
cur_aegir = conn_aegir.cursor(cursor_factory=psycopg2.extras.DictCursor)

query_rule = ("""
SELECT n.nspname AS schemaname, c.relname AS tablename, r.rulename, r.ev_enabled as active,
    pg_get_ruledef(r.oid) AS definition
   FROM pg_rewrite r
   JOIN pg_class c ON c.oid = r.ev_class
   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE true 
-- and r.rulename <> '_RETURN'::name
and c.relname = %(tablename)s
and  r.rulename like 'route_rule_bsms_in_p' || extract(year from now()+ interval '%(week)s week') || '%%'
-- and r.ev_enabled <> %(enabled)s 
and r.rulename  in (
	select  
	'route_rule_bsms_in_p' 
	|| extract(year from now()+ interval '%(week)s week') 
	|| 'w' || extract(week from now() + interval '%(week)s week') 
	as  rulename
	)
order by r.rulename 
limit 1
;
""")

#debug = False
##debug = True
#vc_debug = False
##vc_debug = True
#epg_debug = True
## epg_debug = False

if not vc_debug : print "["+str(datetime.now())+"] : Debug set to false at " + os.path.abspath(__file__) 

print cur_aegir.mogrify(query_rule,{'week' : weeks_to_deactivate,'enabled' : "D",'tablename': table_name_base})


try:
    cur_aegir.execute(query_rule,{'week' : weeks_to_deactivate,'enabled' : "D",'tablename': table_name_base})
except psycopg2.Error, e:
	print ("["+str(datetime.now())+"] : {0!s} conencting to {1!s} at {2!s} as user {3!s}".
	format(
		e.args[0].rstrip('\r\n'),
		config_aegir.get('host').rstrip('\r\n'),
		config_aegir.get('dbname').rstrip('\r\n'),
		config_aegir.get('user').rstrip('\r\n'),
	)
	)

rec = cur_aegir.fetchone()
print "[{0!s}]".format(cur_aegir.rowcount)
if cur_aegir.rowcount:
	if rec['active']:
		print "We have active rule {0!s}".format(rec['rulename'])
	else:
		print "We have inactive rule {0!s}".format(rec['rulename'])
else:
	print "We dont have rule serving that week"
	
#print rec['rulename']

#print rec['active']





cur_aegir.close()
conn_aegir.close()
sys.exit (0)



# CONFIGs, SQL etc ========= [ end ] 

# initialize db conection to nova
#try:
	#cnx_aegir = MySQLdb.connect(**config_nova)
	## open nova cursor
	#cursor_nova = cnx_nova.cursor()
#except MySQLdb.Error, e:
	#print "["+str(datetime.now())+"] : " + "Error %d: %s" % (e.args[0], e.args[1])
	#sys.exit (1)
	
# start the data processing
#try:
	#cursor_nova.execute(query)
	## initialize db conection to pdns
	#try:
		#cnx_pdns = MySQLdb.connect(**config_pdns)
		## open pdns cursor
		#cursor_pdns = cnx_pdns.cursor()
	#except MySQLdb.Error, e:
		#print "["+str(datetime.now())+"] : " + "Error %d: %s" % (e.args[0], e.args[1])
		#sys.exit (1)
#except Exception, e:
	#print "["+str(datetime.now())+"] : " + repr(e)
	#sys.exit (1)
