# ecoding=utf8
# 2013-08-11.22.02.43
# dev.add.create.tables.v.1.1
# 2013-08-17.11.54.20
import psycopg2
import psycopg2.extras
import sys
#import datetime
from datetime import timedelta,date
import calendar
import datetime
#import date
import ConfigParser
import os
os.environ['PGCONNECT_TIMEOUT'] = '5'

#################################################
### CONFIG START 
# import local vars - SQL templates
from vars import *
config = ConfigParser.SafeConfigParser()
# get the config at the db.config file in the same directory
config_file=os.path.dirname(os.path.abspath(__file__))+'/'+os.path.basename(os.path.dirname(os.path.abspath(__file__)))+'.cfg'

try:
	config.read(config_file)
	weeks_to_deactivate = config.getint('rule-conf','weeks_to_deactivate')
	weeks_to_activate = config.getint('rule-conf','weeks_to_activate')
	table_name_base = config.get('rule-conf','table_name_base')
	vc_debug = config.getboolean('rule-conf','vc_debug')
	vc_sql_debug = config.getboolean('rule-conf','vc_sql_debug')
	if len(sys.argv) > 1 and sys.argv[1] == 'darkwater':
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Loading db config : db-darkwater-vpn"
		config_db = dict(config.items("db-darkwater-vpn"))
	elif len(sys.argv) > 1 and sys.argv[1] == 'aegir':
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Loading db config : db-aegir-local"
		config_db = dict(config.items("db-aegir-local"))	
	else:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : [missind db] usege: {0!s} db_server as (aegir|darkwater)".format(sys.argv[0])
		sys.exit(0)	
	if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] db config : {0!s}".format(config_db)
except ConfigParser.Error, e:
	print "["+str(datetime.now())+"] : ["+sys.argv[1]+"] " + "Error : %s" % (e)
	sys.exit(1)
	
if not vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Debug set to false at " + os.path.abspath(__file__) 
### CONFIG ENDS
#################################################


def sql_execute(cursor,sql,vc_sql_debug=False):
	try:
		cursor.execute(sql)
		if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] executed '{0!s}'".format(sql)
		return cursor
	except psycopg2.Error, e:
		print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] ERROR : {0!s} on executing of {1!s}".
			format(
				e.args[0].rstrip('\r\n'),
				sql.rstrip('\r\n'),
			)
		)
		sys.exit(1)

### Initiate db link 
try:
    conn_db = psycopg2.connect(**config_db)
    conn_db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
except:
    print "["+str(datetime.now())+"] : ["+sys.argv[1]+"] Not able to connect:: %s !!".format(config_db)
    sys.exit(1)

### Open db cursor
cur_db = conn_db.cursor(cursor_factory=psycopg2.extras.DictCursor)



for week in xrange(-weeks_to_deactivate,-1):
	relative_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))))
	relative_next_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))) + datetime.timedelta(days=+7))
	relative_week='{0:02d}'.format(date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[1])
	relative_year=date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[0]
	rulename='route_rule_bsms_in_p{0!s}w{1!s}'.format(relative_year,relative_week)
	#print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] {0!s}".format(week)
	if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Checking is the rule {1!s} for {0!s} week is ebabled..".format(week,rulename)
	sql_check_rule_to_deactivate=cur_db.mogrify(query_rule,{'week' : week,'enabled' : "D",'tablename': table_name_base,'rulename':rulename})
	if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(sql_check_rule_to_deactivate)
	cur_db=sql_execute(cur_db,sql_check_rule_to_deactivate,vc_sql_debug)
	rec_deactiavate = cur_db.fetchone()
	
	if int(cur_db.rowcount) > 0:
		# tva e O 
		if rec_deactiavate['active'] == 'O':
			print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have active rule {0!s} to deactivate".format(rec_deactiavate['rulename'])
			sql_to_deactivete="alter table {0!s} disable rule {1!s};".format(table_name_base,rec_deactiavate['rulename'])
			cur_db=sql_execute(cur_db,sql_to_deactivete,vc_sql_debug)	
		else:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have inactive rule {0!s}".format(rec_deactiavate['rulename'])
	else:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We dont have rule serving {0!s} weeks".format(week)
	
# Start checking inherited tables
if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Start checking inherited tables {0!s} weeks ahead..".format(weeks_to_activate-1)
for week in xrange(1,weeks_to_activate):
	relative_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))))
	relative_next_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))) + datetime.timedelta(days=+7))
	relative_week='{0:02d}'.format(date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[1])
	relative_year=date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[0]
	rulename='route_rule_bsms_in_p{0!s}w{1!s}'.format(relative_year,relative_week)
	inherited_table = (datetime.datetime.today() + datetime.timedelta(weeks=week)).strftime("bsms_in_p%Yw%U")
	if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Checking inherited table {0!s} existance..".format(inherited_table)
	sql_check_table_existance=cur_db.mogrify(query_table_exists,{'inherited_table' : inherited_table})
	if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(sql_check_table_existance)
	cur_db=sql_execute(cur_db,sql_check_table_existance,vc_sql_debug)
	if int(cur_db.rowcount) > 0:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Table {0!s} exists.".format(inherited_table)
		sql_check_table_inherits=cur_db.mogrify(query_table_is_inherited,{'inherited_table' : inherited_table,'table_name_base' : table_name_base})
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Checking if table {1!s} inherits {0!s}".format(table_name_base,inherited_table)
		cur_db=sql_execute(cur_db,sql_check_table_inherits,vc_sql_debug)
		if int(cur_db.rowcount) > 0:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Table {1!s} inherits {0!s}".format(table_name_base,inherited_table)
		else:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Table {1!s} does not inherit {0!s}".format(table_name_base,inherited_table)
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Altering table {1!s} to inherit {0!s}".format(table_name_base,inherited_table)
			alter_table_inherit_tmp = alter_table_inherit % (inherited_table,'',table_name_base);
			if vc_sql_debug : print cur_db.mogrify(alter_table_inherit_tmp)
			cur_db=sql_execute(cur_db,alter_table_inherit_tmp,vc_sql_debug)
		# Check corresponsding rule 
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Checking if the rule {1!s} for {0!s} week is ebabled..".format(week,rulename)
		sql_check_rule_to_activate=cur_db.mogrify(query_rule,{'week' : week,'enabled' : "D",'tablename': table_name_base,'rulename':rulename})
		if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(sql_check_rule_to_activate)
		cur_db=sql_execute(cur_db,sql_check_rule_to_activate,vc_sql_debug)
		rec_actiavate = cur_db.fetchone()
		if int(cur_db.rowcount) > 0:
			# tva e O 
			if rec_actiavate['active'] == 'D':
				if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have nonactive rule {0!s} to activate".format(rec_actiavate['rulename'])
				sql_to_activete="alter table {0!s} enable rule {1!s};".format(table_name_base,rec_actiavate['rulename'])
				cur_db=sql_execute(cur_db,sql_to_activete,vc_sql_debug)
			else:
				if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have active rule {0!s}".format(rec_actiavate['rulename'])
		else:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We dont have rule {1!s} serving {0!s} weeks. It will be created now.".format(week,rulename)
			sql_create_missing_rule=create_rule_for_the_table_inherited % {'tablename':'bsms_in_p'+str(relative_year)+'w'+str(relative_week),'table_name_base':table_name_base,'rulename':rulename,'first_day_of_week':relative_first_day_of_week,'next_first_day_of_week':relative_next_first_day_of_week}
			if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(cur_db.mogrify(sql_create_missing_rule))
			cur_db=sql_execute(cur_db,sql_create_missing_rule,vc_sql_debug)
	else:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Table {0!s} does NOT exist. It will be createde now.".format(inherited_table)
		sql_create_missing_table=create_missing_inherited_table % {'tablename':'bsms_in_p'+str(relative_year)+'w'+str(relative_week),'first_day_of_week':relative_first_day_of_week,'next_first_day_of_week':relative_next_first_day_of_week}
		if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(cur_db.mogrify(sql_create_missing_table))
		cur_db=sql_execute(cur_db,sql_create_missing_table,vc_sql_debug)
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We might NOT have rule {1!s} serving {0!s} weeks. It will be created now,".format(week,rulename)
		sql_create_missing_rule=create_rule_for_the_table_inherited % {'tablename':'bsms_in_p'+str(relative_year)+'w'+str(relative_week),'table_name_base':table_name_base,'rulename':rulename,'first_day_of_week':relative_first_day_of_week,'next_first_day_of_week':relative_next_first_day_of_week}
		cur_db=sql_execute(cur_db,sql_create_missing_rule,vc_sql_debug)
# closing db cursor, db links
cur_db.close()
conn_db.close()
sys.exit (0)


