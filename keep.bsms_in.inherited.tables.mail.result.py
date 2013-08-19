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
import smtplib
# import local vars - SQL templates
from vars import *
os.environ['PGCONNECT_TIMEOUT'] = '5'

#################################################
### CONFIG START 
mailtext=''
def logit (what_to_log,db_used):
	global mailtext
	mailtext = mailtext + "\r\n" + "["+str(datetime.datetime.now())+"] : ["+db_used+"] {0!s}".format(what_to_log)
	print "["+str(datetime.datetime.now())+"] : ["+db_used+"] {0!s}".format(what_to_log)


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
	# get mail config
	mail_from_addr = config.get('mail-conf','mail_from_addr')
	mail_smtp = config.get('mail-conf','mail_smtp')
	mail_to_addr_list = config.get('mail-conf','mail_to_addr_list')
	mail_cc_addr_list = config.get('mail-conf','mail_cc_addr_list')
	mail_login = config.get('mail-conf','mail_login')
	mail_passwd = config.get('mail-conf','mail_passwd')

	if len(sys.argv) > 1 and sys.argv[1] == 'darkwater':
		if vc_debug : logit("Loading db config : db-darkwater-vpn",sys.argv[1])
		config_db = dict(config.items("db-darkwater-vpn"))
	elif len(sys.argv) > 1 and sys.argv[1] == 'aegir':
		if vc_debug : logit("Loading db config : db-aegir-local",sys.argv[1])
		config_db = dict(config.items("db-aegir-local"))	
	else:
		if vc_debug : logit("[missind db] usege: {0!s} db_server as (aegir|darkwater)".format(sys.argv[0]),'missind db')
		sys.exit(0)
	if vc_sql_debug : logit("db config : {0!s}".format(config_db),sys.argv[1])
except ConfigParser.Error, e:
	logit("Error : %s" % (e),sys.argv[1])
	sys.exit(1)

if not vc_debug : logit("Debug set to false at " + os.path.abspath(__file__),sys.argv[1])
#if not vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Debug set to false at " + os.path.abspath(__file__) 
### CONFIG ENDS
#################################################

def sendemail(from_addr, to_addr_list, cc_addr_list,
              subject, message,
              login, password,
              smtpserver='smtp.gmail.com:587'):
    header  = 'From: %s\n' % from_addr
    header += 'To: %s\n' % ','.join(to_addr_list)
    header += 'Cc: %s\n' % ','.join(cc_addr_list)
    header += 'Subject: %s\n\n' % subject
    message = header + message
  
    server = smtplib.SMTP(smtpserver)
    ## server.starttls()
    server.login(login,password)
    problems = server.sendmail(from_addr, to_addr_list, message)
    server.quit()

def sql_execute(cursor,sql,vc_sql_debug=False):
	try:
		cursor.execute(sql)
		if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] executed '{0!s}'".format(sql)
		return cursor
	except psycopg2.Error, e:
		logit("ERROR : {0!s} on executing of {1!s}".
			format(
				e.args[0].rstrip('\r\n'),
				sql.rstrip('\r\n'),
			),sys.argv[1]
		)
		sys.exit(1)


### Initiate db link 
try:
	conn_db = psycopg2.connect(**config_db)
	conn_db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
except:
	logit("Not able to connect:: {0!s} !!".format(config_db),sys.argv[1])
#	print "["+str(datetime.now())+"] : ["+sys.argv[1]+"] Not able to connect:: %s !!".format(config_db)
	sys.exit(1)

### Open db cursor
cur_db = conn_db.cursor(cursor_factory=psycopg2.extras.DictCursor)



for week in xrange(-weeks_to_deactivate,-1):
	relative_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))))
	relative_next_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))) + datetime.timedelta(days=+7))
	relative_week='{0:02d}'.format(date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[1])
	relative_year=date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[0]
	rulename='route_rule_bsms_in_p{0!s}w{1!s}'.format(relative_year,relative_week)
	if vc_debug : logit("Checking is the rule {1!s} for {0!s} week is ebabled..".format(week,rulename),sys.argv[1])
	sql_check_rule_to_deactivate=cur_db.mogrify(query_rule,{'week' : week,'enabled' : "D",'tablename': table_name_base,'rulename':rulename})
	if vc_sql_debug : logit("SQL: {0!s}".format(sql_check_rule_to_deactivate),sys.argv[1])
	cur_db=sql_execute(cur_db,sql_check_rule_to_deactivate,vc_sql_debug)
	rec_deactiavate = cur_db.fetchone()
	
	if int(cur_db.rowcount) > 0:
		# tva e O 
		if rec_deactiavate['active'] == 'O':
			logit("We have active rule {0!s} to deactivate".format(rec_deactiavate['rulename']),sys.argv[1])
			#print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have active rule {0!s} to deactivate".format(rec_deactiavate['rulename'])
			sql_to_deactivete="alter table {0!s} disable rule {1!s};".format(table_name_base,rec_deactiavate['rulename'])
			cur_db=sql_execute(cur_db,sql_to_deactivete,vc_sql_debug)	
		else:
			if vc_debug : logit("We have inactive rule {0!s}".format(rec_deactiavate['rulename']),sys.argv[1])
	else:
		if vc_debug : logit("We dont have rule serving {0!s} weeks".format(week),sys.argv[1])
	
# Start checking inherited tables
if vc_debug : logit("Start checking inherited tables {0!s} weeks ahead..".format(weeks_to_activate-1),sys.argv[1])
for week in xrange(1,weeks_to_activate):
	relative_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))))
	relative_next_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))) + datetime.timedelta(days=+7))
	relative_week='{0:02d}'.format(date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[1])
	relative_year=date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[0]
	rulename='route_rule_bsms_in_p{0!s}w{1!s}'.format(relative_year,relative_week)
	#inherited_table = (datetime.datetime.today() + datetime.timedelta(weeks=week)).strftime("bsms_in_p%Yw%U")
	inherited_table = 'bsms_in_p{0!s}w{1!s}'.format(relative_year,relative_week)
	if vc_debug : logit("Checking inherited table {0!s} existance..".format(inherited_table),sys.argv[1])
	sql_check_table_existance=cur_db.mogrify(query_table_exists,{'inherited_table' : inherited_table})
	if vc_sql_debug : logit("SQL: {0!s}".format(sql_check_table_existance),sys.argv[1])
	cur_db=sql_execute(cur_db,sql_check_table_existance,vc_sql_debug)
	if int(cur_db.rowcount) > 0:
		if vc_debug : logit("Table {0!s} exists.".format(inherited_table),sys.argv[1])
		sql_check_table_inherits=cur_db.mogrify(query_table_is_inherited,{'inherited_table' : inherited_table,'table_name_base' : table_name_base})
		if vc_debug : logit("Checking if table {1!s} inherits {0!s}".format(table_name_base,inherited_table),sys.argv[1])
		cur_db=sql_execute(cur_db,sql_check_table_inherits,vc_sql_debug)
		if int(cur_db.rowcount) > 0:
			if vc_debug : logit("Table {1!s} inherits {0!s}".format(table_name_base,inherited_table),sys.argv[1])
		else:
			if vc_debug : logit("Table {1!s} does not inherit {0!s}".format(table_name_base,inherited_table),sys.argv[1])
			if vc_debug : logit("Altering table {1!s} to inherit {0!s}".format(table_name_base,inherited_table),sys.argv[1])
			alter_table_inherit_tmp = alter_table_inherit % (inherited_table,'',table_name_base);
			if vc_sql_debug : logit(cur_db.mogrify(alter_table_inherit_tmp),sys.argv[1])
			cur_db=sql_execute(cur_db,alter_table_inherit_tmp,vc_sql_debug)
		# Check corresponsding rule 
		if vc_debug : logit("Checking if the rule {1!s} for {0!s} week is ebabled..".format(week,rulename),sys.argv[1])
		sql_check_rule_to_activate=cur_db.mogrify(query_rule,{'week' : week,'enabled' : "D",'tablename': table_name_base,'rulename':rulename})
		if vc_sql_debug : logit("SQL: {0!s}".format(sql_check_rule_to_activate),sys.argv[1])
		cur_db=sql_execute(cur_db,sql_check_rule_to_activate,vc_sql_debug)
		rec_actiavate = cur_db.fetchone()
		if int(cur_db.rowcount) > 0:
			# tva e O 
			if rec_actiavate['active'] == 'D':
				if vc_debug : logit("We have nonactive rule {0!s} to activate".format(rec_actiavate['rulename']),sys.argv[1])
				sql_to_activete="alter table {0!s} enable rule {1!s};".format(table_name_base,rec_actiavate['rulename'])
				cur_db=sql_execute(cur_db,sql_to_activete,vc_sql_debug)
			else:
				if vc_debug : logit("We have active rule {0!s}".format(rec_actiavate['rulename']),sys.argv[1])
		else:
			if vc_debug : logit("We dont have rule {1!s} serving {0!s} weeks. It will be created now.".format(week,rulename),sys.argv[1])
			sql_create_missing_rule=create_rule_for_the_table_inherited % {'tablename':'bsms_in_p'+str(relative_year)+'w'+str(relative_week),'table_name_base':table_name_base,'rulename':rulename,'first_day_of_week':relative_first_day_of_week,'next_first_day_of_week':relative_next_first_day_of_week}
			if vc_sql_debug : logit("SQL: {0!s}".format(cur_db.mogrify(sql_create_missing_rule)),sys.argv[1])
			cur_db=sql_execute(cur_db,sql_create_missing_rule,vc_sql_debug)
	else:
		if vc_debug : logit("Table {0!s} does NOT exist. It will be createde now.".format(inherited_table),sys.argv[1])
		sql_create_missing_table=create_missing_inherited_table % {'tablename':'bsms_in_p'+str(relative_year)+'w'+str(relative_week),'first_day_of_week':relative_first_day_of_week,'next_first_day_of_week':relative_next_first_day_of_week}
		if vc_sql_debug : logit("SQL: {0!s}".format(cur_db.mogrify(sql_create_missing_table)),sys.argv[1])
		cur_db=sql_execute(cur_db,sql_create_missing_table,vc_sql_debug)
		if vc_debug : logit("We might NOT have rule {1!s} serving {0!s} weeks. It will be created now,".format(week,rulename),sys.argv[1])
		sql_create_missing_rule=create_rule_for_the_table_inherited % {'tablename':'bsms_in_p'+str(relative_year)+'w'+str(relative_week),'table_name_base':table_name_base,'rulename':rulename,'first_day_of_week':relative_first_day_of_week,'next_first_day_of_week':relative_next_first_day_of_week}
		cur_db=sql_execute(cur_db,sql_create_missing_rule,vc_sql_debug)
#send mail report
sendemail(
	from_addr    = mail_from_addr
	,to_addr_list = [mail_to_addr_list]
	,cc_addr_list = [mail_cc_addr_list]
	,subject      = "Aegir inheritance maintenance report ["+str(datetime.datetime.now())+"]"
	,message      =  mailtext
	,login        = mail_login
	,password     = mail_passwd
	,smtpserver = mail_smtp
	)
if vc_debug : logit("Sent mail report to {0!s}".format(mail_from_addr),sys.argv[1])
# closing db cursor, db links

cur_db.close()
conn_db.close()
sys.exit (0)


