# ecoding=utf8

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
	limit 1
	)
order by r.rulename 
limit 1
;
""")


query_table_is_inherited = ("""

SELECT 
pg_inherits.*
, c.relname AS child
, p.relname AS parent
FROM
    pg_inherits JOIN pg_class AS c ON (inhrelid=c.oid)
    JOIN pg_class as p ON (inhparent=p.oid) 
    where true 
    and p.relname = %(table_name_base)s
    and c.relname = %(inherited_table)s
;
""")
query_table_exists = ("""
select 
	table_name 
	from information_schema.tables 
	where true 
	and table_name=%(inherited_table)s
;
""")

alter_table_inherit = ("""
alter table 
	%s 
	%s inherit
	%s
;
""")

create_trigger_on_table_inherited = ("""
CREATE TRIGGER counters_%s
  AFTER INSERT OR DELETE
  ON %s
  FOR EACH ROW
  EXECUTE PROCEDURE update_counters_bsmsin_trigger();
""")

	
create_missing_inherited_table= ("""
-- DROP TABLE {0!s};

CREATE TABLE {0!s}
(
  id bigint NOT NULL DEFAULT nextval('bsms_in_id_seq'::regclass),
  client_sms_id character varying(100) NOT NULL,
  ext_id character varying(100) NOT NULL,
  msisdn bigint NOT NULL DEFAULT 0,
  shortcode character varying(11) NOT NULL,
  mesg text,
  operator_id smallint NOT NULL DEFAULT 0,
  otime timestamp without time zone NOT NULL,
  ptime timestamp without time zone,
  processed boolean DEFAULT false,
  service_id bigint NOT NULL DEFAULT 0,
  service_url text,
  service_url_responce text,
  request_ip cidr DEFAULT '127.0.0.1/32'::cidr,
  forced_smsc character varying(200) DEFAULT 'infopipTEST'::character varying,
  cyrilic smallint NOT NULL DEFAULT 0,
  wap_push smallint NOT NULL DEFAULT 0,
  polytype_responce xml,
  priority smallint DEFAULT 2,
  flash smallint DEFAULT 0,
  polytype_id character varying(60),
  retry bigint DEFAULT 0,
  hlr integer DEFAULT 0,
  validity integer DEFAULT 1440,
  token character varying(100),
  CONSTRAINT {0!s}_id_pkey PRIMARY KEY (id ),
  CONSTRAINT {0!s}_uniq_ext_id_sid UNIQUE (ext_id , service_id ),
--  CONSTRAINT {0!s}_partition_check CHECK (otime >= '2013-09-23 00:00:00'::timestamp without time zone AND otime < '2013-09-30 00:00:00'::timestamp without time zone)
  CONSTRAINT {0!s}_partition_check CHECK (otime >= '{1!s} 00:00:00'::timestamp without time zone AND otime < '{2!s} 00:00:00'::timestamp without time zone)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE {0!s}
  OWNER TO seik;
GRANT ALL ON TABLE {0!s} TO seik;
GRANT ALL ON TABLE {0!s} TO mtel;
GRANT INSERT, TRIGGER ON TABLE {0!s} TO raiffeisen;

-- Index: {0!s}.{0!s}_client_sms_id_idx

-- DROP INDEX {0!s}.{0!s}_client_sms_id_idx;

CREATE INDEX {0!s}_client_sms_id_idx
  ON {0!s}
  USING btree
  (client_sms_id COLLATE pg_catalog."default" );

-- Index: {0!s}.{0!s}_ext_id_idx

-- DROP INDEX {0!s}.{0!s}_ext_id_idx;

CREATE INDEX {0!s}_ext_id_idx
  ON {0!s}
  USING btree
  (ext_id COLLATE pg_catalog."default" );

-- Index: {0!s}.{0!s}_forced_smsc_idx

-- DROP INDEX {0!s}.{0!s}_forced_smsc_idx;

CREATE INDEX {0!s}_forced_smsc_idx
  ON {0!s}
  USING btree
  (forced_smsc COLLATE pg_catalog."default" );

-- Index: {0!s}.{0!s}_msisdn_idx

-- DROP INDEX {0!s}.{0!s}_msisdn_idx;

CREATE INDEX {0!s}_msisdn_idx
  ON {0!s}
  USING btree
  (msisdn );

-- Index: {0!s}.{0!s}_oday_idx

-- DROP INDEX {0!s}.{0!s}_oday_idx;

CREATE INDEX {0!s}_oday_idx
  ON {0!s}
  USING btree
  (date_part('day'::text, otime) );

-- Index: {0!s}.{0!s}_oeyar_idx

-- DROP INDEX {0!s}.{0!s}_oeyar_idx;

CREATE INDEX {0!s}_oeyar_idx
  ON {0!s}
  USING btree
  (date_part('year'::text, otime) );

-- Index: {0!s}.{0!s}_omonth_idx

-- DROP INDEX {0!s}.{0!s}_omonth_idx;

CREATE INDEX {0!s}_omonth_idx
  ON {0!s}
  USING btree
  (date_part('month'::text, otime) );

-- Index: {0!s}.{0!s}_otime_idx

-- DROP INDEX {0!s}.{0!s}_otime_idx;

CREATE INDEX {0!s}_otime_idx
  ON {0!s}
  USING btree
  (otime );

-- Index: {0!s}.{0!s}_polytype_id

-- DROP INDEX {0!s}.{0!s}_polytype_id;

CREATE INDEX {0!s}_polytype_id
  ON {0!s}
  USING btree
  (polytype_id COLLATE pg_catalog."default" );

-- Index: {0!s}.{0!s}_priority_idx

-- DROP INDEX {0!s}.{0!s}_priority_idx;

CREATE INDEX {0!s}_priority_idx
  ON {0!s}
  USING btree
  (priority );

-- Index: {0!s}.{0!s}_processed_idx

-- DROP INDEX {0!s}.{0!s}_processed_idx;

CREATE INDEX {0!s}_processed_idx
  ON {0!s}
  USING btree
  (processed );

-- Index: {0!s}.{0!s}_service_id_idx

-- DROP INDEX {0!s}.{0!s}_service_id_idx;

CREATE INDEX {0!s}_service_id_idx
  ON {0!s}
  USING btree
  (service_id );

-- Index: {0!s}.{0!s}_shortcode_idx

-- DROP INDEX {0!s}.{0!s}_shortcode_idx;

CREATE INDEX {0!s}_shortcode_idx
  ON {0!s}
  USING btree
  (shortcode COLLATE pg_catalog."default" );

-- Index: {0!s}.new_{0!s}_otime_idx

-- DROP INDEX {0!s}.new_{0!s}_otime_idx;

CREATE INDEX new_{0!s}_otime_idx
  ON {0!s}
  USING btree
  (date_trunc('month'::text, otime) );

-- Index: {0!s}.test_idx_protelecom_{0!s}

-- DROP INDEX {0!s}.test_idx_protelecom_{0!s};

CREATE INDEX test_idx_protelecom_{0!s}
  ON {0!s}
  USING btree
  (service_id , date_part('year'::text, otime) , date_part('month'::text, otime) );


-- Trigger: counters_{0!s} on {0!s}

-- DROP TRIGGER counters_{0!s} ON {0!s};

CREATE TRIGGER counters_{0!s}
  AFTER INSERT OR DELETE
  ON {0!s}
  FOR EACH ROW
  EXECUTE PROCEDURE update_counters_bsmsin_trigger();

""")
