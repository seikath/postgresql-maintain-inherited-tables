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
-- and  r.rulename like 'route_rule_bsms_in_p' || extract(year from now()+ interval '%(week)s week') || '%%'
-- and r.ev_enabled <> %(enabled)s 
and r.rulename = %(rulename)s
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
-- DROP TABLE %(tablename)s;

CREATE TABLE %(tablename)s
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
  CONSTRAINT %(tablename)s_id_pkey PRIMARY KEY (id ),
  CONSTRAINT %(tablename)s_uniq_ext_id_sid UNIQUE (ext_id , service_id ),
  CONSTRAINT %(tablename)s_partition_check CHECK (otime >= '%(first_day_of_week)s 00:00:00'::timestamp without time zone AND otime < '%(next_first_day_of_week)s 00:00:00'::timestamp without time zone)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE %(tablename)s
  OWNER TO seik;
GRANT ALL ON TABLE %(tablename)s TO seik;
GRANT ALL ON TABLE %(tablename)s TO mtel;
GRANT INSERT, TRIGGER ON TABLE %(tablename)s TO raiffeisen;

-- Index: %(tablename)s.%(tablename)s_client_sms_id_idx

-- DROP INDEX %(tablename)s.%(tablename)s_client_sms_id_idx;

CREATE INDEX %(tablename)s_client_sms_id_idx
  ON %(tablename)s
  USING btree
  (client_sms_id);

-- Index: %(tablename)s.%(tablename)s_ext_id_idx

-- DROP INDEX %(tablename)s.%(tablename)s_ext_id_idx;

CREATE INDEX %(tablename)s_ext_id_idx
  ON %(tablename)s
  USING btree
  (ext_id);

-- Index: %(tablename)s.%(tablename)s_forced_smsc_idx

-- DROP INDEX %(tablename)s.%(tablename)s_forced_smsc_idx;

CREATE INDEX %(tablename)s_forced_smsc_idx
  ON %(tablename)s
  USING btree
  (forced_smsc);

-- Index: %(tablename)s.%(tablename)s_msisdn_idx

-- DROP INDEX %(tablename)s.%(tablename)s_msisdn_idx;

CREATE INDEX %(tablename)s_msisdn_idx
  ON %(tablename)s
  USING btree
  (msisdn );

-- Index: %(tablename)s.%(tablename)s_oday_idx

-- DROP INDEX %(tablename)s.%(tablename)s_oday_idx;

CREATE INDEX %(tablename)s_oday_idx
  ON %(tablename)s
  USING btree
  (date_part('day'::text, otime) );

-- Index: %(tablename)s.%(tablename)s_oeyar_idx

-- DROP INDEX %(tablename)s.%(tablename)s_oeyar_idx;

CREATE INDEX %(tablename)s_oeyar_idx
  ON %(tablename)s
  USING btree
  (date_part('year'::text, otime) );

-- Index: %(tablename)s.%(tablename)s_omonth_idx

-- DROP INDEX %(tablename)s.%(tablename)s_omonth_idx;

CREATE INDEX %(tablename)s_omonth_idx
  ON %(tablename)s
  USING btree
  (date_part('month'::text, otime) );

-- Index: %(tablename)s.%(tablename)s_otime_idx

-- DROP INDEX %(tablename)s.%(tablename)s_otime_idx;

CREATE INDEX %(tablename)s_otime_idx
  ON %(tablename)s
  USING btree
  (otime );

-- Index: %(tablename)s.%(tablename)s_polytype_id

-- DROP INDEX %(tablename)s.%(tablename)s_polytype_id;

CREATE INDEX %(tablename)s_polytype_id
  ON %(tablename)s
  USING btree
  (polytype_id);

-- Index: %(tablename)s.%(tablename)s_priority_idx

-- DROP INDEX %(tablename)s.%(tablename)s_priority_idx;

CREATE INDEX %(tablename)s_priority_idx
  ON %(tablename)s
  USING btree
  (priority );

-- Index: %(tablename)s.%(tablename)s_processed_idx

-- DROP INDEX %(tablename)s.%(tablename)s_processed_idx;

CREATE INDEX %(tablename)s_processed_idx
  ON %(tablename)s
  USING btree
  (processed );

-- Index: %(tablename)s.%(tablename)s_service_id_idx

-- DROP INDEX %(tablename)s.%(tablename)s_service_id_idx;

CREATE INDEX %(tablename)s_service_id_idx
  ON %(tablename)s
  USING btree
  (service_id );

-- Index: %(tablename)s.%(tablename)s_shortcode_idx

-- DROP INDEX %(tablename)s.%(tablename)s_shortcode_idx;

CREATE INDEX %(tablename)s_shortcode_idx
  ON %(tablename)s
  USING btree
  (shortcode);

-- Index: %(tablename)s.new_%(tablename)s_otime_idx

-- DROP INDEX %(tablename)s.new_%(tablename)s_otime_idx;

CREATE INDEX new_%(tablename)s_otime_idx
  ON %(tablename)s
  USING btree
  (date_trunc('month'::text, otime) );

-- Index: %(tablename)s.test_idx_protelecom_%(tablename)s

-- DROP INDEX %(tablename)s.test_idx_protelecom_%(tablename)s;

CREATE INDEX test_idx_protelecom_%(tablename)s
  ON %(tablename)s
  USING btree
  (service_id , date_part('year'::text, otime) , date_part('month'::text, otime) );


-- Trigger: counters_%(tablename)s on %(tablename)s

-- DROP TRIGGER counters_%(tablename)s ON %(tablename)s;

CREATE TRIGGER counters_%(tablename)s
  AFTER INSERT OR DELETE
  ON %(tablename)s
  FOR EACH ROW
  EXECUTE PROCEDURE update_counters_bsmsin_trigger();

""")

create_rule_for_the_table_inherited = ("""
-- DROP RULE %(rulename)s ON %(table_name_base)s;

CREATE OR REPLACE RULE %(rulename)s AS
    ON INSERT TO %(table_name_base)s
   WHERE new.otime >= '%(first_day_of_week)s 00:00:00'::timestamp without time zone AND new.otime < '%(next_first_day_of_week)s 00:00:00'::timestamp without time zone DO INSTEAD  INSERT INTO %(tablename)s (client_sms_id, ext_id, msisdn, shortcode, mesg, operator_id, otime, ptime, processed, service_id, service_url, service_url_responce, request_ip, forced_smsc, cyrilic, wap_push, polytype_responce, priority, flash, polytype_id, retry, hlr, validity, token) 
  VALUES (new.client_sms_id, new.ext_id, new.msisdn, new.shortcode, new.mesg, new.operator_id, new.otime, new.ptime, new.processed, new.service_id, new.service_url, new.service_url_responce, new.request_ip, new.forced_smsc, new.cyrilic, new.wap_push, new.polytype_responce, new.priority, new.flash, new.polytype_id, new.retry, new.hlr, new.validity, new.token);
""")