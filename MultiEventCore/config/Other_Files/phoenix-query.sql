create table rce.threat_raw(eT timestamp not null, ti BIGINT not null, b.dT varchar[], lo varchar(200), a.bf varchar(30), a.bc varchar(40), a.al varchar(20), a.ay varchar(70), a.cU integer(11), id varchar CONSTRAINT PK PRIMARY KEY(eT,ti)) TTL = 1296000, SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.partial_rule_match(et timestamp not null, ct varchar(50) not null, mr integer(4) not null, ti BIGINT not null, mp BIGINT, aa varchar(255), ab integer(5), ac integer(6), ad integer(6), a.ae varchar(20), af varchar(255), a.ag integer(6), ah varchar(255), a.ai varchar(255), a.aj varchar(255), a.ak varchar(255), a.al varchar(20), am BIGINT, an BIGINT, ao BIGINT, ap varchar(255), aq varchar(255), ar varchar(255), da varchar(255), at varchar(255), au varchar(255), av varchar(255), aw varchar(255), a.ax varchar(255), a.ay varchar(255), az varchar(255), ba varchar(255), bb varchar(255), a.bc varchar(255), bd timestamp, be varchar(255), a.bf varchar(255), a.bg timestamp, bh BIGINT, bi varchar(255), bj varchar(255), bk varchar(255), bl varchar(255), a.bm varchar(1000), a.bn varchar(1000), bo varchar(255), bp varchar(255), bq varchar(255), br varchar(255), bs varchar(255), bt varchar(255), bu varchar(255), bv varchar(255), a.bw varchar(255), a.bx varchar(255), db varchar(255), dc integer(6), a.bz varchar(255), ca timestamp, cb varchar(255), cg varchar(255), ch varchar(255), ci varchar(255), cp varchar(255), cq varchar(255), cr varchar(255), cs varchar(255),sz varchar(255),dz varchar(255), a.cu varchar(50) , cd varchar(255),a.dT varchar[],id varchar CONSTRAINT PK PRIMARY KEY(et,ct,mr,ti)) TTL = 432000,IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.tifeed_repo(ln varchar(20) not null,ds varchar(400) not null, rs integer(11) not null,CONSTRAINT PK PRIMARY KEY(ln,ds,rs)) IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.threat_rule_match (id integer(11) primary key, ti BIGINT, mr integer(11), et timestamp,dT varchar[],cu varchar(200)) TTL = 7776000, SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.threat_rule_parent_child_mapping(ti BIGINT not null, mr integer(11) not null, dt BIGINT[], tn varchar(200), eT timestamp,tt varchar(20),rr varchar(20),fl integer(11),id varchar CONSTRAINT PK PRIMARY KEY(ti,mr)) TTL = 7776000, SALT_BUCKETS = 4, IMMUTABLE_ROWS=true,COMPRESSION='GZ';

create table rce.geo_location_master(id integer(11),tin BIGINT NOT NULL,fin BIGINT NOT NULL, fia varchar(20), tia varchar(20), cc varchar(5),con varchar(128),rn varchar(128),cin varchar(128),la DOUBLE,lo DOUBLE,isn varchar(256), ud timestamp, pid varchar CONSTRAINT PK PRIMARY KEY(tin,fin)) SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.network_zone_master(id integer(11) primary key,ipn BIGINT, cu varchar(10), zo varchar(50)) IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create sequence rce.threat_rule_match_id;

create table rce.kafka_offsets(TOPIC varchar not null, PARTITION integer not null, OFFSETS BIGINT, CONSTRAINT PK PRIMARY KEY(TOPIC,PARTITION))  IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.delay_logger (AA varchar(20), AN varchar(200), CN varchar(20), DN varchar(200), DA varchar(20), DV varchar(200), RT TIMESTAMP , ET TIMESTAMP, DM INTEGER (15), LU TIMESTAMP, LM TIMESTAMP, ART TIMESTAMP, FL VARCHAR(20),BH BIGINT CONSTRAINT PK PRIMARY KEY(AA,AN,CN,DN,DA,DV,FL)) IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.batch_offsets(TOPIC varchar not null, PARTITION integer not null, OFFSETS BIGINT, CONSTRAINT PK PRIMARY KEY(TOPIC,PARTITION))  IMMUTABLE_ROWS=true, COMPRESSION='GZ';

-- This is used for Active List project
create table rce.active_list_details(id integer(11) primary key,LT varchar(20),LN varchar(50),ETT varchar(10)) SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ';

-- This is used for log stoppage project
create table rce.log_stopage_master(id integer(11) primary key,c.dg varchar(200),c.sv varchar(200),c.lo varchar(200),c.bf varchar(200),c.bc varchar(200),c.al varchar(200),c.ay varchar(200),c.ls varchar(200),c.et varchar(200),c.cu integer(11),c.te varchar(200),c.cc varchar(200)) SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.log_stopage_audit(id integer(11) primary key,c.mi integer(11),c.et timestamp) SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create table rce.log_stopage_email_alert_audit(id integer(11) primary key,c.mi integer(11),c.yn varchar(100),c.ss varchar(1000),c.et timestamp) SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create sequence rce.log_stopage_audit_id;
create sequence rce.log_stopage_master_id;
create sequence rce.log_stopage_email_alert_audit_id;

-- This is used for raw stat project
create table rce.rawstat( id bigint not null ,lo varchar(200), a.bf varchar(100), a.bc varchar(100), a.al varchar(100), a.ay varchar(100), a.cU integer(11), count bigint ,eT timestamp, lT timestamp CONSTRAINT PK PRIMARY KEY(id)) SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ';

create sequence rce.rawstatseq;

-- This is used for base event count project
create table rce.rce_base_event_count(pi BIGINT not null, mr integer(11) not null, ti BIGINT not null, b.dT varchar[], eT timestamp, id varchar CONSTRAINT PK PRIMARY KEY(pi,mr,ti)) SALT_BUCKETS = 4, IMMUTABLE_ROWS=true,COMPRESSION='GZ';
