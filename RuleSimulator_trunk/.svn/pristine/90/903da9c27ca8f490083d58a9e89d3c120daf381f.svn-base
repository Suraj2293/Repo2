-- --------------------------------------------------------
--
-- Table structure for table `simulator_threat_raw`
--
DROP TABLE IF EXISTS rce.simulator_threat_raw;
CREATE TABLE IF NOT EXISTS rce.simulator_threat_raw(
eT timestamp not null, //'event time'
ti BIGINT not null, //'unique threat id'
b.dT varchar, //'string of data array'
lo varchar(200), //'customer name'
a.bf varchar(30), //'device vendor'
a.bc varchar(40), //'device product'
a.al varchar(20), //'device address'
a.ay varchar(70), //'device host name'
a.cU integer(11), //'tenant-id'
id varchar CONSTRAINT PK PRIMARY KEY(eT,ti)//'composite key of et and ti'
) SALT_BUCKETS = 4, IMMUTABLE_ROWS=true, COMPRESSION='GZ' /*'table where all the raw logs are stored'*/;

-- --------------------------------------------------------
--
-- Table structure for table `simulator_partial_rule_match`
--
DROP TABLE IF EXISTS rce.simulator_partial_rule_match;
CREATE TABLE IF NOT EXISTS rce.simulator_partial_rule_match(
et timestamp not null,//'event time' 
ct varchar(50) not null,//'customer'
mr integer(4) not null,//'matched rule id'
ti BIGINT not null,//'threat id'
mp BIGINT,//'matched parent id'
aa varchar(255),//'application protocol'
ab integer(5),//'base event count'
ac integer(6),//'bytes in'
ad integer(6),//'bytes out'
a.ae varchar(20),//'destination address'
af varchar(255),//'destination host name'
a.ag integer(6),//'destination port'
ah varchar(255),//'destination process name'
a.ai varchar(255),//'destination service name'
a.aj varchar(255),//'destination user name'
a.ak varchar(255),//'device action'
a.al varchar(20),//'device address'
am BIGINT,//'device custom number 1'
an BIGINT,//'device custom number 2'
ao BIGINT,//'device custom number 3'
ap varchar(255),//'device custom string 1' 
aq varchar(255),//'device custom string 2'
ar varchar(255),//'device custom string 3'
da varchar(255),//'device custom string 4'
at varchar(255),//'device custom string 5'
au varchar(255),//'device custom string 6'
av varchar(255),//'device direction'
aw varchar(255),//'device event category'
a.ax varchar(255),//'device event class id' 
a.ay varchar(255),//'device host name' 
az varchar(255),//'device inbound interface' 
ba varchar(255),//'device outbound interface' 
bb varchar(255),//'device process name'
a.bc varchar(255),//'device product'
bd timestamp,//'device receipt time'
be varchar(255),//'device severity' 
a.bf varchar(255),//'device vendor'
a.bg timestamp,//'endTime'
bh BIGINT,//'eventId'
bi varchar(255),//'external id' 
bj varchar(255),//'file name'
bk varchar(255),//'file path' 
bl varchar(255),//'flex string 1' 
a.bm varchar(1000),//'message' 
a.bn varchar(1000),//'name'
bo varchar(255),//'reason'
bp varchar(255),//'request client application'
bq varchar(255),//'request context'
br varchar(255),//'request cookie'
bs varchar(255),//'request cookies'
bt varchar(255),//'request method'
bu varchar(255),//'request url'
bv varchar(255),//'request url file name'
a.bw varchar(255),//'source address'
a.bx varchar(255),//'source host name'
db varchar(255),//'source nt domain'
dc integer(6),//'source port'
a.bz varchar(255),//'source user name'
ca timestamp,//'start time'
cb varchar(255),//'transport protocol' 
cg varchar(255),//'category behavior' 
ch varchar(255),//'category outcome' 
ci varchar(255),//'customer URI'
cp varchar(255),//'source geo city name'
cq varchar(255),//'source geo country name'
cr varchar(255),//'destination geo city name'
cs varchar(255),//'destination geo country name'
sz varchar(255),//'source zone'
dz varchar(255),//'destination zone'
a.cu varchar(50),//'customer name' 
cd varchar(255),//''
a.dT varchar[],//' data array'
id varchar CONSTRAINT PK PRIMARY KEY(et,ct,mr,ti)//'composite key with et,ct,mr,ti'
) IMMUTABLE_ROWS=true,COMPRESSION='GZ'/*'table where all the partially matched rules are stored' */;

-- --------------------------------------------------------
--
-- Table structure for table `simulator_threat_rule_match`
--
DROP TABLE IF EXISTS rce.simulator_threat_rule_match;
CREATE TABLE IF NOT EXISTS rce.simulator_threat_rule_match(
id integer(11) primary key,//'the auto-incremental record id -PRIMARY KEY' 
ti BIGINT,//'threat id'
mr integer(11),//' matched rule id' 
et timestamp,//' event time'
b.dT varchar,//'data array'
cu varchar(200),//'customer id'
bh BIGINT,//' event id' 
bd timestamp//'device receipt time'
) SALT_BUCKETS = 4,IMMUTABLE_ROWS=true, COMPRESSION='GZ'/*'table where all the matched rules are stored'*/;

-- --------------------------------------------------------
--
-- Table structure for table `simulator_threat_rule_parent_child_mapping`
--
DROP TABLE IF EXISTS rce.simulator_threat_rule_parent_child_mapping;
CREATE TABLE IF NOT EXISTS rce.simulator_threat_rule_parent_child_mapping(
ti BIGINT not null,//'threat id' 
mr integer(11) not null,//'matched rule' 
dt BIGINT[],//'data array'
tn varchar(200),//'threat raw table name' 
eT timestamp,//'event id'
tt varchar(20),//'time in minutes'
rr varchar(20),//'rule type - stream rule or batch rule'
fl integer(11),//'flag - 0 or 1 or null'
id varchar CONSTRAINT PK PRIMARY KEY(ti,mr)//'composite key with ti,mr'
) SALT_BUCKETS = 4,IMMUTABLE_ROWS=true,COMPRESSION='GZ'/*'table where all the parent and child rules are stored'*/;

-- --------------------------------------------------------
--
-- Table structure for table `rule_simulation_details`
--
DROP TABLE IF EXISTS rce.rule_simulation_details;
CREATE TABLE IF NOT EXISTS rce.rule_simulation_details(
rec_id integer(11) not null PRIMARY KEY,//'the auto-incremental record id - PRIMARY KEY'
rule_id integer(11),//'id provided for the rule'
rule_name varchar(200),//'name of the rule'
ingestion_start_time timestamp,//'ingestion start time'
ingestion_end_time timestamp,//'ingestion end time'
customer_id integer(11),//'customer id'
device_product varchar(200),//'name of the device product'
device_vendor varchar(200),//'name of the device vendor'
created_date timestamp,//'entry creation time on phoenix'
flag varchar(10)//'flag value - NULL or Y or N'
)IMMUTABLE_ROWS=true,COMPRESSION='GZ'/*'table where all the rule simulation details are stored'*/;

-- --------------------------------------------------------
--
-- Sequence structure for table `simulator_threat_rule_match`
--
DROP SEQUENCE IF EXISTS rce.simulator_threat_rule_match_id;
CREATE SEQUENCE IF NOT EXISTS rce.simulator_threat_rule_match_id;

-- --------------------------------------------------------
--
-- Sequence structure for table `rule_simulation_details`
--
DROP SEQUENCE IF EXISTS rce.rule_simulation_details_id;
CREATE SEQUENCE IF NOT EXISTS rce.rule_simulation_details_id;

