
REGISTER hdfs://head02.hathi.surfsara.nl/jars/*.jar;

register hdfs://head02.hathi.surfsara.nl/user/naward06/lib/*.jar; 

register hdfs://head02.hathi.surfsara.nl/user/naward06/dist/lib/commoncrawl-examples-1.0.1-HM.jar;

register hdfs://head02.hathi.surfsara.nl/user/naward06/trunk/myudfs.jar;

REGISTER 'hdfs://head02.hathi.surfsara.nl/jars/warcutils.jar';

DEFINE WarcFileLoader nl.surfsara.warcutils.pig.WarcSequenceFileLoader();

SET default_parallel 25;

data = LOAD 'hdfs://head02.hathi.surfsara.nl/data/public/common-crawl/crawl-data/CC-MAIN-2014-10/*/13945*/seq/*' USING WarcFileLoader AS (url, length, type);

countrylist = LOAD 'hdfs://head02.hathi.surfsara.nl/user/naward06/GDPrank' USING PigStorage(':') AS (rank:int, name:chararray);
--X = FOREACH data GENERATE html;

Z = FILTER data BY TRIM(LOWER(type)) == 'text/html';

--Z = FILTER X BY html IS NOT NULL;

P = FOREACH Z GENERATE myudfs.CountryUDF(url) AS present;

P2 = FILTER P BY present IS NOT NULL;
P3 = FOREACH P2 GENERATE FLATTEN(present) AS country;
P4 = GROUP P3 BY country;
P5 = FOREACH P4 GENERATE group, COUNT(P3) AS counting;

P6 = JOIN countrylist BY name, P5 BY group;
result = FOREACH P6 GENERATE rank, name, counting;
orderedResult = ORDER result BY rank;

STORE orderedResult INTO 'resultScriptfianl3';

