CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_curated` (
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` bigint,
  `x` float,
  `y` float,
  `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://krasinski-udacity-1/step_trainer/curated/'
TBLPROPERTIES ('classification' = 'json');