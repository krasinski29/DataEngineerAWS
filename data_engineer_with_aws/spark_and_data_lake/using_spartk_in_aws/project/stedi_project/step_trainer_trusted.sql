CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://krasinski-udacity-1/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer landing to trusted', 
  'CreatedByJobRun'='jr_8da42c602d2d8215641244dbed914eb251ab2a614572774c440d75143833138e', 
  'classification'='json')