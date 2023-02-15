import sys
import redshift_connector

conn = redshift_connector.connect(host='sunny-redshift.czyamnxhlqou.us-east-2.redshift.amazonaws.com', port='5439',
                                  database='testload', user='awsuser', password='xxxxxx')
conn.autocommit = True
cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute("""
CREATE TABLE "factCovid" (
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" INTEGER,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
)
""")

cursor.execute("""
CREATE TABLE "dimRegion" (
 "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "dimHospital" (
  "fips" INTEGER,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "dimDate" (
  "fips" INTEGER,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")


cursor.execute("""
               COPY testload.public.dimdate (fips,date,year,month,day_of_week) 
               FROM 's3://sunny-general-bucket/output/dimDate.csv' 
               IAM_ROLE 'arn:aws:iam::724295183885:role/service-role/AmazonRedshift-CommandsAccessRole-20230212T180621' 
               FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 DATEFORMAT 'yyyy-mm-dd' REGION AS 'us-east-2'
               """)

cursor.execute("""
               COPY testload.public.dimregion
               FROM 's3://sunny-general-bucket/output/dimRegion.csv' 
               IAM_ROLE 'arn:aws:iam::724295183885:role/service-role/AmazonRedshift-CommandsAccessRole-20230212T180621' 
               FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 DATEFORMAT 'yyyy-mm-dd' REGION AS 'us-east-2'
               """)

cursor.execute("""
               COPY testload.public.dimHospital
               FROM 's3://sunny-general-bucket/output/dimHospital.csv' 
               IAM_ROLE 'arn:aws:iam::724295183885:role/service-role/AmazonRedshift-CommandsAccessRole-20230212T180621' 
               FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 DATEFORMAT 'yyyy-mm-dd' REGION AS 'us-east-2'
               """)
               
cursor.execute("""
               COPY testload.public.factcovid
               FROM 's3://sunny-general-bucket/output/factCovid.csv' 
               IAM_ROLE 'arn:aws:iam::724295183885:role/service-role/AmazonRedshift-CommandsAccessRole-20230212T180621' 
               FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 DATEFORMAT 'yyyy-mm-dd' REGION AS 'us-east-2'
               """)               

