# Versioned Time-series Database

This guide will help you to use 

(1) IoTDB/IoTDB-v on version queries

(2) Postgres/Postgres-v (based on PostgreSQL) on translated version queries

(3) InfluxDB on translated version query to FluxQL

(4) TardisDB on version queries

## Prepare Docker/Local Runtime for Each Database

Note that PostgreSQL:latest is not compatible to TardisDB.

(1) IoTDB: Please refer to <https://iotdb.apache.org/Download/> or <https://github.com/apache/iotdb>.
Our experiment chooses default parameters. 
So, you can use released package or build the package from source.
To build from source at root path of downloaded repo, the following command would help.

``mvn clean package -pl distribution -am -DskipTests``

Then, please add the package (library-udf-1.3.1-SNAPSHOT.jar) in ./dependent-package folder to 

``./distribution/target/apache-iotdb-1.3.1-SNAPSHOT-all-bin/apache-iotdb-1.3.1-SNAPSHOT-all-bin/ext/udf/*``

Start standalone server (start-standalone) and open CLI (start-cli) of IoTDB from 

``./distribution/target/apache-iotdb-1.3.1-SNAPSHOT-all-bin/apache-iotdb-1.3.1-SNAPSHOT-all-bin/sbin/``

Register UDF function for branch merge in CLI by

``create function coalesce as 'org.apache.iotdb.library.util.coalesce';``

The above path is based on build-from-source of IoTDB.
If using package, please find the correspondent paths by suffix.

(2) PostgreSQL: Please use a docker to test PostgreSQL.
Follow the command to build a docker container for PostgreSQL.

``docker run --name postgres -e POSTGRES_PASSWORD=postgres -d postgres``

The port number is set by default, and is used in our classes Postgres and PostgresV.java by,

``jdbc:postgresql://localhost:5432/db?user=postgres&password=postgres``

If the port number, user, pwd is changed, please also update them under

``./src/main/java/Postgres->init()``

, and the same for PostgresV.java


(3) InfluxDB: InfluxDB also has an official docker image.
Check this to build a InfluxDB docker <https://hub.docker.com/_/influxdb>.

``docker run --name influxdb -p 8086:8086 influxdb:2.7.4``

Visit localhost and create a user, pwd, and the token at:

``localhost:8086``

The org name and bucket name is important and used in:

``final String org = "org"; final String bucket = "version";``

in this repo:

``./src/main/java/Influx``

It would show a token in a wekpage, please remember to copy it to the field in this repo:

``./src/main/java/Influx -> String token=??``

(4) TardisDB: It relies on LLVM 7, which is not compatible to PostgreSQL.
So, please DO NOT build it by changing your LOCAL environment!
It does not have an official docker image. 

But, you can use the beta feature of docker: Dev Environment.

First, create one container from source code:

<https://github.com/tum-db/TardisDB>

Then, follow their docker file to install env and build.
Its integration is to simply compile the benchmark code along with their repo.
Since no network features is support there, it can not benchmark in distributed nodes. 

## Integration to IoTDB

In IoTDB, we introduce the SQL provided by its language features, corresp. Sec 4.1

In IoTDBV, we show optimization rules-integrated SQL to IoTDB language features for each benchmark queries.

In both benchmark cases, the codes create schemas and register branches itself.

Above the main function, we show simple cases to test, e.g., scalability.
We use Constants.java to change and alter the schemas, attribute names.

If interests, One can open the IoTDB CLI to see the time series by

``show timeseries;``

and count the inserted data points,

``select count(main) from root.Climate.K0;``

taking Climate dataset as an example.

Also notice that the dataset link is set in Constants.java, please change it into your local paths.
For example: ./dataset/iot.climate.csv.

## Integration to existing relational databases


### PostgreSQL:

1. We use maven to control packages. At the pom.xml, please install.

2. Launch an instance in docker: postgres

Execute SQL: database db for baseline Postgres, and database dbv for proposal PostgresV.

``create database db;``

``create database dbv;``


3. See postgres/PostgresV.java in ./src/main/java/

4. It provides the interface to 

(1) connect the docker instance

(2) import the data

(3) benchmark the database through SQLs.

In these SQLs, we show the patterns on

a. Simple relational reducibility translation for baseline Postgres

and,

b. Using version and relational reducibility for proposal PostgresV.

They create with pk for timestamp and new relations for branches and updates.

## InfluxDB and Flux

Different from relational databases and SQL-alike languages, InfluxDB use a different style of writing codes, named Flux.

<https://docs.influxdata.com/influxdb/v2/query-data/flux/>

We show in this repo to fit the version queries into their declaration styles, also based on relational reducibility and use branches as version control.
To see details, we show benchmark queries in 

``./src/main/java/Influx.java``


## Benchmark-related Codes

./src/main/java/:{IoTDB, IoTDBV, Postgres, PostgresV, Influx}

The related services are {Constants, SchemaDescriptor, Schema} and their references

Due to anonymous issue, commits to IoTDB for a new engine will be shown later.