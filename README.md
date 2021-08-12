# BigConnect Connector for Apache Spark

This repository contains the BigConnect Connector for Apache Spark 2.4+ and v3.

## Building

Prerequisites:

- JDK 1.8 is required

### Building for Spark 2.4

You can build for Spark 2.4 with both Scala 2.11 and Scala 2.12

```
./mvnw clean package -P spark-2.4 -P scala-2.11 -DskipTests
./mvnw clean package -P spark-2.4 -P scala-2.12 -DskipTests
```

These commands will generate the corresponding targets
* `spark-2.4/target/bc-connector-apache-spark_2.11-4.3.0_for_spark_2.4.jar`
* `spark-2.4/target/bc-connector-apache-spark_2.12-4.3.0_for_spark_2.4.jar`


### Building for Spark 3

You can build for Spark 3 by running

```
./mvnw clean package -P spark-3 -P scala-2.12 -DskipTests
```

This will generate `spark-3/target/bc-connector-apache-spark_2.12-4.3.0_for_spark_3.jar`


## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars bc-connector-apache-spark_2.11-4.3.0_for_spark_2.4.jar`

For Spark 3 add log4j-api and log4j-core 2.11.1 to SPARK_HOME/jars folder 
`$SPARK_HOME/bin/spark-shell --jars bc-connector-apache-spark_2.12-4.3.0_for_spark_3.jar`
`$SPARK_HOME/bin/spark-shell --packages io.bigconnect:bc-connector-apache-spark_2.12:4.3.0_for_spark_3`

**sbt**

If you use the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package), in your sbt build file, add:

```scala spDependencies += "io.bigconnect/bc-connector-apache-spark_2.11:4.3.0_for_spark_2.4"```

Otherwise,

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "io.bigconnect" % "bc-connector-apache-spark_2.12" % "4.3.0_for_spark_2.4"
```

Or, for Spark 3

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "io.bigconnect" % "bc-connector-apache-spark_2.12" % "4.3.0_for_spark_3"
```  

**maven**  
In your pom.xml, add:   

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>io.bigconnect</groupId>
    <artifactId>bc-connector-apache-spark_2.11</artifactId>
    <version>4.2.1_for_spark_2.4</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```

In case of Spark 3

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>io.bigconnect</groupId>
    <artifactId>bc-connector-apache-spark_2.12</artifactId>
    <version>4.2.1_for_spark_3</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```
