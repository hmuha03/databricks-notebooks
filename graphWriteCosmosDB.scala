// Databricks notebook source
// MAGIC %md # Writing GraphFrames to Azure Cosmos DB Gremlin API
// MAGIC This notebook is based on the `GraphFrames` example [specified here](https://graphframes.github.io/user-guide.html#tab_scala_0). It requires [graphframes](https://spark-packages.org/package/graphframes/graphframes) and [azure-cosmosdb-spark (uber jar)](https://github.com/Azure/azure-cosmosdb-spark#using-databricks-notebooks) libraries to be uploaded and attached to the cluster. **Python version** of this notebook can be [found here](https://github.com/syedhassaanahmed/databricks-notebooks/blob/master/graph_write_cosmosdb.py)
// spark-shell --jars graphframes-0.7.0-spark2.4-s_2.11.jar,azure-cosmosdb-spark_2.4.0_2.11-1.3.5-uber.jar
// COMMAND ----------

import org.apache.spark.sql.functions.lit

val display = (value: org.apache.spark.sql.DataFrame) => {
   value.show()
}

val v = spark.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
)).toDF("id", "name", "age").withColumn("entity", lit("person"))

// COMMAND ----------

val e = spark.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
)).toDF("src", "dst", "relationship")

// COMMAND ----------

import org.graphframes.GraphFrame
val g = GraphFrame(v, e)

// COMMAND ----------

display(g.vertices)

// COMMAND ----------

display(g.edges)

// COMMAND ----------

// MAGIC %md ## Convert Vertices and Edges to Cosmos DB internal format
// MAGIC Cosmos DB Gremlin API internally keeps a JSON document representation of Edges and Vertices [as explained here](https://github.com/LuisBosquez/azure-cosmos-db-graph-working-guides/blob/master/graph-backend-json.md). Also `id` in Cosmos DB is [part of the resource URI](https://github.com/Azure/azure-cosmosdb-dotnet/issues/35#issuecomment-121009258) and hence must be URL encoded.

// COMMAND ----------

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.types.StringType

val urlEncode = (value: String) => {
  URLEncoder.encode(value, StandardCharsets.UTF_8.toString).replaceAll("\\+", "%20")
}

val udfUrlEncode = udf(urlEncode, StringType)

def md5Hash(text: String) : String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}

val udfMd5Hash = udf(md5Hash _, StringType)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer

def toCosmosDBVertices(dfVertices: DataFrame, labelColumn: String, partitionKey: String = "") : DataFrame = {
  val dfResult = dfVertices
        .withColumn("id", udfUrlEncode($"id")).withColumn("pk", udfMd5Hash($"id"))
   
  //if (!partitionKey.isEmpty()) {
      // dfResult = dfVertices
           
        
  //}
  
  var columns = ListBuffer("id", labelColumn)
  
  if (!partitionKey.isEmpty()) {
    columns += "pk"
  }
  
  columns ++= dfResult.columns.filterNot(columns.contains(_))
    .map(x => s"""nvl2($x, array(named_struct("id", uuid(), "_value", $x)), NULL) AS $x""")
  
  dfResult.selectExpr(columns:_*).withColumnRenamed(labelColumn, "label")
}

// COMMAND ----------

val cosmosDbVertices = toCosmosDBVertices(g.vertices, "entity")
display(cosmosDbVertices)

// COMMAND ----------

import org.apache.spark.sql.functions.{concat_ws, col}

def toCosmosDBEdges(g: GraphFrame, labelColumn: String, partitionKey: String = "") : DataFrame = {
  var dfEdges = g.edges
  
  if (!partitionKey.isEmpty()) {
    dfEdges = dfEdges.alias("e")
      .join(g.vertices.alias("sv"), $"e.src" === $"sv.id")
      .join(g.vertices.alias("dv"), $"e.dst" === $"dv.id")
      .selectExpr("e.*", "sv." + partitionKey, "dv." + partitionKey + " AS _sinkPartition")
  }
  
  dfEdges = dfEdges
    .withColumn("id", udfUrlEncode(concat_ws("_", $"src", col(labelColumn), $"dst")))
    .withColumn("_isEdge", lit(true))
    .withColumn("_vertexId", udfUrlEncode($"src"))
    .withColumn("_sink", udfUrlEncode($"dst"))
    .withColumnRenamed(labelColumn, "label")
    .drop("src", "dst")
  
  dfEdges
}

// COMMAND ----------

val cosmosDbEdges = toCosmosDBEdges(g, "relationship")
display(cosmosDbEdges)

// COMMAND ----------

import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.sql.SaveMode

val cosmosDBConfig = Config(Map(
  "Endpoint" -> "https://<COSMOSDB_ENDPOINT>.documents.azure.com:443/",
  "Masterkey" -> "<COSMOSDB_PRIMARYKEY>",
  "Database" -> "<DATABASE>",
  "Collection" -> "<COLLECTION>"
))

cosmosDbVertices.write.mode(SaveMode.Overwrite).cosmosDB(cosmosDBConfig)
cosmosDbEdges.write.mode(SaveMode.Overwrite).cosmosDB(cosmosDBConfig)

