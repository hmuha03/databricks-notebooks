// Databricks notebook source
// MAGIC %md # Writing GraphFrames to Azure Cosmos DB Gremlin API
// MAGIC This notebook is based on the `GraphFrames` example [specified here](https://graphframes.github.io/user-guide.html#tab_scala_0). It requires [graphframes](https://spark-packages.org/package/graphframes/graphframes) and [azure-cosmosdb-spark (uber jar)](https://github.com/Azure/azure-cosmosdb-spark#using-databricks-notebooks) libraries to be uploaded and attached to the cluster. **Python version** of this notebook can be [found here](https://github.com/syedhassaanahmed/databricks-notebooks/blob/master/graph_write_cosmosdb.py)
// spark-shell --jars graphframes-0.7.0-spark2.4-s_2.11.jar,azure-cosmosdb-spark_2.4.0_2.11-1.3.5-uber.jar
// COMMAND ----------

import org.apache.spark.sql.functions.lit

val display = (value: org.apache.spark.sql.DataFrame) => {
   value.show()
}

val ing = spark.createDataFrame(List(
  ("heavy cream", "dairy and eggs"),
  ("Sugar", "Bob"),
  ("Salt pepper", "Charlie"),
  ("chicken", "David"),
  ("beef piece", "Esther"),
  ("fish", "Fanny"),
  ("garlic souce", "Gabby")
)).toDF("id", "category").withColumn("entity", lit("ingredient"))

val rec = spark.createDataFrame(List(
  ("0_fizzy-strawberry-mocktail", "dairy and eggs"),
  ("0_shrimp-and-black-bean-ceviche", "Bob"),
  ("0_creamy-coconut-baked-oats", "Charlie"),
  ("0_grilled-flatbread-pizzas", "David"),
  ("0_cheesy-chicken-enchiladas", "Esther"),
  ("0_grilled-chickpea--eggplant---tomato-flatbread", "Fanny"),
  ("0_winter-salad-with-granny-smith-apple-vinaigrette", "Gabby")
)).toDF("id", "category").withColumn("entity", lit("recipe"))

val v = rec.union(ing)

// COMMAND ----------

val e = spark.createDataFrame(List(
  ("heavy cream", "0_fizzy-strawberry-mocktail", "belongsTo"),
  ("Sugar", "0_shrimp-and-black-bean-ceviche", "belongsTo"),
  ("Salt pepper", "0_creamy-coconut-baked-oats", "belongsTo"),
  ("chicken", "0_grilled-flatbread-pizzas", "belongsTo"),
  ("beef", "0_grilled-chickpea--eggplant---tomato-flatbread", "belongsTo"),
  ("fish", "0_cheesy-chicken-enchiladas", "belongsTo"),
  ("garlic souce", "0_cheesy-chicken-enchiladas", "belongsTo"),
  ("garlic souce", "0_grilled-chickpea--eggplant---tomato-flatbread", "belongsTo")
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
  var dfResult = dfVertices
        .withColumn("id", udfUrlEncode($"id"))
   
  if (!partitionKey.isEmpty()) {
      dfResult = dfResult.withColumn(partitionKey, udfMd5Hash($"id"))
  }
  
  var columns = ListBuffer("id", labelColumn)
  
  if (!partitionKey.isEmpty()) {
    columns += partitionKey
  }
  
  columns ++= dfResult.columns.filterNot(columns.contains(_))
    .map(x => s"""nvl2($x, array(named_struct("id", uuid(), "_value", $x)), NULL) AS $x""")
  
  dfResult.selectExpr(columns:_*).withColumnRenamed(labelColumn, "label")
}

// COMMAND ----------

val cosmosDbVertices = toCosmosDBVertices(g.vertices, "entity", "pk")
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



