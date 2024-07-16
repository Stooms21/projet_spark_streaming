import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types._

object StructuredStreamingApp extends App {

  val spark = SparkSession.builder()
    .appName("StructuredStreamingApp")
    .master("local[*]")
    .getOrCreate()

  val flightDataSchema = StructType(Array(
    StructField("timeline", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true),
    StructField("id", StringType, true),
    StructField("icao_24bit", StringType, true),
    StructField("heading", IntegerType, true),
    StructField("altitude", IntegerType, true),
    StructField("ground_speed", IntegerType, true),
    StructField("aircraft_code", StringType, true),
    StructField("registration", StringType, true),
    StructField("time", LongType, true),
    StructField("origin_airport_iata", StringType, true),
    StructField("destination_airport_iata", StringType, true),
    StructField("number", StringType, true),
    StructField("airline_iata", StringType, true),
    StructField("on_ground", IntegerType, true),
    StructField("vertical_speed", IntegerType, true),
    StructField("callsign", StringType, true),
    StructField("airline_icao", StringType, true),
    StructField("origin_airport_name", StringType, true),
    StructField("origin_airport_coordinates", StringType, true),
    StructField("destination_airport_name", StringType, true),
    StructField("destination_airport_coordinates", StringType, true),
    StructField("airline_name", StringType, true),
    StructField("aircraft_name", StringType, true),
    StructField("manufacturer", StringType, true)
  ))

  // Lecture des fichiers CSV en continu
  val inputDirectory = "C:/Users/Stooms/IdeaProjects/sparkStreaming/data"

  val df = spark
    .readStream
    .option("header", "true") // Supposer que les fichiers CSV ont une ligne d'en-tête
    .option("sep", ",")
    .option("maxFilesPerTrigger", 1)
    .schema(flightDataSchema) // Définir le schéma des données
    .csv(inputDirectory)
    .withColumn("filename", input_file_name())

  // Traitement simple : afficher le nombre de lignes par fichier
  val query = df
    .groupBy("aircraft_name")
    .count()
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()
}