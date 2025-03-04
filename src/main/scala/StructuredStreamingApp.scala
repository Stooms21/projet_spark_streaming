import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{count, input_file_name}


// Définition de l'objet principal pour l'application de streaming structuré
object StructuredStreamingApp extends App {
  // Set log level to WARN to reduce verbosity
  val configFilePath = "src/main/resources/application.conf"
  val conf = ConfigFactory.parseFile(new java.io.File(configFilePath))

  val appName = conf.getString("app.name")
  val master = conf.getString("app.master")
  val inputDirectory = conf.getString("app.inputDirectory")
  val outputDirectory = conf.getString("app.outputDirectory")
  val checkpointLocation = conf.getString("app.checkpointLocation")

  // Initialisation de la session Spark avec configuration spécifique
  val spark = SparkSession.builder()
    .appName(appName) // Nom de l'application Spark
    .master(master) // Exécution en mode local, utilisant tous les cœurs disponibles
    .getOrCreate() // Crée une nouvelle session Spark ou récupère une existante

  // Définition du schéma des données pour les fichiers CSV à lire
  val flightDataSchema = StructType(Array(
    StructField("timeline", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true),
    // Définition des champs supplémentaires avec leur type et la possibilité d'être null
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

  // Chemin vers le répertoire contenant les fichiers CSV à lire

  // Configuration de la lecture des fichiers CSV en mode streaming
  val df = spark
    .readStream
    .option("header", "true") // Indique que les fichiers CSV contiennent une ligne d'en-tête
    .option("sep", ",") // Délimiteur utilisé dans les fichiers CSV
    .option("maxFilesPerTrigger", 1) // Nombre de fichiers à lire à chaque déclenchement du streaming
    .schema(flightDataSchema) // Utilisation du schéma défini précédemment pour les données
    .csv(inputDirectory) // Chemin vers les fichiers CSV
    .withColumn("filename", input_file_name()) // Ajout d'une colonne contenant le nom du fichier source

  // Agrégation pour compter les occurrences de chaque aircraft_name
  val aggregatedDF = df.groupBy("aircraft_name").agg(count("aircraft_name").as("count"))

  // Fonction pour écrire chaque micro-batch
  def writeBatch(batchDF: DataFrame, batchId: Long): Unit = {
    batchDF.coalesce(1)
      .write
      .mode("append")
      .csv(s"$outputDirectory/batch-$batchId")
  }

  // Utilisation de foreachBatch pour écrire les résultats agrégés
  val query = aggregatedDF.writeStream
    .outputMode("update") // Utilisez "update" pour écrire les mises à jour incrémentales
    .foreachBatch(writeBatch _)
    .option("checkpointLocation", checkpointLocation) // Chemin pour le checkpoint
    .start()


  query.awaitTermination() // Attend que la requête de streaming se termine
}