import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types._

// Définition de l'objet principal pour l'application de streaming structuré
object StructuredStreamingApp extends App {

  // Initialisation de la session Spark avec configuration spécifique
  val spark = SparkSession.builder()
    .appName("StructuredStreamingApp") // Nom de l'application Spark
    .master("local[*]") // Exécution en mode local, utilisant tous les cœurs disponibles
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
  val inputDirectory = "C:/Users/Stooms/IdeaProjects/sparkStreaming/data"

  // Configuration de la lecture des fichiers CSV en mode streaming
  val df = spark
    .readStream
    .option("header", "true") // Indique que les fichiers CSV contiennent une ligne d'en-tête
    .option("sep", ",") // Délimiteur utilisé dans les fichiers CSV
    .option("maxFilesPerTrigger", 1) // Nombre de fichiers à lire à chaque déclenchement du streaming
    .schema(flightDataSchema) // Utilisation du schéma défini précédemment pour les données
    .csv(inputDirectory) // Chemin vers les fichiers CSV
    .withColumn("filename", input_file_name()) // Ajout d'une colonne contenant le nom du fichier source

  // Configuration de la requête de streaming pour le traitement des données
  val query = df
    .groupBy("aircraft_name") // Groupe les données par nom d'avion
    .count() // Compte le nombre d'occurrences pour chaque groupe
    .writeStream // Préparation de l'écriture du résultat du streaming
    .outputMode("complete") // Mode de sortie complet pour afficher tous les comptes à chaque déclenchement
    .format("console") // Affichage du résultat dans la console
    .start() // Démarre la requête de streaming

  query.awaitTermination() // Attend que la requête de streaming se termine
}