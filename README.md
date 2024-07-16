# Remarques
J'ai dû changer la version de hadoop à cause de windows
Dans le fichier build.sbt, modifier la version de hadoop qui correspond à votre version
```
"org.apache.hadoop" % "hadoop-client" % "3.3.6"
```
WARNING : Prenez l'ensemble du projet de ce git

Avant de faire tourner le stream, il faut générer les fichiers csv des vols découpés selon leur timeline
Pour cela exécutez le fichier python cut_csv.py 
Ce programme vous demandera de mettre en input le chemin vers le fichier "flights_final.csv"
Une fois généré, vous pouvez lancer le stream
