# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a5cb1905-9dc1-4184-bbd0-90939a31d0f4",
# META       "default_lakehouse_name": "LH_Bronze_HR",
# META       "default_lakehouse_workspace_id": "a86295e8-77ac-4e51-af33-ae30f9373e80",
# META       "known_lakehouses": [
# META         {
# META           "id": "a5cb1905-9dc1-4184-bbd0-90939a31d0f4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

"""
  - This nootebooks is for create Delta Table.
  - by using csv That in LH_Bronze_HR 

    from notebookutils import mssparkutils

    for f in mssparkutils.fs.ls("Files"):
      print(f.name)
      print(f.path)
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv 
import pandas as pd 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from notebookutils import mssparkutils
import os

source_folder = "abfss://a86295e8-77ac-4e51-af33-ae30f9373e80@onelake.dfs.fabric.microsoft.com/a5cb1905-9dc1-4184-bbd0-90939a31d0f4/Files/"

# Lister tous les fichiers
files = mssparkutils.fs.ls(source_folder)

for file in files:

    # Vérifier que c'est un CSV
    if file.name.endswith(".csv"):

        # Nom de la table = nom du fichier sans .csv
        table_name = os.path.splitext(file.name)[0]

        file_path = file.path

        print(f"Lecture : {file.name}")

        # Lire le CSV
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(file_path)
        )

        print("Colonnes :", df.columns)

        # Créer ou remplacer la table Delta
        (
            df.write
            .mode("overwrite")
            .format("delta")
            .saveAsTable(table_name)
        )

        print(f"Table créée : {table_name}")

print("Toutes les tables ont été créées avec succès.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
