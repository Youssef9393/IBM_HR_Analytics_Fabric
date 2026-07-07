# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1119bcc4-07e1-4d34-a9b0-98ae777dced9",
# META       "default_lakehouse_name": "LH_Silver_HR",
# META       "default_lakehouse_workspace_id": "a86295e8-77ac-4e51-af33-ae30f9373e80",
# META       "known_lakehouses": [
# META         {
# META           "id": "1119bcc4-07e1-4d34-a9b0-98ae777dced9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

""" 
    * I have data stored across multiple Delta tables in a Bronze Lakehouse.
    * I create activity  pipeline that reads data from the Bronze Lakehouse and copies it into the Silver Lakehouse.
    * Create a table if the table does not already exist.
    * Add new rows if the table already exists.

from notebookutils import mssparkutils

for f in mssparkutils.fs.ls("Tables"):
    print(f.name)
    print(f.path)  
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
import notebookutils

# ===========================
# Lakehouse Paths
# ===========================
bronze_path = "abfss://a86295e8-77ac-4e51-af33-ae30f9373e80@onelake.dfs.fabric.microsoft.com/a5cb1905-9dc1-4184-bbd0-90939a31d0f4/Tables/dbo"
silver_path = "abfss://a86295e8-77ac-4e51-af33-ae30f9373e80@onelake.dfs.fabric.microsoft.com/1119bcc4-07e1-4d34-a9b0-98ae777dced9/Tables/dbo"

# ===========================
# Liste des tables Bronze
# ===========================

tables = notebookutils.fs.ls(bronze_path)

for item in tables:

    if not item.isDir:
        continue

    table_name = item.name.rstrip("/")

    print("=" * 60)
    print(f"Traitement de la table : {table_name}")

    source = f"{bronze_path}/{table_name}"
    target = f"{silver_path}/{table_name}"

    try:

        # Lecture de la table Bronze
        df = spark.read.format("delta").load(source)

        # Vérifier si la table Silver existe
        if not DeltaTable.isDeltaTable(spark, target):

            print("Création de la table Silver...")

            (
                df.write
                  .format("delta")
                  .mode("overwrite")
                  .save(target)
            )

            print("Table créée avec succès.")

        else:

            print("Table déjà existante.")
            print("MERGE des nouvelles données...")

            deltaTable = DeltaTable.forPath(spark, target)

            (
                deltaTable.alias("target")
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            print("Synchronisation terminée.")

    except Exception as e:

        print(f"Erreur sur la table {table_name}")
        print(e)

print("Toutes les tables ont été traitées.")

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
