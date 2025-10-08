# actividades_ocio_pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import utilsJoaquim as utils
from typing import List, Optional


def process_actividades_selected(
    spark: Optional[SparkSession] = None,
    db_name: str = "landing",
    table_name: str = "actividades_Ocio",
    provinces: Optional[List[str]] = None,
    tgt_db: str = "trusted",
    tgt_tbl: str = "actividades_Ocio_selected",
    show_rows: int = 0,
):
    """
    Procesa la tabla landing.actividades_Ocio: normaliza, limpia, filtra provincias y escribe en trusted.

    Args:
      spark: SparkSession opcional. Si no se pasa, se crea con utils.create_context().
      db_name, table_name: tabla de origen en Iceberg (landing).
      provinces: lista de provincias a seleccionar. Si None, usa una lista por defecto.
      tgt_db, tgt_tbl: destino Iceberg (trusted).
      show_rows: si >0, hace df_filtered.show(show_rows) para debug.

    Returns:
      DataFrame guardado (df_filtered).
    """
    created_spark = False
    if provinces is None:
        provinces = ["Madrid", "Barcelona", "Sevilla", "Illes Balears", "Valencia"]

    if spark is None:
        spark = utils.create_context()
        created_spark = True
        spark.sparkContext.setLogLevel("ERROR")

    try:
        print(f"→ Reading spark_catalog.{db_name}.{table_name}")
        df = utils.read_iceberg_table(spark, db_name, table_name)

        # Normalize province names
        df_norm = df.withColumn(
            "PROVINCIA",
            when(col("PROVINCIA") == "Valencia/València", "Valencia").otherwise(col("PROVINCIA")),
        )

        # Cleaned dataset (drop nulls)
        df_clean = df_norm.dropna()

        # Data quality report (lightweight; keep in logs)
        print("\n=== Province Data Quality Report ===")
        for prov in provinces:
            df_prov = df_norm.filter(col("PROVINCIA") == prov)
            count_total = df_prov.count()

            df_clean_prov = df_clean.filter(col("PROVINCIA") == prov)
            count_clean = df_clean_prov.count()

            count_nulls = count_total - count_clean

            if count_clean > 0:
                first_row = df_clean_prov.select("AÑO", "MES").orderBy("AÑO", "MES").first()
                last_row = df_clean_prov.select("AÑO", "MES").orderBy(col("AÑO").desc(), col("MES").desc()).first()

                first_year, first_month = first_row["AÑO"], int(first_row["MES"])
                last_year, last_month = last_row["AÑO"], int(last_row["MES"])

                total_months_expected = (last_year - first_year) * 12 + (last_month - first_month + 1)
                months_missing = total_months_expected - count_clean

                print(f"✅ {prov}")
                print(f"   - Total rows before cleaning: {count_total}")
                print(f"   - Rows dropped (nulls): {count_nulls}")
                print(f"   - Rows after cleaning: {count_clean}")
                print(f"   - First instance → AÑO={first_year}, MES={first_month:02d}")
                print(f"   - Last instance  → AÑO={last_year}, MES={last_month:02d}")
                print(f"   - Missing months in series: {months_missing}\n")
            else:
                print(f"⚠️ {prov}: No data found\n")

        # Filter and write
        df_filtered = df_clean.filter(col("PROVINCIA").isin(provinces))
        if show_rows > 0:
            df_filtered.show(show_rows)

        print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
        utils.overwrite_iceberg_table(spark, df_filtered, tgt_db, tgt_tbl)

        print("✅ Trusted load complete.")


    finally:
        if created_spark:
            try:
                spark.stop()
            except Exception:
                pass


if __name__ == "__main__":
    # Ejecución local para debugging
    process_actividades_selected(show_rows=10)
