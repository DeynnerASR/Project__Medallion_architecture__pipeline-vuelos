import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook


def cargar_gold_layer_snowflake(**context):
    # pull filename from previous task; use `task_ids` (plural) in newer Airflow versions
    gold_file = context["ti"].xcom_pull(
        key="gold_file",
        task_ids="agregacion_gold"
    )

    if not gold_file:
        raise ValueError("No se encontró el archivo de la capa gold")

    execution_date = context["data_interval_start"].strftime("%Y-%m-%d")

    df = pd.read_csv(gold_file)

    print(df.head())

    conn = BaseHook.get_connection("flight_snowflake")
    
    sf_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson.get("warehouse"),
        database = conn.extra_dejson.get("database"),
        schema=conn.schema,
        role=conn.extra_dejson.get("role")
    )

    ## Realizare una consulta para agregar los datos
    ## Usando merge, debido a que este me ayuda a evitar duplicados

    ## FLIGHT_KPIS tabla destino

    merge_sql = """
        MERGE INTO FLIGHT_KPIS tgt
        USING (
                SELECT 
                    TO_TIMESTAMP(%s) AS WINDOW_START,
                    %s AS PAIS,
                    %s AS CANTIDAD_DE_VUELOS,
                    %s AS VELOCIDAD_PROMEDIO,
                    %s AS EN_TIERRA
            ) src
        ON tgt.WINDOW_START = src.WINDOW_START 
           AND tgt.PAIS = src.PAIS
        WHEN MATCHED THEN
            UPDATE SET
                CANTIDAD_DE_VUELOS = src.CANTIDAD_DE_VUELOS,
                VELOCIDAD_PROMEDIO = src.VELOCIDAD_PROMEDIO,
                EN_TIERRA = src.EN_TIERRA,
                LOAD_TIME = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (CANTIDAD_DE_VUELOS, EN_TIERRA,LOAD_TIME,PAIS, VELOCIDAD_PROMEDIO,WINDOW_START)
            VALUES (src.CANTIDAD_DE_VUELOS,src.EN_TIERRA,CURRENT_TIMESTAMP(),src.PAIS,src.VELOCIDAD_PROMEDIO,src.WINDOW_START)
    """
 
    ## ejecuto las queries anteriormente instanciadas, usando el dataframe de la capa agregada gold
    with sf_conn.cursor() as cursor:
        for _, row in df.iterrows():
            cursor.execute(merge_sql, (
                execution_date,
                row["origin_country"],
                row["total_vuelos"],
                row["velocidad_media"],
                row["altitud_media"]
            ))

    sf_conn.close()

