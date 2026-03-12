# TUBERIA ETL con MEDALLION ARCHITECTURE
El siguiente proyecto realiza una implementación Batch ETL basíca, usando la api open source de OpenSky Network. Esta nos permite ver el trafico aereo de varios paises en tiempo real.
Su proceso consiste en realizar solicitudes a la API dentro de un tiempo definido (orchestada con AIRFLOW), guardar esta información por capas (medallion architecture); extraer los datos que necesitamos, limpiarlos, y transformarlos para que se puedan cargar dentro de un Data Warehouse (en este caso Snowflake). Finalmente se utiliza una herramienta BI open source llamada Looker Studio con el objetivo de mostrar los resultados dependiendo de nuestras necesidades.


## DIAGRAMA

![data_architecture_diagram](/documentation/Proyecto%20Vuelos.drawio.png)


## TECNOLOGIAS
- Docker 
- Python Pandas
- Apache Airflow
- Snowflake connector
- Looker Studio
- OpenSky Network API
- Medallion Architecture

## INICIALIZACIÓN
1. Clona el repositorio:
    `git clone https://github.com/DeynnerASR/-Project-Medallion_architecture_pipeline-vuelos.git`

2. Crea el archivo .env similar a las siguientes credenciales:
    ```.env
        POSTGRES_USER=usuario
        POSTGRES_PASSWORD=contraseña
        POSTGRES_DB=base_datos

        AIRFLOW_ADMIN_USER=usuario
        AIRFLOW_ADMIN_FIRSTNAME=nombre
        AIRFLOW_ADMIN_LASTNAME=apellido
        AIRFLOW_ADMIN_EMAIL=correo@example.com
        AIRFLOW_ADMIN_PASSWORD=contraseña
        AIRFLOW_UID=50000

        SNOWFLAKE_CONNECTION_ID=nombre
        CONNECTION_TYPE=Snowflake
        SCHEMA=nombre_schema
        SNOWFLAKE_USER=nombre_usuario_snowflake
        SNOWFLAKE_PASSWORD=contraseña_usuario_snowflake
        ACCOUNT=codigo_snowflake_contiene_region
        WAREHOUSE=COMPUTE_WH
        SNOWFLAKE_DATABASE=nombre_db_snowflake
        ROLE=ACCOUNTADMIN
    ```

3. Antes de iniciar, procura limpiar volumenes existentes:
    `docker compose down -v`
    
4. Inicia el contenedor:
    `docker compose up -d --build`

5. Espera unos cuantos minutos. Y una vez iniciado, ve a la siguiente direccion:
    http://localhost:8085/

6. Logeate con las credenciales, en el airflow webser.

7. Activa el DAG y ejecutalo
