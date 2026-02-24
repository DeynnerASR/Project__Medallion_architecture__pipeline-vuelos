# TUBERIA ETL con MEDALLION ARCHITECTURE


## DIAGRAMA


## TECNOLOGIAS


## INICIALIZACIÓN
1. Clona el repositorio:
```git
    `git clone https://github.com/DeynnerASR/-Project-Medallion_architecture_pipeline-vuelos.git`

2. Crea el archivo .env similar a las siguientes credenciales:
    `
        POSTGRES_USER=usuario
        POSTGRES_PASSWORD=contraseña
        POSTGRES_DB=base_datos

        AIRFLOW_ADMIN_USER=usuario
        AIRFLOW_ADMIN_FIRSTNAME=nombre
        AIRFLOW_ADMIN_LASTNAME=apellido
        AIRFLOW_ADMIN_EMAIL=correo@example.com
        AIRFLOW_ADMIN_PASSWORD=contraseña
        AIRFLOW_UID=50000
    `

3. Antes de iniciar, procura limpiar volumenes existentes:
    `docker compose down -v`
    
4. Inicia el contenedor:
    `docker compose up -d --build`

5. Espera unos cuantos minutos. Y una vez iniciado, ve a la siguiente direccion:
    http://localhost:8085/

6. Logeate con las credenciales, en el airflow webser.

7. Activa el DAG y ejecutalo