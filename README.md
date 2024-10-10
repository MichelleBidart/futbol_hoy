# Football Api

Esta aplicación se creó para realizar el análisis de los partidos de fútbol en Argentina.

Se utilizó la API dinámica de API-Football: https://www.api-football.com/

El modelo de datos sigue un diagrama estrella (Star Schema):
    DER
    ![alt text](image.png)


ejecutar el proyecto de forma local
    
    docker-compose up --build

Para ejecutar partidos anteriores:

    docker exec -it <airflow-webserver-id> 

    airflow dags backfill -s 2024-03-03 -e 2024-10-07 <dag_id> para ejecutar fechas anteriores 

airflow web
    localhost:8080

explicacion 

Cuando se ejecuta por primera vez el Docker Compose, se ejecuta el script script_tables.sql, el cual crea las tablas necesarias para el proyecto. Además, se realiza una carga única de datos donde los países (countries) se insertan en la tabla country, los equipos (teams) en la tabla team, y los estadios o lugares de eventos (venues) en la tabla venue

Los scripts están diseñados para ejecutarse múltiples veces. Si la tabla ya existe, no se vuelve a crear, y cada vez que se desea recargar los datos, se realiza un DELETE previo a la nueva inserción

Hay dos dags. El primero es el league que se ejecuta una vez por mes con una arquitectura etl

El segundo es match que se ejecuta todos los dias para ver el resultado del dia anterior. Se utilizo una arquitecuta bronze, silver, gold

