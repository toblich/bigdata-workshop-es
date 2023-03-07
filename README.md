# ITBA ECD - Seminario: Trabajo final

El trabajo consiste en una aproximación a un sistema real para procesar datos. En este caso, a grandes rasgos, el sistema tiene los siguientes pasos:
1. Una API que se considera como fuente de datos, a la que cada vez que se le piden datos devuelve un batch de lo que "se acumuló" en el último rato.
2. Un DAG en Airflow que consume periódicamente esa API y persiste, tal como se los obtuvo y sin modificar, cada batch de datos en un bucket "landing".
3. Otro DAG que recibe una notificación via Kafka cuando hay un nuevo batch en "landing" y lo procesa para tener un dataset más usable, y persiste el resultado en un bucket "delta" en formato parquet.
4. Un jupyter notebook que, usando Spark, toma el dataset del bucket y entrena un clasificador, persistiendo los parámetros, métricas y modelos de cada corrida en MLFlow.

## Servicios

El `docker-compose.yml` contiene todos los servicios que se usan. Sin embargo, se lista aquí todo lo relevante:
- `MinIO` como Object Storage, ya que es fácil de usar dockerizado y tiene una api compatible con S3.
- `Airflow` como orquestador de workflows para la parte de ETL/ELT.
- `Kafka` (+ `Zookeeper`) como event bus para notificaciones. _Nota: Se usó una versión reciente de Kafka en vez de la provista por la materia, ya que se hacía complicado integrar MinIO y desarrollar un sensor de Airflow para versiones viejas_
- `./api/`: Una API hecha en NodeJS que devuelve, de a páginas, los registros del [SMS Spam Collection Dataset](https://www.kaggle.com/datasets/uciml/sms-spam-collection-dataset)
- `MLFlow` como registro de experimentos y modelos, usando el `MinIO` ya nombrado para la persistencia
- `Jupyter` + `Spark` para entrenar un clasificador. _Nota: no se usó el cluster de Spark que estaba en el docker-compose provisto por la materia debido a que el mismo usa imágenes custom versiones muy viejas sin binarios disponibles para procesadores ARM, y para las que se volvió complicado encontrar los binarios en las versiones correctas que permitieran interactuar con S3/MinIO._

## Flujo detallado

1. La API, en cada request, responde con una nueva página del dataset base. Esto simula el caso de un servicio al que periódicamente (una vez por hora, una vez al día, etc) se le pide un reporte de lo que sucedió
2. El DAG `landing` en Airflow crea un bucket `landing` en MinIO, si no existe, configurando la notificación a Kafka ante la creación de objetos en el mismo. Periódicamente pide a la API una página de resultados y la persiste en dicho bucket, tal como se obtuvo y sin modificarla. Este bucket `landing` permite persistir la información cruda, ya que ante nuevos pedidos la API no volverá a servir el mismo contenido, y así no tener pérdida de información.
3. El DAG `delta` crea un bucket `delta` en MinIO y usa un sensor sobre kafka. Cuando ve un mensaje en kafka avisando que hay un nuevo objeto en `landing`, este DAG se baja este objeto de MinIO, lo procesa generando nuevas features más limpias, y lo persiste en el bucket `delta` en formato parquet. Entre otras cosas, por ejemplo, se encarga de resolver el encoding que tiene el dataset original.
4. El jupyter notebook, que se ejecuta a mano, usa Spark y los binarios necesarios para interactuar con S3 (que sirven para interactuar también con MinIO). Antes de ejecutarlo, también hay que crear a mano (una sola vez) un bucket en MinIO llamado `MLFlow`, para que se puedan persistir los detalles de todas las ejecuciones. Este notebook toma el dataset completo del bucket `delta` y lo carga en un dataframe de Spark, para luego tokenizar el texto, eliminar stopwords, contar ocurrencias, acomodar todos los datos, y así entrenar un Naive Bayes que permite reconocer si un SMS es spam o no. Con esto, se logró un AUC de 0.976. Este notebook loguea todos los parámetros, métricas, y el modelo resultante en `MLFlow` (que a su vez usa MinIO para persistir todo).

## Cómo ejecutar todo

1. Clonar el repositorio
2. Correr `docker-compose up -d` para levantar todos los servicios.
3. Entrar a http://localhost:9090/ para ver el estado de los DAGs y sus corridas. Los mismos arrancan ya prendidos y de a poco van recibiendo, procesando y persistiendo los datos (ETL/ELT).
4. Entrar a http://localhost:9091/ para ver los buckets y cómo se van populando de datos. Las credenciales están en el `docker-compose.yml`, como variables de entorno del servicio `minio`.
5. Crear a mano en MinIO un bucket más, llamado `mlflow`
6. Una vez que el bucket `delta` tiene objetos (puede esperarse a que esté completo con todas las partes del dataset, o ejecutarlo en cualquier momento antes para entrenar con un dataset parcial y volver a ejecutarlo más tarde cuando haya más datos para reentrenar el modelo), entrar a http://localhost:8888/notebooks/work/sms-spam-classifier.ipynb y ejecutar ese notebook
7. Entrar a http://localhost:4000/ para ver en MLFlow los resultados de la corrida. También se puede ver en MinIO que el bucket `mlflow` tiene datos ahora.
