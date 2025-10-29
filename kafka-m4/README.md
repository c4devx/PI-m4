# Kafka

Se utiliza Apache Kafka como broker de mensajes con el objeto de desacoplar la ingesta de datos del procesamiento inicial de los mismos.

* [producer.py](producer.py) 
    - Script que, al ejecutarse, realiza una ingesta de datos, usando la AÏ de OW y los manda al topic predeterminado `openweather_topic`.
* [consumer.py](consumer.py)
    - Script que lee mensajes del topic. 
* `server.properties`
    - configuración del broker 

### Topic

* **Nombre -->** `openweather_topic`
* **Función -->** recibe el payload JSON raw directo desde la fuente de datos
* **Fuente -->** `https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric&lang=es`

### Ejecución 

- pip install kafka-python requests python-dotenv:

- **Correr producer**
    Dentro del contendor de kafka, ejecutar el producer para comenzar  a enviar datos al topic.
    ```bash
    docker-compose exec kafka python3 /producer.py
    ```

2.  **Consumer**
    Ejecutar el consumer para verificar que los datos se reciben y son cargados al bucket que corresponda.
    ```bash
    docker-compose exec kafka python3 /consumer.py
    ```

3.  **Verificación**
    Chequear en AWS, S3,, que los datos se hayan guardado en el bucket