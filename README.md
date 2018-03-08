# Ejecución de ProDiGen en un cluster de Spark

### Prerequisites

Debemos tener generados previamente todos los .jar del proyecto. Para ello, mediante IntelliJ generamos los "artifacts" y los linkamos vía MANIFEST.MF

* Debemos eliminar los .jar logback-core-1.1.11.jar y logback-classic-1.1.11.jar ya que causan conflictos con Spark y Spring.

### Installing

Debemos crear las imágenes de Docker mediante los "Dockerfile" que se aportan. En primer lugar generamos una imagen base, que reutilizaremos para agilizar el despliegue.

```
docker build -t="martin/prodigen-backend" .
docker run -d  martin/prodigen-backend
```



## Built With

* [Docker](https://www.docker.com/)

## Authors

* **Martín Dacosta Salgado**