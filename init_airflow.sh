#!/bin/bash

echo "🚀 Inicializando Airflow..."

# Crear directorios necesarios
mkdir -p logs
chmod 777 logs

# Inicializar la base de datos de Airflow
echo "📊 Inicializando base de datos de Airflow..."
docker-compose exec airflow-webserver airflow db init

# Crear usuario admin
echo "👤 Creando usuario admin..."
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@travelmind.com \
    --password admin

echo "✅ Airflow inicializado correctamente!"
echo "🌐 Accede a Airflow en: http://localhost:8081"
echo "👤 Usuario: admin"
echo "🔑 Contraseña: admin"
