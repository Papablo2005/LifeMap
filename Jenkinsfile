// archivo: Jenkinsfile
// Pipeline CI/CD de LifeMap (Entregas 1 y 2). Si cualquier prueba falla, la etapa "Desplegar" NO se ejecuta.
// Entrega 2: prueba de humo del streaming (100 mensajes reales -> Kafka -> clasificados en MongoDB)
//            y prueba de que el modelo cargado por el streaming es el versionado.
// Credenciales en Jenkins (Secret text):  mongo-password   y   kaggle-api-token
// Jenkins corre en su propio compose (jenkins/docker-compose.yml): el pipeline nunca lo recrea.
pipeline {
    agent any

    triggers {
        githubPush()                     // webhook de GitHub (si Jenkins es alcanzable desde internet)
        pollSCM('H/2 * * * *')           // respaldo documentado: revisa GitHub cada ~2 minutos
    }

    options {
        disableConcurrentBuilds()
        timeout(time: 240, unit: 'MINUTES')
    }

    parameters {
        booleanParam(name: 'RUN_INGEST', defaultValue: false,
                     description: 'API de Kaggle -> Dask (limpieza, vida) -> MongoDB (LENTO: descarga ~4 GB la primera vez)')
        booleanParam(name: 'FORCE_DOWNLOAD', defaultValue: false,
                     description: 'Con RUN_INGEST: volver a descargar el dataset aunque ya exista')
        booleanParam(name: 'RUN_SENTIMIENTO', defaultValue: false,
                     description: 'Entrenar Naive Bayes y regresión logística y guardar el modelo')
        booleanParam(name: 'RUN_TASAS', defaultValue: false,
                     description: 'MapReduce con RDD: tasas de cierre por categoría y por geohash')
        booleanParam(name: 'RUN_BALANCEO', defaultValue: false,
                     description: 'Tabla de tiempos (2/8/32/200 particiones) y sesgo por partición')
        booleanParam(name: 'RUN_HOTSPOTS', defaultValue: false,
                     description: 'E2: ventanas históricas por geohash hasta T y umbral de hotspot')
        booleanParam(name: 'RUN_RIESGO', defaultValue: false,
                     description: 'E2: modelo de riesgo de cierre (variables hasta T) y estado de los negocios en T')
        booleanParam(name: 'RUN_SUPERVIVENCIA', defaultValue: false,
                     description: 'E2: curvas de Kaplan-Meier por categoría y por zona')
    }

    environment {
        PROD = 'docker compose -p lifemap -f docker-compose.yml'
        CI   = 'docker compose -p lifemap-ci -f docker-compose.yml'
        // Entorno de prueba del streaming: usa los modelos versionados de producción en SOLO LECTURA
        // (mismos puertos que el entorno de prueba: así Compose no recrea el Mongo de prueba en el puerto de producción)
        CI_STREAM = 'API_PORT=5099 MONGO_PORT=27099 HEALTH_REQUIRED=mongo docker compose -p lifemap-ci -f docker-compose.yml -f docker-compose.ci.yml'
        // Solo estos servicios se despliegan: Jenkins NO está en este compose, así que nunca se recrea a sí mismo
        SERVICIOS = 'mongo dask-scheduler dask-worker spark-master spark-worker kafka streaming api'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                script { env.GIT_SHA = sh(script: 'git rev-parse HEAD', returnStdout: true).trim() }
                echo "Commit: ${env.GIT_SHA}"
            }
        }

        stage('Preparar secretos') {
            steps {
                withCredentials([string(credentialsId: 'mongo-password', variable: 'MONGO_PASS')]) {
                    // Comillas simples: el secreto lo expande el shell, no Groovy (no queda en el log)
                    sh '''
                        printf "MONGO_USER=admin\\nMONGO_PASSWORD=%s\\n" "$MONGO_PASS" > .env
                    '''
                }
            }
        }

        stage('Construir imágenes') {
            steps {
                // GIT_COMMIT entra a la imagen de la API: /health lo devuelve
                withEnv(["GIT_COMMIT=${env.GIT_SHA}"]) { sh "${PROD} build" }
            }
        }

        stage('Pruebas unitarias (pytest)') {
            steps {
                // API: validación y endpoints. Ingesta/common: fórmula de la tasa, vida, limpieza, geohash...
                sh "${CI} run --rm --no-deps api pytest -q"
                sh "${CI} run --rm --no-deps ingest pytest -q"
            }
        }

        stage('Levantar entorno de prueba') {
            steps {
                // Entorno aislado (proyecto lifemap-ci): solo Mongo y API; en /health solo Mongo es obligatorio
                sh "API_PORT=5099 MONGO_PORT=27099 HEALTH_REQUIRED=mongo ${CI} up -d --wait mongo api"
            }
            post {
                failure {
                    // Diagnóstico: por qué la API no quedó healthy (logs + respuesta de /health)
                    sh "${CI} ps -a || true"
                    sh "${CI} logs --tail=80 api || true"
                    sh """${CI} exec -T api python -c "import urllib.request as u; print(u.urlopen('http://localhost:5000/health', timeout=10).read().decode())" || true"""
                }
            }
        }

        stage('Pruebas contra la API') {
            steps {
                sh "${CI} exec -T api python tests/seed.py"
                sh "${CI} exec -T api python tests/smoke.py"
            }
        }

        stage('Prueba de humo del streaming') {
            steps {
                // Kafka + streaming + Mongo de prueba (proyecto lifemap-ci, aislado de producción)
                sh "${CI_STREAM} up -d --wait mongo kafka streaming"
                // 100 reseñas REALES posteriores a T salen de la base de producción y se publican en el Kafka
                // de prueba (por una tubería: no se escriben archivos ni se toca producción)
                sh "${PROD} run --rm --no-deps -T productor python exportar_muestra.py --n 100 | ${CI_STREAM} run --rm --no-deps -T productor python productor.py --stdin --velocidad 0"
                // deben aparecer las 100 clasificadas en MongoDB (predicción, probabilidad, marca de procesamiento)
                sh "${CI_STREAM} run --rm --no-deps -T productor python verificar_streaming.py --esperadas 100 --timeout 300"
            }
            post {
                failure {
                    sh "${CI_STREAM} logs --tail=120 streaming || true"
                }
            }
        }

        stage('Prueba del modelo versionado') {
            steps {
                // el modelo que cargó el streaming de prueba es la versión de config.env (y no cambió)
                sh "${CI_STREAM} exec -T streaming python3 /opt/jobs/verificar_modelo.py"
            }
        }

        stage('Desplegar') {
            steps {
                withEnv(["GIT_COMMIT=${env.GIT_SHA}"]) {
                    sh "${PROD} up -d --wait ${SERVICIOS}"
                    // el commit que informa /health debe ser el de este build
                    sh "${PROD} exec -T -e ESPERADO=${env.GIT_SHA} api python tests/verify_deploy.py"
                }
            }
        }

        stage('Ingesta (opcional)') {
            when { expression { params.RUN_INGEST } }
            steps {
                // El token de Kaggle vive solo como credencial de Jenkins; nunca en el repositorio
                withCredentials([string(credentialsId: 'kaggle-api-token', variable: 'KAGGLE_API_TOKEN')]) {
                    sh "${PROD} run --rm -e KAGGLE_API_TOKEN -e FORCE_DOWNLOAD=${params.FORCE_DOWNLOAD ? '1' : '0'} ingest"
                }
            }
        }

        stage('Sentimiento (opcional)') {
            when { expression { params.RUN_SENTIMIENTO } }
            steps { sh 'COMPOSE_PROJECT_NAME=lifemap bash scripts/spark_submit.sh sentimiento' }
        }

        stage('MapReduce y tasas (opcional)') {
            when { expression { params.RUN_TASAS } }
            steps { sh 'COMPOSE_PROJECT_NAME=lifemap bash scripts/spark_submit.sh mapreduce_tasas' }
        }

        stage('Balanceo de carga (opcional)') {
            when { expression { params.RUN_BALANCEO } }
            steps { sh 'COMPOSE_PROJECT_NAME=lifemap bash scripts/spark_submit.sh balanceo' }
        }

        stage('E2: hotspots históricos (opcional)') {
            when { expression { params.RUN_HOTSPOTS } }
            steps { sh 'COMPOSE_PROJECT_NAME=lifemap bash scripts/spark_submit.sh hotspots_historico' }
        }

        stage('E2: modelo de riesgo (opcional)') {
            when { expression { params.RUN_RIESGO } }
            steps { sh 'COMPOSE_PROJECT_NAME=lifemap bash scripts/spark_submit.sh riesgo' }
        }

        stage('E2: supervivencia (opcional)') {
            when { expression { params.RUN_SUPERVIVENCIA } }
            steps { sh "${PROD} run --rm ingest python /app/analitica/supervivencia.py" }
        }
    }

    post {
        always {
            sh "${CI_STREAM} down -v || true"
            sh 'rm -f .env'
        }
        success { echo 'Despliegue completado.' }
        failure { echo 'El pipeline falló: NO se desplegó la nueva versión.' }
    }
}