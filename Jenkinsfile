// archivo: Jenkinsfile
// Pipeline CI/CD de LifeMap. Si cualquier prueba falla, la etapa "Desplegar" NO se ejecuta.
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
    }

    environment {
        PROD = 'docker compose -p lifemap -f docker-compose.yml'
        CI   = 'docker compose -p lifemap-ci -f docker-compose.yml'
        // Solo estos servicios se despliegan: Jenkins NO está en este compose, así que nunca se recrea a sí mismo
        SERVICIOS = 'mongo dask-scheduler dask-worker spark-master spark-worker api'
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
        }

        stage('Pruebas contra la API') {
            steps {
                sh "${CI} exec -T api python tests/seed.py"
                sh "${CI} exec -T api python tests/smoke.py"
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
    }

    post {
        always {
            sh "API_PORT=5099 MONGO_PORT=27099 HEALTH_REQUIRED=mongo ${CI} down -v || true"
            sh 'rm -f .env'
        }
        success { echo 'Despliegue completado.' }
        failure { echo 'El pipeline falló: NO se desplegó la nueva versión.' }
    }
}
