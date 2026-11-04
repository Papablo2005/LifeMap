# archivo: jenkins/Dockerfile
# Jenkins + CLI de Docker + plugin compose, para que el pipeline pueda
# construir y levantar contenedores usando el Docker del computador (socket montado).
FROM jenkins/jenkins:lts-jdk17
USER root
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker:27-cli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose
RUN jenkins-plugin-cli --plugins git github workflow-aggregator credentials-binding
# Se queda como root para poder usar /var/run/docker.sock (aceptable en un proyecto académico).
