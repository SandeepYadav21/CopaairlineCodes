# The CNS_IMAGE argument may be used to change repo Location for base container image
#ARG CNS_IMAGE=azrintaks03.azurecr.io/tibco:centos7-latest
#Changed Dockerfile to use tibco base image 2.5.4 generated in RMS project
#ARG CNS_IMAGE=azrintacr02.azurecr.io/tibcobwce_final_novalidation:2.5.3
#ARG CNS_IMAGE=azrintaks03.azurecr.io/tibco/bwceas:2.6.3

#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.8.3
#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.9.1
#ARG EDA_IMAGE=azracroslprocesuate1001.azurecr.io/bwce:2.9.1.debian.bookworm.slim
#ARG EDA_IMAGE=azracroslprocesuate1001.azurecr.io/bwce:2.9.1.ubi.latest

ARG EDA_IMAGE=azracroslprocesproe1001.azurecr.io/bwce:2.9.2.ubi
FROM $EDA_IMAGE

ARG SVC_BUILD_VERSION

ADD "eda-operational-seats-affected.application_1.0.0.ear" /


#adding plugins
#COPY addons/*.zip /resources/addons/plugins
#COPY lib/*.zip /resources/addons/lib

# for businessworks domain service
EXPOSE 8080
EXPOSE 8087
ENV BW_JAVA_OPTS="-Dbw.engine.enable.memory.saving.mode=true -Dbw.application.activity.validateInput=false -Dbw.application.activity.validateOutput=false -Xms1G -Xmx1G -Dbw.engine.enable.memory.saving.mode=true -DBW_DISABLE_SWAGGER_UI=false -Dswagger.disable=false -XX:+UseG1GC -XX:+DisableExplicitGC -XX:MaxGCPauseMillis=300 -XX:ParallelGCThreads=8 -XX:ConcGCThreads=4 -XX:InitiatingHeapOccupancyPercent=50"
# for health check probe
EXPOSE 7777 