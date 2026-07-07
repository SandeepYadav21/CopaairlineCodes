# The CNS_IMAGE argument may be used to change repo Location for base container image
#ARG CNS_IMAGE=azrintaks03.azurecr.io/tibco:centos7-latest
#Changed Dockerfile to use tibco base image 2.5.4 generated in RMS project
#ARG CNS_IMAGE=azrintacr02.azurecr.io/tibcobwce_final_novalidation:2.5.3
#ARG CNS_IMAGE=azrintaks03.azurecr.io/tibco/bwceas:2.6.3

#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.8.3

#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.9.1
#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.9.1.debian.bookworm.slim
#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.9.1.ubi.latest
ARG EDA_IMAGE=azracroslprocesproe1001.azurecr.io/bwce:2.9.1.ubi
FROM $EDA_IMAGE

ARG SVC_BUILD_VERSION

ADD "seat-assigned.application_1.0.0.ear" /


#adding plugins
#COPY addons/*.zip /resources/addons/plugins
#COPY lib/*.zip /resources/addons/lib

# for businessworks domain service
EXPOSE 8080

# for health check probe
EXPOSE 7777 