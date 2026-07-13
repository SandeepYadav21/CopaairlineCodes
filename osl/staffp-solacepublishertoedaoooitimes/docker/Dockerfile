# The CNS_IMAGE argument may be used to change repo Location for base container image
#ARG CNS_IMAGE=azrintaks03.azurecr.io/tibco:centos7-latest
#Changed Dockerfile to use tibco base image 2.5.4 generated in RMS project
#ARG CNS_IMAGE=azrintacr02.azurecr.io/tibcobwce_final_novalidation:2.5.3
#ARG CNS_IMAGE=azrintaks03.azurecr.io/tibco/bwceas:2.6.3

#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.8.3

#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.9.1
#ARG EDA_IMAGE=azracroslprocesinte1001.azurecr.io/bwce:2.9.1.debian.bookworm.slim
#ARG EDA_IMAGE=azracroslprocesuate1001.azurecr.io/bwce:2.9.1.ubi.latest
FROM azracroslprocesproe1001.azurecr.io/bwce:2.9.2.FTL
#FROM $EDA_IMAGE

#FROM azrosbdev01.azurecr.io/bwce:2.9.1.FTL
#ADD staffp_solacepublishertoedaoooitimes.application/target/*.ear /
ADD "staffp-solacepublishertoedaoooitimes.application_1.0.0.ear" /
EXPOSE 21000

ARG SVC_BUILD_VERSION