#!/bin/bash

##
##  COPYRIGHT NOTICE
##  
##  Copyright (c) 2020 David Harper
##  
##  All rights reserved.
##

if [ -z "${APPCLASS}" ]
then
    echo "Set APPCLASS to name of application class and re-run."
    exit 1
fi

if [ -z "${STARCATS_BASE}" ]
then
    echo "Set STARCATS_BASE to application base directory and re-run."
    exit 1
fi

BASEDIR=`dirname $0`

CLASSDIR=${BASEDIR}/build/classes/java/main

if [ ! -d "${CLASSDIR}" ]
then
    echo "Class directory ${CLASSDIR} not found.  Run 'gradle build' then re-run."
    exit 1
fi

JARLIBDIR=${BASEDIR}/build/extlibs

if [ ! -d "${JARLIBDIR}" ]
then
    echo "External JAR library directory ${JARLIBDIR} not found.  Run 'gradle build' then re-run."
    exit 1
fi

java  -Dstarcats.base="${STARCATS_BASE}" \
        -cp ${CLASSDIR}:${JARLIBDIR}/\* \
        ${STARCATS_JAVA_OPTS} \
	${APPCLASS} "$@"
