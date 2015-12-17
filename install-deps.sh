#!/usr/bin/env bash

mvn install:install-file -DgroupId=local -DartifactId=RedshiftJDBC41 \
    -Dversion=1.1.10.1010 -Dpackaging=jar -Dfile=jars/RedshiftJDBC41-1.1.10.1010.jar