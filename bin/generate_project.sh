#!/bin/bash
mvn archetype:generate                               \
    -DarchetypeGroupId=org.apache.flink              \
    -DarchetypeArtifactId=flink-quickstart-java      \
    -DarchetypeCatalog=https://repo1.maven.org/maven2 \
    -DarchetypeVersion=1.16.0
