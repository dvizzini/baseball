#!/bin/bash

#script deletes jar, recompiles software, and reruns program

if [ "$1" = "--help" ]
then
        echo "refresh.sh [output_file], where output_file defaults to output.txt"
        exit 0
fi

#assign variables
if [ -z "$1" ]
then
        output_file="output.txt"
else
        output_file=$1
fi

target="target/classes/"
jar="baseball.jar"

echo ".........creating new jar........."
rm -f "$jar"
rm -f target/classes/com/danielvizzini/hadoop/*.class
javac -cp /usr/lib/hadoop/client-0.20/\* -d "$target" src/main/java/com/danielvizzini/hadoop/Baseball.java 
jar -cvf "$jar" -C "$target" .

echo "...removing files from hadoop fs.."
hadoop fs -rm /user/cloudera/baseball/output/*
hadoop fs -rmdir /user/cloudera/baseball/output/

hadoop jar baseball.jar com.danielvizzini.baseball.Baseball /user/cloudera/baseball/input/* /user/cloudera/baseball/output

hadoop fs -cat /user/cloudera/baseball/output/* > $output_file

if [[ $output_file == 'output.txt' ]]
then
        ./sort.py
fi

exit 0
