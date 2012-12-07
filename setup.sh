#!/bin/bash

hadoop fs -mkdir /user/
hadoop fs -mkdir /user/cloudera/
hadoop fs -mkdir /user/cloudera/baseball/
hadoop fs -mkdir /user/cloudera/baseball/input/
hadoop fs -put lib/2012eve/* /user/cloudera/baseball/input/
