#!/bin/bash

if [ $# -eq 0 ]; then
  echo ""
  echo "Usage:  ./deploy.sh <instance_name> <choice_1> <choice_2> ... <choice_3>"
  echo ""
  echo "where choice denotes one of the following:"
  echo "            kafka (for kafkaspout)"
  echo "            hbase (for hbasebolt)"
  echo "            hdfs  (for hdfsbolt)"
  echo "            local (for custom writelocal bolt)"
  echo ""
  echo "NOTE:  Specifying no spout will run a default custom spout (RandomSentenceSpout)."
  echo "       You must select at least one bolt.
  echo ""
  echo "For more information:  http://github.com/kuwabarasan"
  echo ""
  exit 1;
fi

storm jar ComponentTester-1.0-SNAPSHOT.jar hiro.storm.demo.ComponentTester ./topology.properties $@
