# Storm topology: component tester
This is a topology used for troubleshooting purposes for the commonly used pre-packaged storm components:  Kafkaspout, HBasebolt, HDFSbolt

It will generate a simple topology that only has two levels:  spout -> bolt

The spouts available for choosing (one of the following):  Kafkaspout, RandomSentenceSpout (custom)

The bolts available for choosing (at least one):  HBasebolt, HDFSbolt, Writelocalbolt (custom)

##To install:
- Run git clone on a directory of choice on your storm cluster:
```
git clone https://github.com/kuwabarasan/storm-topology_component-tester
```
- Install maven if you do not have it locally.  Add the maven bin to $PATH.
- While inside the root directory with pom.xml, run the following:
```
mvn clean package
```
- This will produce a 'ComponentTester-1.0-SNAPSHOT.jar' in 'target' directory.  Copy this into the root directory (needs to be in the same directory path as deploy.sh and topology.properties

##Before you run:
- Before deploying, make sure to inject the necessary *.xml config files.  For example (with bash):
```
function inject {
  printf "Injecting %s..\n" "$2"
  jar uf ComponentTester-1.0-SNAPSHOT.jar -C "$1" "$2"
}

inject "/etc/hadoop/conf" "hdfs-site.xml"
inject "/etc/hadoop/conf" "core-site.xml"
inject "/etc/hbase/conf" "hbase-site.xml"
```
- Modify the topology.properties file to match cluster configurations
- Check the following points for each component:
  1.  HDFSBolt
    - Make sure that the output directory has the correct permissions (e.g. hdfs dfs -ls /, hdfs dfs -chmod NNN <path>)
  2.  HBaseBolt
    - The HBasebolt created as test will simply write to a one column family table.  Make sure to create this destination table.  For example, to create a test table 't1' with column family 'cf':
    ```
    $ hbase shell 
    hbase(main)> create 't1', {NAME => 'cf'} 
    hbase(main)> grant '<user>', 'RWXCA', 't1' 
    (e.g. if kerberized and using HEADLESS_PRINCIPAL@REALM, then: grant 'HEADLESS_PRINCIPAL', 'RWXCA', 't1') 
    ```
  3.  WritelocalBolt
    - This custom bolt will write to a destination local file specified intopology.properties.  If the file does not exist, make sure the   parent directory has write permissions for the storm user.
  4.  Kafkaspout
    - If kafka is kerberized, make sure the principal specified in the 'KafkaClient' section in the utilized storm_jaas.conf file has permissions to access the topic.  Also, double-check the security protocol specified.
    
##To deploy:
In order to deploy this topology, you will need to run the 'deploy.sh' script with specific arguments:
```
./deploy.sh <instance_name> <choice_1> <choice_2> ... <choice_3>
```
where choice denotes one of the following:
            kafka (for kafkaspout)
            hbase (for hbasebolt)
            hdfs  (for hdfsbolt)
            local (for custom writelocal bolt)

**NOTE:**   Specifying no spout will run a default custom spout (RandomSentenceSpout).  Also, you must select at least one bolt.

For example:
```
./deploy.sh testinstance1 kafka hbase hdfs
```
The above command will submit a topology with the name 'testinstance1', and it will contain a kafkaspout feeding messages to both HBase and HDFS via HBasebolt and HDFSbolt respectively.

Another example:
```
./deploy.sh testinstance2 hbase local
```
The above command will submit a topology with the name 'testinstance2', and it will contain a RandomSentenceSpout (custom) feeding messages to HBase and to a local file via HBasebolt and writelocalbolt (custom) respectively.
