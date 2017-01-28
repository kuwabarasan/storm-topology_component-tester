package hiro.storm.demo;

// Import statements for 1.0 Storm
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;


public class ComponentTester {

	// General
	private static final String NUMWORKERS = "topology.num.workers";
	private static final String TOPOLOGYDEBUG = "topology.debug.enabled";

	// Parallelism
	private static final String WRITELOCALBOLTPARALLELISM = "writelocal.bolt.parallelism";
	private static final String KAFKASPOUTPARALLELISM = "kafka.spout.parallelism";
	private static final String HBASEBOLTPARALLELISM = "hbase.bolt.parallelism";
	private static final String HDFSBOLTPARALLELISM = "hdfs.bolt.parallelism";

	private static final Logger LOG = Logger.getLogger(ComponentTester.class);


	public static void main(String[] args) {

		// Main arguments
		// @param: properties file path as argument
		// @param: choices -> argument list with values: [kafka|hbase|hdfs|local]

		// Load properties
		Properties props;
		try {
			props = loadProperties(args[0]);
		} catch (IOException e){
			return;
		}

		// General topology properties
		Config config = new Config();
		config.setNumWorkers(Integer.parseInt(props.getProperty(NUMWORKERS)));
		config.setDebug(Boolean.parseBoolean(props.getProperty(TOPOLOGYDEBUG)));

		List<String> choices = subsetListFromArgs(Arrays.asList(args).iterator());
		if(!validateChoices(choices))
			return;

		TopologyBuilder builder;
		try {
			builder = choiceBuilder(choices, props, config);
		} catch (Exception e){
			LOG.error("Error building topology: " + e);
			return;
		}

		try {
			StormSubmitter.submitTopology(args[1], config, builder.createTopology());
		} catch (Exception e) {
			LOG.error("Error submitting topology to cluster: " + e);
		}
	}

	private static Properties loadProperties(String path) throws IOException {
		Properties props = new Properties();

		LOG.info("Loading properties file: " + path);
		try (FileReader reader = new FileReader(path)) {
			props.load(reader);
		} catch (IOException e) {
			LOG.error("Error encountered loading properties file: " + e);
			throw new IOException("Cannot load properties file.");
		}
		return props;
	}

	private static List<String> subsetListFromArgs(Iterator<String> iter){
		iter.next(); // Skip first argument, which is the submission instance name
		iter.next(); // Skip second argument, which is the properties path
		List<String> choices = new ArrayList<>();
		while(iter.hasNext()){
			String thisChoice = iter.next().toLowerCase();
			// Duplicate checker, just in case
			if (!choices.contains(thisChoice))
				choices.add(thisChoice);
		}
		return choices;
	}

	private static boolean validateChoices(List<String> choices){
		for (String choice : choices){
			if (!choice.equals("kafka") && !choice.equals("hbase")
					&& !choice.equals("hdfs") && !choice.equals("hive")){
				LOG.error(choice + " is not a valid argument.  Please check your arguments.");
				return false;
			}
		}
		return true;
	}

	private static TopologyBuilder choiceBuilder(List<String> choices, Properties props, Config config) {
		TopologyBuilder builder = new TopologyBuilder();
		boolean packagedSpout = false;
		boolean atLeastOneBolt = false;

		for (String choice : choices) {
			switch (choice) {
				case "kafka":
					builder.setSpout("messages", KafkaSpoutGen.createKafkaSpout(props), Integer.parseInt(props.getProperty(KAFKASPOUTPARALLELISM)));
					packagedSpout = true;
					break;
				case "hbase":
					builder.setBolt("hbasebolt", HBaseBoltGen.createHBaseBolt(props, config), Integer.parseInt(props.getProperty(HBASEBOLTPARALLELISM)))
							.shuffleGrouping("messages");
					if (!atLeastOneBolt) atLeastOneBolt = true;
					break;
				case "hdfs":
					builder.setBolt("hdfsbolt", HDFSBoltGen.createHDFSBolt(props, config), Integer.parseInt(props.getProperty(HDFSBOLTPARALLELISM)))
							.shuffleGrouping("messages");
					if (!atLeastOneBolt) atLeastOneBolt = true;
					break;
				case "local":
					builder.setBolt("writelocal", WriteLocalBoltGen.createWriteLocalBolt(props), Integer.parseInt(props.getProperty(WRITELOCALBOLTPARALLELISM)))
							.shuffleGrouping("messages");
					if (!atLeastOneBolt) atLeastOneBolt = true;
					break;
			}
		}

		if(!packagedSpout)
			builder.setSpout("messages", RandomSentenceSpoutGen.createRandomSentenceSpout(props));

		if(!atLeastOneBolt)
			throw new IllegalArgumentException("At least one bolt needs to be specified.");

		return builder;
	}


}
