package hiro.storm.demo;

// Import statements for pre-1.0 Storm
/*
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
*/

// Import classes for 1.0+ Storm
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;


public class WriteLocalBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(WriteLocalBolt.class);

    private String filepath;
    private FileWriter writer;
    private static Object writeLock;

    private OutputCollector outputCollector;

    public WriteLocalBolt(String filepath) {
        this.filepath = filepath;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.writeLock = new Object();
        outputCollector = collector;

        LOG.info("Preparing file reader with filepath: " + filepath);
        try {
            writer = new FileWriter(filepath, true);
        } catch (IOException e) {
            LOG.error("Encountered IOException when creating writer to filepath: " + e);
        }
    }

    @Override
    public void execute(Tuple tuple) {

        String msg = tuple.getString(0);

        synchronized (writeLock) {
            try {
                LOG.debug("Writing message: " + msg);
                writer.write(msg);
                writer.write(System.getProperty("line.separator"));
                writer.flush();
            } catch (IOException e) {
                LOG.error("Failed to write message: ");
            } finally {
                // Skip past messages that could not be persisted
                outputCollector.ack(tuple);
            }
        }
    }

    @Override
    public void cleanup() {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                LOG.error("Failed to close writer!");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

}
