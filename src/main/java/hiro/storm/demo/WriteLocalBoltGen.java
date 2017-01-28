package hiro.storm.demo;


import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class WriteLocalBoltGen {

    private static final String WRITELOCAL_FILEPATH = "writelocal.file.path";

    private static final Logger LOG = Logger.getLogger(WriteLocalBoltGen.class);


    public static WriteLocalBolt createWriteLocalBolt(Properties props){

        // Check filepath, create if file does not exist
        String filepath = props.getProperty(WRITELOCAL_FILEPATH);
        File file = new File(filepath);
        if (!file.isFile()){
            try {
                LOG.info("Specified destination file does not exist - " + filepath);
                LOG.info("Creating file...");
                Files.write(Paths.get(filepath), "".getBytes("utf8"));
            } catch (IOException e) {
                LOG.error("Failed to create file.  Reason: " + e);
                throw new IllegalArgumentException();
            }
        } else
            LOG.info("Specified destination file exists - path: " + filepath);

        return new WriteLocalBolt(filepath);
    }

}
