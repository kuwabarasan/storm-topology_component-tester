package hiro.storm.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HDFSBoltGen {

    private static final Logger LOG = Logger.getLogger(HDFSBoltGen.class);

    private static final String HDFS_FS_URL          =  "hdfs.fs.url";
    private static final String HDFS_SYNC_COUNT      =  "hdfs.sync.count";
    private static final String HDFS_FILE_SIZE       =  "hdfs.rotation.file.size";   // in MB
    private static final String HDFS_FILE_PATH       =  "hdfs.file.path";
    private static final String HDFS_KEYTAB          =  "hdfs.keytab.file";
    private static final String HDFS_PRINCIPAL       =  "hdfs.kerberos.principal";


    public static HdfsBolt createHDFSBolt(Properties props, Config config){

        // Pipe delimited
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        SyncPolicy syncPolicy = new CountSyncPolicy(Integer.parseInt(props.getProperty(HDFS_SYNC_COUNT)));
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(Float.parseFloat(props.getProperty(HDFS_FILE_SIZE)), FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(props.getProperty(HDFS_FILE_PATH));

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl(props.getProperty(HDFS_FS_URL))
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        //Security check
        if (isSecure()){
            LOG.info("HDFS is secure. Passing specified keytab and principal into config for HDFSBolt.");
            Map<String, Object> securityConf = new HashMap<>();
            securityConf.put("hdfs.keytab.file", props.getProperty(HDFS_KEYTAB));
            securityConf.put("hdfs.kerberos.principal", props.getProperty(HDFS_PRINCIPAL));
            config.put("hdfs.config", securityConf);
            hdfsBolt.withConfigKey("hdfs.config");
        }
        return hdfsBolt;
    }

    public static boolean isSecure(){
        Configuration configuration = new Configuration();
        configuration.addResource("hdfs-site.xml");
        if (configuration.get("hadoop.security.authentication") == null)
            LOG.info("Property 'hadoop.security.authentication' not found in hbase-site.xml.");
        else if(configuration.get("hadoop.security.authentication").equalsIgnoreCase("kerberos"))
            return true;

        return false;
    }

}
