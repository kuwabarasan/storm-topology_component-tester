package hiro.storm.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HBaseBoltGen {

    private static final String HBASE_ROOTDIR	= "hbase.rootdir";
    private static final String HBASE_TABLE		= "hbase.table.name";
    private static final String HBASE_CF        = "hbase.column.family";
    private static final String HBASE_KEYTAB	= "hbase.keytab.file";
    private static final String HBASE_PRINCIPAL	= "hbase.kerberos.principal";

    private static final Logger LOG = Logger.getLogger(HBaseBoltGen.class);


    public static HBaseBolt createHBaseBolt(Properties props, Config config){
        Map<String, String> HBConfig = new HashMap<>();
        HBConfig.put("hbase.rootdir", props.getProperty(HBASE_ROOTDIR));

        if(isSecure()){
            HBConfig.put("storm.keytab.file", props.getProperty(HBASE_KEYTAB));
            HBConfig.put("storm.kerberos.principal", props.getProperty(HBASE_PRINCIPAL));
        }

        config.put("HBCONFIG",HBConfig);

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("str")
                .withColumnFields(new Fields("str"))
                .withColumnFamily(props.getProperty(HBASE_CF));

        return new HBaseBolt(props.getProperty(HBASE_TABLE), mapper)
                    .withConfigKey("HBCONFIG");
    }

    public static boolean isSecure(){
        Configuration configuration = new Configuration();
        configuration.addResource("hbase-site.xml");
        if(configuration.get("hbase.security.authentication").equalsIgnoreCase("kerberos"))
            return true;
        else return false;
    }

}
