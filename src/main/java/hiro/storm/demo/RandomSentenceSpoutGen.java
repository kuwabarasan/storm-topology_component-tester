package hiro.storm.demo;

import java.util.Properties;

public class RandomSentenceSpoutGen {

    private static final String RSSPOUT_PAUSE   = "rsspout.pause";

    public static RandomSentenceSpout createRandomSentenceSpout(Properties props){
        return new RandomSentenceSpout(Long.parseLong(props.getProperty(RSSPOUT_PAUSE), 10));
    }
}