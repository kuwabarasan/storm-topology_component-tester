package hiro.storm.demo;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RandomSentenceSpout extends BaseRichSpout{

    private static final Logger LOG = Logger.getLogger(RandomSentenceSpout.class);

    // RandomSentenceSpout: Generates a random sentence
    // A simple way to accomplish this is to select a sentence composition
    // E.g. The <singular_noun_1> <verb> the <adjective> <singular_noun_2>.
    // For each compositional element, we can have a small word bank (String list) to randomly choose a word from

    private SpoutOutputCollector outputCollector;

    private final String[] SINGULAR_NOUNS_1 = {"man", "woman", "bird", "pig", "elephant", "dog", "zookeeper", "falcon"};
    private final String[] VERBS            = {"throws", "drives", "jumps", "kicks", "punches", "pushes", "flies", "lifts"};
    private final String[] ADJECTIVES       = {"small", "big", "beautiful", "ugly", "heavy", "shiny", "interesting", "lazy"};
    private final String[] SINGULAR_NOUNS_2 = {"car", "tree", "tank", "plane", "rock", "house", "road", "computer"};

    private final int WORD_BANK_LENGTH = 8;
    private Long pause;

    public RandomSentenceSpout(Long pause){
        this.pause = pause;
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.outputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        outputCollector.emit(new Values(generateSentence()));
        try {
            Thread.sleep(pause);
        } catch (InterruptedException e){
            LOG.error("nextTuple() pause interrupted.", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("str"));
    }

    private String generateSentence(){
        int randomNum1 = ThreadLocalRandom.current().nextInt(0, 9);
        int randomNum2 = ThreadLocalRandom.current().nextInt(0, 9);
        int randomNum3 = ThreadLocalRandom.current().nextInt(0, 9);
        int randomNum4 = ThreadLocalRandom.current().nextInt(0, 9);

        return "The " + SINGULAR_NOUNS_1[WORD_BANK_LENGTH % randomNum1]
                + " " + VERBS[WORD_BANK_LENGTH % randomNum2]
                + " the " + ADJECTIVES[WORD_BANK_LENGTH % randomNum3]
                + " " + SINGULAR_NOUNS_2[WORD_BANK_LENGTH % randomNum4];
    }
}
