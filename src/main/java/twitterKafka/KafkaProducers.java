package twitterKafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaProducers {
    Logger logger = LoggerFactory.getLogger(KafkaProducers.class.getName());
    public KafkaProducers(){}
    public static void main(String[] args) {
        new KafkaProducers().run();


    }

    private void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        //create a twitter client
        String consumerKey = "OWQe70B3YtV29eguTkE9kMGjR";
        String consumerSecret = "WGO7Ko1Nzol1afpBN2rYRdRaEBUno2cQHVR41HfYy6oYX7CQSC";
        String token = "1301970491696058368-gwhu9Y3CxEMDopJIb2j7XpiywxdC3w";
        String secret = "B0Om4ixd4G5oVX7ucXJI1cs5qsKCdD0FgisnH4C8ZvXrV";

        Client client = createTwitterClient(msgQueue,consumerKey,consumerSecret,token,secret);
        client.connect();

        //create a kafka consumer
        KafkaProducer<String, String> producer = createKafkaProducer();


        // loop to display results
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);


            }catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }
            if (msg!=null){
//                logger.info(msg);
                producer.send(new ProducerRecord<>("twitterTweets",null,msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null){
                            logger.error("Something had happened",exception);
//
                        }
                    }
                });
            }

        }
        logger.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

       //idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        //High throughput properties
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));


        KafkaProducer<String,String> KafkaProducer = new KafkaProducer<String, String>(properties);
        return KafkaProducer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue,String consumerKey, String consumerSecret, String token , String secret ) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bitcoin","SSR");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret,token,secret);

        //creating a twitter client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                                         // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
        // Attempts to establish a connection.
    }

}
