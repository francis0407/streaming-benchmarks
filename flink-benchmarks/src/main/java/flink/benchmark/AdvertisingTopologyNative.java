/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.Utils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyNative {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyNative.class);

    private static Map<String, String> getAdCampaignMap(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        Map<String, String> ad_campaign = new HashMap<String, String>();
        String tmpString;
        while ((tmpString = reader.readLine()) != null) {
            String[] kv = tmpString.split(",");
            ad_campaign.put(kv[0], kv[1]);
        }
        return ad_campaign;
    }

    public static void main(final String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Map conf = Utils.findAndReadConfigFile(parameterTool.getRequired("confPath"), true);
        int kafkaPartitions = ((Number)conf.get("kafka.partitions")).intValue();
        int hosts = ((Number)conf.get("process.hosts")).intValue();
        int cores = ((Number)conf.get("process.cores")).intValue();

        // This map is used for joining ad with campaign.
        Map<String, String> ad_camp_map = getAdCampaignMap(conf.get("ad_to_campaign_path").toString());

        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(getFlinkConfs(conf));

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", flinkBenchmarkParams.toMap());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(flinkBenchmarkParams);

		// Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(flinkBenchmarkParams.getLong("flink.buffer-timeout", 100));

//        if(flinkBenchmarkParams.has("flink.checkpoint-interval")) {
//            // enable checkpointing for fault tolerance
//            env.enableCheckpointing(flinkBenchmarkParams.getLong("flink.checkpoint-interval", 1000));
//        }
        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
        env.setParallelism(hosts * cores);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                flinkBenchmarkParams.getRequired("topic"),
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                flinkBenchmarkParams.getProperties());
        consumer.setStartFromEarliest(); // from the first message in kafka

        DataStream<String> messageStream = env
                .addSource(consumer)
                .setParallelism(Math.min(hosts * cores, kafkaPartitions));
        System.out.println(886);





        messageStream
                .rebalance()
                // Parse the String as JSON
                .flatMap(new DeserializeBolt())

                //Filter the records if event type is "view"
                .filter(new EventFilterBolt())

                // project the event
                .<Tuple2<String, String>>project(2, 6)

                // perform join with redis data
                .flatMap(new RedisJoinBolt(ad_camp_map))

                // process campaign
                .keyBy(0)
                .flatMap(new CampaignProcessor());


        env.execute();
    }

    public static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);
            Long current_time = System.currentTimeMillis();
            Tuple7<String, String, String, String, String, String, String> tuple =
                    new Tuple7<String, String, String, String, String, String, String>(
                            obj.getString("user_id"),
                            obj.getString("page_id"),
                            obj.getString("ad_id"),
                            obj.getString("ad_type"),
                            obj.getString("event_type"),
                            obj.getString("event_time"),
                            current_time.toString());
            out.collect(tuple);
        }
    }

    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

//        RedisAdCampaignCache redisAdCampaignCache;
        Map<String, String> ad_campaign;

        RedisJoinBolt(Map<String, String> ad_campaign_map) {
            ad_campaign = new HashMap<String, String>(ad_campaign_map.size());
            for (Map.Entry<String, String> entry : ad_campaign_map.entrySet()) {
                ad_campaign.put(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
//            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//            parameterTool.getRequired("jedis_server");
//            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
//            this.redisAdCampaignCache = new RedisAdCampaignCache(parameterTool.getRequired("jedis_server"));
//            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, String> input,
                            Collector<Tuple3<String, String, String>> out) throws Exception {
            String ad_id = input.getField(0);
            String campaign_id = ad_campaign.get(ad_id);//this.redisAdCampaignCache.execute(ad_id);
            if(campaign_id == null) {
                return;
            }

            Tuple3<String, String, String> tuple = new Tuple3<String, String, String>(
                    campaign_id,
                    input.getField(0),
                    input.getField(1));
            out.collect(tuple);
        }
    }

    public static class CampaignProcessor extends RichFlatMapFunction<Tuple3<String, String, String>, String> {

//        CampaignProcessorCommon campaignProcessorCommon;
        Map<Long, Long> latency;

        Long first_time, last_time;

        String hashtable_name;

        Object lock;
        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}, table {}",
                    parameterTool.getRequired("jedis_server"),
                    parameterTool.getRequired("jedis_hashtable"));
            Jedis jedis = new Jedis(parameterTool.getRequired("jedis_server"));
            hashtable_name = parameterTool.getRequired("jedis_hashtable");
//            this.campaignProcessorCommon = new CampaignProcessorCommon(parameterTool.getRequired("jedis_server"));

//            this.campaignProcessorCommon.prepare();
            first_time = System.currentTimeMillis();
            latency = new HashMap<Long, Long>(10000);
            lock = new Object();
            Runnable flusher = () -> {
                try {
                    while (true) {
                        Thread.sleep(1000);
                        Map<Long, Long> tmp;
                        Long tmp2;

                        synchronized (lock) {
                            tmp = latency;
                            tmp2 = last_time - first_time;
                            latency = new HashMap<Long, Long>(latency.size() * 2);
                        }
                        LOG.info("Writing Redis {}, size {}!", hashtable_name, tmp.size());
                        jedis.hset(hashtable_name, "running_time", tmp2.toString());
                        for (Map.Entry<Long, Long> entry : tmp.entrySet()) {
                            jedis.hincrBy(hashtable_name, entry.getKey().toString(), entry.getValue());
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted", e);
                }
            };
            new Thread(flusher).start();
        }

        @Override
        public void flatMap(Tuple3<String, String, String> tuple, Collector<String> out) throws Exception {

//            String campaign_id = tuple.getField(0);
            String event_time =  tuple.getField(2);
            long current_time = System.currentTimeMillis();
            Long processing_latency = current_time - Long.parseLong(event_time);

            synchronized (lock) {
                Long count = latency.getOrDefault(processing_latency, 0L) + 1;
                latency.put(processing_latency, count);
                last_time = current_time;
            }
//            this.campaignProcessorCommon.execute(campaign_id, event_time);
        }

    }

    private static Map<String, String> getFlinkConfs(Map conf) {
        String kafkaBrokers = getKafkaBrokers(conf);
        String zookeeperServers = getZookeeperServers(conf);

        Map<String, String> flinkConfs = new HashMap<String, String>();
        flinkConfs.put("topic", getKafkaTopic(conf));
        flinkConfs.put("bootstrap.servers", kafkaBrokers);
        flinkConfs.put("zookeeper.connect", zookeeperServers);
        flinkConfs.put("jedis_server", getRedisHost(conf));
        flinkConfs.put("group.id", "myGroup");
        flinkConfs.put("jedis_hashtable", (String)conf.get("redis.hashtable"));
        return flinkConfs;
    }

    private static String getZookeeperServers(Map conf) {
        if(!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")));
    }

    private static String getKafkaBrokers(Map conf) {
        if(!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if(!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")));
    }

    private static String getKafkaTopic(Map conf) {
        if(!conf.containsKey("kafka.topic")) {
            throw new IllegalArgumentException("No kafka topic found!");
        }
        return (String)conf.get("kafka.topic");
    }

    private static String getRedisHost(Map conf) {
        if(!conf.containsKey("redis.host")) {
            throw new IllegalArgumentException("No redis host found!");
        }
        return (String)conf.get("redis.host");
    }

    public static String listOfStringToString(List<String> list, String port) {
        String val = "";
        for(int i=0; i<list.size(); i++) {
            val += list.get(i) + ":" + port;
            if(i < list.size()-1) {
                val += ",";
            }
        }
        return val;
    }
}
