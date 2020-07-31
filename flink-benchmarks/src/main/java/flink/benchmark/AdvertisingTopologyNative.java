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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
        env.setParallelism(1);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                flinkBenchmarkParams.getRequired("topic"),
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                flinkBenchmarkParams.getProperties());
        consumer.setStartFromEarliest(); // from the first message in kafka

        long window_size = Long.parseLong(conf.get("window.size").toString());
        int map_partitions = ((Number)conf.get("map.partitions")).intValue();
//        DataStream<String> messageStream = env.readTextFile(conf.get("events_path").toString()).setParallelism(map_partitions);
        DataStream<String> messageStream = env
                .addSource(new FileBasedDataSource(conf.get("events_path").toString(), (int)window_size))
                .setParallelism(map_partitions);
//        DataStream<String> messageStream = env
//                .addSource(consumer)
//                .setParallelism(1);
//                .setParallelism(Math.min(hosts * cores, kafkaPartitions));
        System.out.println("Begin");

//        messageStream
//                .countWindowAll(window_size)
//                .apply(new WindowedArrowFormatBolter(conf.get("shared_file").toString(), (int)window_size))
//                .flatMap(new LatencyRecordBolter());

        messageStream
//                .countWindowAll(window_size)
//                .apply(new WindowedDeserializeBolt())
                .flatMap(new MockWindowedFlatMap()).setParallelism(map_partitions)
                .filter(new EventFilterBolt()).setParallelism(map_partitions)
                .<Tuple2<String, String>>project(2, 6).setParallelism(map_partitions)
                .flatMap(new RedisJoinBolt(ad_camp_map)).setParallelism(map_partitions)
                .keyBy(0)
                .flatMap(new CampaignProcessor()).setParallelism(((Number)conf.get("reduce.partitions")).intValue()).slotSharingGroup("g1");


//        messageStream
//                .rebalance()
//                // Parse the String as JSON
//                .flatMap(new DeserializeBolt())
//
//                //Filter the records if event type is "view"
//                .filter(new EventFilterBolt())
//
//                // project the event
//                .<Tuple2<String, String>>project(2, 6)
//
//                // perform join with redis data
//                .flatMap(new RedisJoinBolt(ad_camp_map))
//
//                // process campaign
//                .keyBy(0)
//                .flatMap(new CampaignProcessor());


        env.execute();
    }

    public static class FileBasedDataSource implements ParallelSourceFunction<String> {
        String filePath;
        int batch_size;
        FileBasedDataSource(String filePath, int batch_size) {
            this.filePath = filePath;
            this.batch_size = batch_size;
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String tmpString;
            while ((tmpString = reader.readLine()) != null) {
                sourceContext.collect(tmpString);
            }
        }

        @Override
        public void cancel() {

        }
    }

    public static class MockWindowedFlatMap extends RichFlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        String hashtable_name;
        Jedis jedis;
        long partition_size;
        long num_partitions;
        boolean need_start;
        Long my_partition;
        ArrayList<String> data;
        int index;
        int times;
        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            hashtable_name = parameterTool.getRequired("jedis_hashtable");
            String jedis_host = parameterTool.getRequired("jedis_server");
            jedis = new Jedis(jedis_host);
            Long window_size = Long.parseLong(parameterTool.getRequired("window.size"));
            Long map_paritions = Long.parseLong(parameterTool.getRequired("map.partitions"));
            partition_size = window_size / map_paritions;
            num_partitions = map_paritions;
            need_start = false;
            data = new ArrayList<String>((int)partition_size);
            LOG.info("partition_size: " + new Long(partition_size).toString());
            index = 0;
            times = 0;
        }

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            data.add(input);
            index = data.size();
            if (index == partition_size) {
                start_new_window();
                String timestamp;
                if (need_start) {
                    timestamp = finish_window();
                } else {
                    timestamp = wait_window();
                }

                for (int i = 0; i < partition_size; i++) {
                    String[] items = data.get(i).split("\\|");
                    Tuple7<String, String, String, String, String, String, String> tuple =
                            new Tuple7<String, String, String, String, String, String, String>(
                                    items[0],
                                    items[1],
                                    items[2],
                                    items[3],
                                    items[4],
                                    items[5],
                                    timestamp);
                    out.collect(tuple);
                }
                index = 0;
                data.clear();
            }
        }

        void start_new_window() {
            times ++;
            need_start = false;
            jedis.hdel(hashtable_name, "start_time");
            my_partition = jedis.hincrBy(hashtable_name, "partition_count", 1);
//            LOG.info("Start partition: " + my_partition.toString() + ", time: " + new Integer(times).toString());
            if (my_partition == num_partitions) {
                need_start = true;
                jedis.hset(hashtable_name, "partition_count", "0");
            }
        }

        String finish_window() {
            Long current_time = System.currentTimeMillis();
            jedis.hset(hashtable_name, "start_time", current_time.toString());
            return current_time.toString();
        }

        String wait_window() throws Exception {
            while(true) {
                Thread.sleep(1);
                String res = jedis.hget(hashtable_name, "start_time");
                if (res != null) {
                    return res;
                }
            }
        }
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

    public static class WindowedArrowFormatBolter implements
            AllWindowFunction<Tuple2<String, Long>, Tuple4<Long, Long, Long, Long>, GlobalWindow> {
        String sharedFile;
        int buffer_size;
        int batch_size;
        Long count = 0L;
        int[] entry_len = {36, 36, 36, 4, 4, 8, 8};
        int[] entry_start = new int[7];
        int page_align(int size) {
            int page_size = 4096;
            return ((size + page_size - 1) & (~(page_size - 1)));
        }

        WindowedArrowFormatBolter(String sharedFile, int batch_size) {
            System.out.print(sharedFile);
            this.sharedFile = sharedFile;

            buffer_size = 0;
            this.batch_size = batch_size;
            for (int i = 0; i < entry_len.length; i++) {
                int tmp = page_align(entry_len[i] * batch_size);
                entry_start[i] = buffer_size;
                buffer_size += tmp;
            }


        }

        @Override
        public void apply(GlobalWindow globalWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple4<Long, Long, Long, Long>> collector) throws Exception {
//                        new Tuple7<String, String, String, String, String, String, String>(
//                                obj.getString("user_id"),
//                                obj.getString("page_id"),
//                                obj.getString("ad_id"),
//                                obj.getString("ad_type"),
//                                obj.getString("event_type"),
//                                obj.getString("event_time"),
//                                current_time.toString());
            Long receive_time = System.currentTimeMillis();
            String fileName = sharedFile + "_" + count.toString();
            count ++;
            MappedByteBuffer mbb = new RandomAccessFile(fileName, "rw")
                    .getChannel()
                    .map(FileChannel.MapMode.READ_WRITE, 0, this.buffer_size);
            int i = 0;
            Long start_time = 0L;

            // row to column
            for (Tuple2<String, Long> input: iterable) {
                String[] tuple = input.f0.split("\\|");
                for (int j = 0; j < 5; j++) {
                    mbb.position(entry_start[j] + entry_len[j] * i);
                    mbb.put(tuple[j].getBytes(), 0, entry_len[j]);
                }
                mbb.putLong(entry_start[5] + entry_len[5] * i, Long.parseLong(tuple[5]));
                mbb.putLong(entry_start[6] + entry_len[6] * i, 10L);
                i++;
                start_time = input.f1;
            }

            mbb.force();
            Long row_to_col = System.currentTimeMillis();

            // column to row:
            mbb.position(0);
            Object[] buf = new Object[this.batch_size];
            for (i = 0; i < this.batch_size; i++) {
                byte[] f0 = new byte[entry_len[0]];
                Long f5;
                Long f6;
                mbb.get(f0, 0, entry_len[0]);
                f5 = mbb.getLong(entry_start[5] + entry_len[5] * i);
                f6 = mbb.getLong(entry_start[6] + entry_len[6] * i);
                buf[i] = new Tuple3<String, Long, Long>(new String(f0), f5, f6);
            }
            Long col_to_row = System.currentTimeMillis();
            collector.collect(new Tuple4<Long, Long, Long, Long>(start_time, receive_time, row_to_col, col_to_row));
        }
    }

    public static class LatencyRecordBolter extends RichFlatMapFunction<Tuple4<Long, Long, Long, Long>, String> {
//        String jedis_host;
        Jedis jedis;
        String hashtable_name;
        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}, table {}",
                    parameterTool.getRequired("jedis_server"),
                    parameterTool.getRequired("jedis_hashtable"));
            hashtable_name = parameterTool.getRequired("jedis_hashtable");
            String jedis_host = parameterTool.getRequired("jedis_server");
            jedis = new Jedis(jedis_host);
        }

        @Override
        public void flatMap(Tuple4<Long, Long, Long, Long> input, Collector<String> collector) throws Exception {
            Long window_latency = input.f1 - input.f0;
            Long r2c_latency = input.f2 - input.f1;
            Long c2r_latency = input.f3 - input.f2;
            jedis.hset(hashtable_name + "_window", input.f0.toString(), window_latency.toString());
            jedis.hset(hashtable_name + "_r2c", input.f0.toString(), r2c_latency.toString());
            jedis.hset(hashtable_name + "_c2r", input.f0.toString(), c2r_latency.toString());
//            LOG.info("Jedis: %s %s, %s, %s\n", input.f0.toString(),
//                    window_latency.toString(), r2c_latency.toString(), c2r_latency.toString());
        }
    }

    public static class WindowedSendBolt implements
            AllWindowFunction<String, String, GlobalWindow> {
        @Override
        public void apply(GlobalWindow globalWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
            for (String input: iterable) {
                collector.collect(input);
            }
        }
    }


    public static class WindowedDeserializeBolt implements
            AllWindowFunction<String, Tuple7<String, String, String, String, String, String, String>, GlobalWindow> {
        @Override
        public void apply(GlobalWindow window, Iterable<String> iterable, Collector<Tuple7<String, String, String, String, String, String, String>> collector) throws Exception {
            Long current_time = System.currentTimeMillis();
            for (String input: iterable) {
//                JSONObject obj = new JSONObject(input);
                String[] items = input.split("\\|");
                Tuple7<String, String, String, String, String, String, String> tuple =
//                        new Tuple7<String, String, String, String, String, String, String>(
//                                obj.getString("user_id"),
//                                obj.getString("page_id"),
//                                obj.getString("ad_id"),
//                                obj.getString("ad_type"),
//                                obj.getString("event_type"),
//                                obj.getString("event_time"),
//                                current_time.toString());
                        new Tuple7<String, String, String, String, String, String, String>(
                                items[0],
                                items[1],
                                items[2],
                                items[3],
                                items[4],
                                items[5],
                                current_time.toString());
                collector.collect(tuple);
            }
            Long end_time = System.currentTimeMillis();
            System.out.printf("Window time: %d\n", end_time - current_time);
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

        Long first_time;

        String hashtable_name, jedis_host;

        AtomicLong last_time;

        Object lock;
        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}, table {}",
                    parameterTool.getRequired("jedis_server"),
                    parameterTool.getRequired("jedis_hashtable"));
            hashtable_name = parameterTool.getRequired("jedis_hashtable");
            jedis_host = parameterTool.getRequired("jedis_server");
//            this.campaignProcessorCommon = new CampaignProcessorCommon(parameterTool.getRequired("jedis_server"));
//
//            this.campaignProcessorCommon.prepare();
            first_time = System.currentTimeMillis();
            last_time = new AtomicLong(first_time);
            latency = new HashMap<Long, Long>(1000000);
        }

        @Override
        public void flatMap(Tuple3<String, String, String> tuple, Collector<String> out) throws Exception {

            String campaign_id = tuple.getField(0);
            // Do some calculation here!
            Long event_time =  Long.parseLong(tuple.getField(2));

            long current_time = System.currentTimeMillis();
            Long processing_latency = current_time - event_time;

            latency.put(event_time, processing_latency);
            last_time.set(current_time);

//            this.campaignProcessorCommon.execute(campaign_id, event_time);
        }
        @Override
        public void close() throws Exception {
            Jedis jedis = new Jedis(jedis_host);
            LOG.info("Writing Redis {}, size {}!", hashtable_name, latency.size());
            Long my_idx = jedis.hincrBy(hashtable_name ,"thread_idx", 1);

            Long running_time = last_time.get() - first_time;
            jedis.hset(hashtable_name, "running_time" + ":" + my_idx.toString(), running_time.toString());
            for (Map.Entry<Long, Long> entry : latency.entrySet()) {
                jedis.hset(hashtable_name, entry.getKey().toString() + ":" + my_idx.toString(), entry.getValue().toString());
            }
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
        flinkConfs.put("window.size",((Integer)conf.get("window.size")).toString());
        flinkConfs.put("events.num",((Integer)conf.get("events.num")).toString());
        flinkConfs.put("map.partitions",((Integer)conf.get("map.partitions")).toString());
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
