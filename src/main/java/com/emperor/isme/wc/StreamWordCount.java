package com.emperor.isme.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamWordCount {
    private static final Logger log = LoggerFactory.getLogger(StreamWordCount.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        System.out.println(host + ": " + port);
        log.info((host + ": " + port));

        DataStreamSource<String> source = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.flatMap(new MyFlatMapper())
                .keyBy(0).sum(1);
        log.info("finish process");
        result.print();
        env.execute();
    }


    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(",");
            for (String word: words){
                out.collect(new Tuple2(word,1));
            }

        }
    }

}
