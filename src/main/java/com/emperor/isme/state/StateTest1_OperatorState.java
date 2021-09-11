package com.emperor.isme.state;

import com.emperor.isme.state.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class StateTest1_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            SensorReading sensorReading = new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            return sensorReading;
        });

        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMap());

        resultStream.print();

        env.execute();

    }

    private static class MyCountMap implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count ++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state){
                count += num ;
            }
        }
    }
}
