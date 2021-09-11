package com.emperor.isme.state;

import com.emperor.isme.state.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy("id").map(new MyKeyedMap());

        resultStream.print();

        env.execute();
    }

    private static class MyKeyedMap extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));

        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = valueState.value();
            if (count == null){
                count = 0;
            }
            count ++ ;
            valueState.update(count);
            return count;
        }

        @Override
        public void close() throws Exception {

        }
    }
}
