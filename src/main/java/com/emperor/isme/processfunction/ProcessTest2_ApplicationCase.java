package com.emperor.isme.processfunction;

import com.emperor.isme.state.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest2_ApplicationCase {
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

        dataStream.keyBy("id").process(new TempIncreseProcessFunction(10)).print();

        env.execute();
    }

    private static class TempIncreseProcessFunction extends KeyedProcessFunction<Tuple,SensorReading, String> {
        private int interval = 10;
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTimeState;

        public TempIncreseProcessFunction(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
            timerTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-time", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Double currTmep = value.getTemperature();
            Long timerTime = timerTimeState.value();
            if (lastTemp != null){
                if(currTmep > lastTemp && timerTime == null){
                    Long ts = ctx.timerService().currentProcessingTime() + interval*1000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    timerTimeState.update(ts);
                }
                if(currTmep < lastTemp){
                    ctx.timerService().deleteProcessingTimeTimer(timerTime);
                    timerTimeState.clear();
                }
            }
            lastTempState.update(currTmep);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "s上升");
            timerTimeState.clear();
        }
    }
}
