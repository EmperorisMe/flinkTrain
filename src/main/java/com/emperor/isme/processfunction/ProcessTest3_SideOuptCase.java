package com.emperor.isme.processfunction;

import com.emperor.isme.state.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOuptCase {
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

        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp"){};
        SingleOutputStreamOperator<SensorReading> highStream = dataStream.process(new SideProcesssFunction(lowTempTag));

        highStream.print("high temp");
        highStream.getSideOutput(lowTempTag).print("low temp");

        env.execute();
    }

    private static class SideProcesssFunction extends ProcessFunction<SensorReading, SensorReading> {
        private OutputTag<SensorReading> lowTempTag;

        public SideProcesssFunction(OutputTag<SensorReading> lowTempTag) {
            this.lowTempTag = lowTempTag;
        }
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (value.getTemperature() > 30){
                out.collect(value);
            }else{
                ctx.output(lowTempTag, value);
            }
        }
    }
}
