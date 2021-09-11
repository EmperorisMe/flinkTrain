package com.emperor.isme.source;

import com.emperor.isme.state.beans.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class MySensorSource implements SourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random random = new Random();
        HashMap<String, Double> tempMap = new HashMap<>();
        for (int i=0; i<10; i++ ){
            tempMap.put("sensor_" + (i+1), 60 + random.nextGaussian()*20);
        }

        while (running){
            for(String sensorId : tempMap.keySet()){
                Double newTemp = tempMap.get(sensorId) + random.nextGaussian();
                tempMap.put(sensorId, newTemp);
                ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
            }

            Thread.sleep(1000L);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
