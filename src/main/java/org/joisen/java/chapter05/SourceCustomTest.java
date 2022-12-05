package org.joisen.java.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @Author Joisen
 * @Date 2022/12/5 20:13
 * @Version 1.0
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<Event> customSource = env.addSource(new ClickSource());
        DataStreamSource<Integer> customParallelSource = env.addSource(new ParallelCustomSource()).setParallelism(2);
//        customSource.print();
        customParallelSource.print();


        env.execute();
    }

    public static class ParallelCustomSource implements ParallelSourceFunction<Integer>{
        private Boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while(running){
                sourceContext.collect(random.nextInt());
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
