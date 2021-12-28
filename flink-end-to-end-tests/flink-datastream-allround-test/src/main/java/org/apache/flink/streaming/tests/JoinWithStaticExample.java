package org.apache.flink.streaming.tests;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

public class JoinWithStaticExample {

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(new String[] {
                "--classloader.check-leaked-classloader","false",
                "--state_backend.checkpoint_directory", "file:///home/shengqun97/",
                "--test.simulate_failure", "false",
                "--test.simulate_failure.max_failures", String.valueOf(1),
                "--test.simulate_failure.num_records", "100",
                "--print-level", "0",
//                "--hdfs-log-storage","hdfs://10.128.0.5:8020/",
                "--enable-logging","false",
        });

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = 56;
        setupEnvironment(env, pt);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://10.128.0.10:8020"),conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new org.apache.hadoop.fs.Path("/tweets/slangs.csv"))));
        reader.readLine();
        String strLine;
        ArrayList<Tuple3<Integer, String, String>> slangs = new ArrayList<>();
        while ((strLine = reader.readLine()) != null)   {
            String[] arr = strLine.split(",");
            slangs.add(new Tuple3<>(Integer.parseInt(arr[0]),arr[1],arr[2].toLowerCase()));
        }

        String hdfsURI2 = "hdfs://10.128.0.10:8020/tweets/sampletweet.csv";
        RowCsvInputFormat format2 = new RowCsvInputFormat(new Path(hdfsURI2), new TypeInformation[] { BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO });
        format2.setSkipFirstLineAsHeader(true);
        DataStream<Row> dynamicSource = env.readFile(format2, hdfsURI2).setParallelism(parallelism);
//        DataStream<Row> dynamicSource = env.addSource(new SourceFunction<Row>() {
//            @Override
//            public void run(SourceContext<Row> ctx) throws Exception {
//                for(int i=0;i<56;++i) {
//                    Thread.sleep(2000);
//                    Row row = new Row(3);
//                    row.setField(0, i);
//                    row.setField(1, "123");
//                    row.setField(2, 1);
//                    ctx.collect(row);
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        });
        dynamicSource.filter(row -> (Integer)row.getField(2) == 1).setParallelism(parallelism).keyBy(new KeySelector<Row, Integer>() {
            @Override
            public Integer getKey(Row value) throws Exception {
                return (Integer)value.getField(0);
            }
        }).process(new KeyedProcessFunction<Integer, Row, Object>() {

            @Override
            public void processElement(
                    Row value,
                    Context ctx,
                    Collector<Object> out) throws Exception {
                for(int i=0;i<24;i++){
                    final int idx= i;
                    slangs.forEach(tuple ->{
                        if(Objects.equals(value.getField(0), tuple.f0)){
                            if(((String)value.getField(1)).contains(tuple.f2) && idx == 0){
                                out.collect(Row.join(value, Row.of(tuple.f0,tuple.f1,tuple.f2)));
                            }
                        }
                    });
                };
            }
        }).setParallelism(parallelism).print();


//        leftSource.filter((x) -> x%20==0).forward().union(rightSource).keyBy(x -> x).sum(0).print();

//        leftSource.connect(rightSource).process(new CoProcessFunction<Long, Long, Tuple2<Long, Long>>() {
//
//            private final ArrayList<Long> list1 = new ArrayList<>();
//            private final ArrayList<Long> list2 = new ArrayList<>();
//
//            @Override
//            public void processElement1(
//                    Long value,
//                    Context ctx,
//                    Collector<Tuple2<Long, Long>> out) throws Exception {
//                list1.add(value);
//                for (Long aLong : list2) {
//                    out.collect(new Tuple2<Long, Long>(value, aLong));
//                }
//            }
//
//            @Override
//            public void processElement2(
//                    Long value,
//                    Context ctx,
//                    Collector<Tuple2<Long, Long>> out) throws Exception {
//                list2.add(value);
//                for (Long aLong : list1) {
//                    out.collect(new Tuple2<Long, Long>(value, aLong));
//                }
//            }
//        }).map(x -> x.f0*x.f1).windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(1000))).sum(0).print();

        // execute program
          env.execute("Reshape Example");
//        Thread.sleep(5000);
//        jobClient.pause();
//        Thread.sleep(20000);
//        jobClient.resume();
//        jobClient.getJobExecutionResult().get();
    }


//    static class MyWatermarkStrategy implements WatermarkStrategy<Long> {
//        @Override
//        public WatermarkGenerator<Long> createWatermarkGenerator(
//                WatermarkGeneratorSupplier.Context context) {
//            return new WatermarkGenerator<Long>(){
//                long current = 0;
//                @Override
//                public void onEvent(
//                        Long event,
//                        long eventTimestamp,
//                        WatermarkOutput output) {
//                    current++;
//                }
//
//                @Override
//                public void onPeriodicEmit(WatermarkOutput output) {
//                    output.emitWatermark(new Watermark(current));
//                }
//            };
//        }
//    }


    public static class StreamDataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws InterruptedException {
            long count = 0;
            while (running && count < 10000) {
                ctx.collect(new Tuple2<>(1L, 1L));
                for(int i=0;i<10;++i){
                    ctx.collect(new Tuple2<>(2L, 1L));
                }
                ctx.collect(new Tuple2<>(3L, 1L));
                ctx.collect(new Tuple2<>(4L, 1L));
                count++;
            }
            ctx.close();
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    public static class StreamDataSource1 extends RichParallelSourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws InterruptedException {

            int count = 0;
            while (running && count < 30) {
                ctx.collect((long)count%3);
                count++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
