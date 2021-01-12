package com.panli.examples.streaming.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lipan
 * @date 2021-01-10
 * @desc Flink Streaming WordCount by socket
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // 参数校验
        if (args == null || args.length != 2) {
            System.err.println("USAGE: \n Flink Streaming WordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        Integer port = Integer.valueOf(args[1]);

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        // 计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumWords = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = s.toLowerCase().split("\\W+");
                for (String word : words) {
                    if (word.length() > 0) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }
            }
        }).keyBy(0).sum(1);

        // 打印控制台
        sumWords.print();

        //
        env.execute("Java WordCount from SocketTextStream");
    }
}
