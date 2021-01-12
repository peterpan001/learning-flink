package com.panli.examples.batch.file;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author lipan
 * @date 2021-01-11
 * @desc 批处理统计单词个数，source来源于文件
 */
public class Main {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("/Users/lipan/workcode/flink-code/learning-flink/learning-flink-examples/src/main/resources/word.txt").flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = s.toLowerCase().split("\\W+");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
