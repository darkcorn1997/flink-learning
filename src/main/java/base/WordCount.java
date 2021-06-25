package base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        //set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //source: get input data
        DataSet<String> text = env.fromElements("To be, or not to be, that is the question:\n" +
                "Whether 'tis nobler in the mind to s uffer\n" +
                "The slings and arrows of outrageous fortune,\n" +
                "Or to take arms against a sea of troubles\n" +
                "And by opposing end them.");

        //transform
        //AggregateOperator向上转型为Dataset
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new LineSplitter())
                //聚合操作 根据第一个field key
                .groupBy(0)
                //第二个field value
                .sum(1);

        //sink
        counts.print();
    }


    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //用非单词字符分割为string数组
            String[] tokens = value.toLowerCase().split("\\W+");

            //对数组遍历，包装成格式为 key：token，value：1的tuple然后收集起来
            for (String token:tokens) {
                if  (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
