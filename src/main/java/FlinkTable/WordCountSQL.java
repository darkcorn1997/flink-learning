package FlinkTable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;

public class WordCountSQL {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //获得一个table的上下文
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);


        //WC数据格式，javabean
        List<WC> list = new ArrayList<>();
        String wordStr = "To be, or not to be, that is the question:\n" +
                "Whether 'tis nobler in the mind to s uffer\n" +
                "The slings and arrows of outrageous fortune,\n" +
                "Or to take arms against a sea of troubles\n" +
                "And by opposing end them.";
        String[] words = wordStr.split("\\W+");
        for (String word : words ){
            WC wc = new WC(word, 1);
            list.add(wc);
        }

        //source: get input data
        DataSet<WC> input = env.fromCollection(list);

        //注册成flink table 内存表
        Table table = tEnv.fromDataSet(input, "word, frequency");
        tEnv.createTemporaryView("WordCount", table);

        //注册自定义函数
        tEnv.createTemporarySystemFunction("StringToSite", new StringToSite("com"));

        //查询 返回一个table
        Table table1 = tEnv.sqlQuery("SELECT StringToSite(word) as word, SUM(frequency) as frequency " +
                "FROM WordCount GROUP BY word");

        //把table映射成javabean类型的dataset并打印
        DataSet<WC> result = tEnv.toDataSet(table1, WC.class);
        result.print();
    }

    //自定义函数 StringToSite : hello->hello.com
    //1.继承ScalarFunction  2.复写eval方法  3.注册函数  4.应用
    public static class StringToSite extends ScalarFunction {
        private final String address;
        public StringToSite(String s) {
            address = s;
        }

        public String eval(String s) {
            return s + address;
        }
    }

    //javabean
    public static class WC {
        public String word;
        public long frequency;

        public WC() {}
        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return word + ", " + frequency;
        }
    }

}
