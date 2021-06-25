package FlinkTable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

public class TableSQL {
    public static void main(String[] args) throws Exception {
        //1.获取上下文环境 包括table环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        //2.读取csv文件
        DataSet<String> input = env.readTextFile("score.csv");
        DataSet<PlayerData> topInput = input.map(new MapFunction<String, PlayerData>() {
            @Override
            public PlayerData map(String value) throws Exception {
                String[] split = value.split(",");
                return new PlayerData(String.valueOf(split[0]),
                        String.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        Double.valueOf(split[3]),
                        Double.valueOf(split[4]),
                        Double.valueOf(split[5]),
                        Double.valueOf(split[6])
                );
            }
        });

        //3.注册成内存表
        Table topScore = tEnv.fromDataSet(topInput);
        tEnv.createTemporaryView("score", topScore);

        //4.sql执行
        Table result = tEnv.sqlQuery("select player, count(season) as num " +
                "from score group by player order by num desc");

        //5.把table映射成javabean类型的dataset并打印
        DataSet<Result> resultDataSet = tEnv.toDataSet(result, Result.class);
        resultDataSet.print();
    }


    //javabean
    public static class PlayerData {
        public String season;
        public String player;
        public Integer times;
        public Double assists;
        public Double steals;
        public Double blocks;
        public Double scores;

        public PlayerData() {}
        public PlayerData(String season, String player, Integer times,
                          Double assists, Double steals, Double blocks, Double scores) {
            this.season=season;
            this.player=player;
            this.times =times;
            this.assists=assists;
            this.steals=steals;
            this.blocks=blocks;
            this.scores=scores;
        }
        @Override
        public String toString() {
            return player;
        }
    }

    public static class Result {
        public String player;
        public Long num;

        public Result() {}
        public Result(String player,Long num ) {
            this.player=player;
            this.num=num;
        }

        @Override
        public String toString() {
            return player + ":" + num;
        }
    }
}
