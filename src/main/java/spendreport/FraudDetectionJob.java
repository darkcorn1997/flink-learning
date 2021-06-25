/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {
		// 执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// source；绑定name属性以方便调试
		DataStream<Transaction> transactions = env
			.addSource(new TransactionSource())
			.name("transactions");

		// keyBy基于账户对流分区
		// process()函数对流绑定了一个操作，这个操作将会对流上的每一个消息调用所定义好的函数。
		// 通常一个操作会紧跟着keyBy被调用，在这个例子中，这个操作是FraudDetector，该操作是在一个keyed context上执行的。
		DataStream<Alert> alerts = transactions
			.keyBy(Transaction::getAccountId)
			.process(new FraudDetector())
			.name("fraud-detector");

		// sink 会将 DataStream 写出到外部系统，例如 Apache Kafka 等。
		// AlertSink 使用 INFO 的日志级别打印每一个 Alert 的数据记录，而不是将其写入持久存储，以便你可以方便地查看结果。
		alerts
			.addSink(new AlertSink())
			.name("send-alerts");

		// 运行作业
		env.execute("Fraud Detection");
	}
}
