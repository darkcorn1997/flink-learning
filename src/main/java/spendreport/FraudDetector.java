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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	// 最基础的状态类型 ValueState, 只能被用于keyed context提供的操作中
	// ValueState是一个包装类 update:更新状态  value:获取状态值  clear:清空状态。
	private transient ValueState<Boolean> flagState;  //记录交易的状态
	private transient ValueState<Long> timerState; //记录定时器的状态

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	// ValueState 需要使用 ValueStateDescriptor 来创建
	// 状态在使用之前需要先使用 open() 函数来注册
	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<Long>("timer-state", Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);

	}

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// 获得当前的交易状态
		Boolean lastTransactionWasSmall = flagState.value();

		// 检查状态是否为空
		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				// 超出最大金额 输出警报
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());

				collector.collect(alert);
			}
			// 清空状态
			//flagState.clear();
			cleanup(context);
		}

		if (transaction.getAmount() < SMALL_AMOUNT) {
			// 设置状态
			flagState.update(true);

			// 设置一个1m定时器
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerEventTimeTimer(timer);
			//记录定时器状态
			timerState.update(timer);
		}
	}

	//定时器触发时，将会调用KeyedProcessFunction#onTimer方法
	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
		// 1m后清空交易状态和定时器状态
		timerState.clear();
		flagState.clear();
	}

	private void cleanup(Context context) throws Exception {
		// delete timer
		Long timer = timerState.value();
		context.timerService().deleteProcessingTimeTimer(timer);

		// clean up all state
		timerState.clear();
		flagState.clear();
	}
}
