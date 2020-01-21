package com.pfpt.udemy.link;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Assignment1 {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> data = env.readTextFile("/Users/kwoods/workspaces/udemy-flink/cab-flink.txt");
		DataStream<Tuple2<String, Integer>> destinations = data.map(new DestTokenizer());
		DataStream<Tuple2<String, Integer>> counts = destinations.keyBy(0).reduce(new DestCounter());
		counts.print();
		counts.writeAsText("/tmp/out1");

		DataStream<Tuple3<String, Integer, Integer>> passengers = data.map(new PassengerTokenizer());
		DataStream<Tuple3<String, Integer, Integer>> passengerCounts = passengers.keyBy(0)
				.reduce(new PassengerCounter());
		DataStream<Tuple2<String, Double>> avgPassengerByOrigin = passengerCounts
				.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
					public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> input) {
						return new Tuple2<String, Double>(input.f0, new Double((input.f1 * 1.0) / input.f2));
					}
				});

		avgPassengerByOrigin.print();
		avgPassengerByOrigin.writeAsText("/tmp/out2");
		

		DataStream<Tuple3<String, Integer, Integer>> trips = data.map(new TripTokenizer());
		DataStream<Tuple3<String, Integer, Integer>> tripsCounts = trips.keyBy(0)
				.reduce(new TripCounter());
		DataStream<Tuple2<String, Double>> avgTrips = tripsCounts
				.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
					public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> input) {
						return new Tuple2<String, Double>(input.f0, new Double((input.f1 * 1.0) / input.f2));
					}
				});
		

		avgTrips.print();
		avgTrips.writeAsText("/tmp/out3");
//		DataStream<Tuple8<String, String, String, String, Boolean, String, String, Integer>> mapped = data
//				.map(new Splitter());
//
//		DataStream<Tuple8<String, String, String, String, Boolean, String, String, Integer>> reduced = mapped.keyBy(6)
//				.reduce(new Reduce1());
//
//		DataStream<Tuple8<String, String, String, String, Boolean, String, String, Integer>> counts = mapped.keyBy(6)
//				.sum(1);

		env.execute("Assignment1");
//		DataStream<Tuple8<String, String, String, String, Boolean, String, String, Integer>> reduced = mapped.keyBy(6).reduce(new Reduce1());
//

	}

}

final class DestCounter implements ReduceFunction<Tuple2<String, Integer>> {

	public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
			throws Exception {
		return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
	}

}

final class DestTokenizer implements MapFunction<String, Tuple2<String, Integer>> {

	public Tuple2<String, Integer> map(String value) {
		String[] words = value.split(",");
		return new Tuple2(words[6], Integer.valueOf(1));
	}
}

final class PassengerCounter implements ReduceFunction<Tuple3<String, Integer, Integer>> {

	public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1,
			Tuple3<String, Integer, Integer> value2) throws Exception {
		return new Tuple3<String, Integer, Integer>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
	}

}

final class PassengerTokenizer implements MapFunction<String, Tuple3<String, Integer, Integer>> {

	public Tuple3<String, Integer, Integer> map(String value) {
		String[] words = value.split(",");
		String noPassengers = words[7];
		if (noPassengers.equals("'null'")) {
			noPassengers = "0";
		}
		return new Tuple3(words[5], Integer.valueOf(noPassengers), Integer.valueOf(1));
	}
}
final class TripTokenizer implements MapFunction<String, Tuple3<String, Integer, Integer>> {

	public Tuple3<String, Integer, Integer> map(String value) {
		String[] words = value.split(",");
		String noPassengers = words[7];
		if (noPassengers.equals("'null'")) {
			noPassengers = "0";
		}
		return new Tuple3(words[3], Integer.valueOf(noPassengers), Integer.valueOf(1));
	}
}
final class TripCounter implements ReduceFunction<Tuple3<String, Integer, Integer>> {

	public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1,
			Tuple3<String, Integer, Integer> value2) throws Exception {
		return new Tuple3<String, Integer, Integer>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
	}

}
final class Reduce1
		implements ReduceFunction<Tuple8<String, String, String, String, Boolean, String, String, Integer>> {
	public Tuple8<String, String, String, String, Boolean, String, String, Integer> reduce(
			Tuple8<String, String, String, String, Boolean, String, String, Integer> current,
			Tuple8<String, String, String, String, Boolean, String, String, Integer> pre_result) {
		return new Tuple8<String, String, String, String, Boolean, String, String, Integer>(current.f0, current.f1,
				current.f2, current.f3, current.f4, current.f5, current.f6, current.f7);
	}
}

final class Splitter
		implements MapFunction<String, Tuple8<String, String, String, String, Boolean, String, String, Integer>> {

	public Tuple8<String, String, String, String, Boolean, String, String, Integer> map(String value) throws Exception {
		String[] words = value.split(",");
		return new Tuple8<String, String, String, String, Boolean, String, String, Integer>(words[0], words[1],
				words[2], words[3], Boolean.getBoolean(words[4]), words[5], words[6], Integer.parseInt(words[7]));
	}
}