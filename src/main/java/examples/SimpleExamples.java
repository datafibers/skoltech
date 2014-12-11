package examples;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SimpleExamples {

	public static void main (String [] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<String> someStrings = env.fromElements("a", "b", "c");

		someStrings.print();

		// With Java 8 lambdas
		// someStrings.map(s -> {return s + s;}).print();
		someStrings.map(new MapFunction<String, String>() {
			@Override
			public String map(String s) throws Exception {
				return s + s;
			}
		}).print();


		DataSet<String> someData = someStrings.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String s, Collector<String> out) throws Exception {
				out.collect(s + s);
				out.collect(s);
			}
		});

		someData.print();

		DataSet<Tuple2<Integer,String>> stringsWithKeys = someData
				.map(new MapFunction<String, Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, String> map(String s) throws Exception {
						return new Tuple2(s.charAt(0  ) - 'a' + 1, s);
					}
				});

		stringsWithKeys.print();

		stringsWithKeys.groupBy(0)
				.reduce(new ReduceFunction<Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, String> reduce(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
						return new Tuple2(first.f0, first.f1 + second.f1);
					}
				}).print();



		env.execute();

	}

}
