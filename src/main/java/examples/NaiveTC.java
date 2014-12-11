package examples;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;

public class NaiveTC {

	public static Tuple2<Long,Long> [] edgeList = new Tuple2[] {
		new Tuple2<Long,Long>(1L, 2L),
		new Tuple2<Long,Long>(2L, 3L),
		new Tuple2<Long,Long>(2L, 4L),
		new Tuple2<Long,Long>(3L, 5L),
		new Tuple2<Long,Long>(6L, 7L),
		new Tuple2<Long,Long>(8L, 9L),
		new Tuple2<Long,Long>(8L, 10L),
		new Tuple2<Long,Long>(5L, 11L),
		new Tuple2<Long,Long>(11L, 12L),
		new Tuple2<Long,Long>(10L, 13L),
		new Tuple2<Long,Long>(9L, 14L),
		new Tuple2<Long,Long>(13L, 14L),
		new Tuple2<Long,Long>(1L, 15L),
		new Tuple2<Long,Long>(16L, 1L)		
	};
	
	public static void main (String [] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long,Long>> edges = env.fromElements(edgeList);

		IterativeDataSet<Tuple2<Long,Long>> paths = edges.iterate (10);

		DataSet<Tuple2<Long,Long>> nextPaths = paths
				.join(edges).where(1).equalTo(0)
				.with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
					@Override
					/**
					 left: Path (z,x) - x is reachable by z
					 right: Edge (x,y) - edge x-->y exists
					 out: Path (z,y) - y is reachable by z
					 */
					public Tuple2<Long, Long> join(Tuple2<Long, Long> left, Tuple2<Long, Long> right) throws Exception {
						return new Tuple2<Long, Long>(left.f0, right.f1);
					}
				})
				.union(paths)
				.distinct();

		DataSet<Tuple2<Long, Long>> tc = paths.closeWith(nextPaths);

		tc.print();

		env.execute();

	}
}
