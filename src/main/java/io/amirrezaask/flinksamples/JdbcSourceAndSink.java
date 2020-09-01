package io.amirrezaask.flinksamples;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;

public class JdbcSourceAndSink {
    private static final String DBURL = "jdbc:mysql://localhost:3306/flinksampler";
    public static void main(String[] args) throws Exception {
        // create env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // using RocksDBState as the state backend.
        final StateBackend state = BackendFactory.getRocksDBBackend("./state/");

        // set state backend in env.
        env.setStateBackend(state);

        // Create JDBC input to be fed into environment.
        JdbcInputFormat jdbcInput = JdbcInputFormat
            .buildJdbcInputFormat()
            .setDBUrl(DBURL)
            .setPassword("toor")
            .setUsername("root")
            .setDrivername("mysql")
            .setQuery("SELECT id FROM sometable")
            .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO))
            .finish();
        // Create Jdbc Options to pass into Sink.
        JdbcOptions options = JdbcOptions.builder().
                setTableName("output").
                setDriverName("mysql").
                setUsername("root").
                setPassword("toor").
                setDBUrl(DBURL).
                build();


        // our data source from jdbc that we already configured
        DataStreamSource<Row> source = env.createInput(jdbcInput);

        // transform our database rows into Event objects and then count names and then print all events
        source
                .flatMap(new TransformToEvent())
                .flatMap(new UserCounter())
                .addSink(JdbcSink.sink("INSERT INTO processed (id, name) VALUES (?,?)", (ps, t) -> {
                    ps.setInt(1, t.getId());
                    ps.setString(2,  t.getName());
                }, options));

        // execute our application
        env.execute();
        
    }

}

class Event {
    private final int id;
    private final String name;

    public Event(int id, String name) {
        this.id = id;
        this.name = name;
    }
    public String getName() {
        return this.name;
    }
    public Integer getId() {
        return this.id;
    }
}

class TransformToEvent extends RichFlatMapFunction<Row, Event> { // first generic argument to Richflatmapfunction shows the input and the second is the output type of the processor.

    @Override
    public void flatMap(Row row, Collector<Event> collector) {
        String name = (String) row.getField(2);
        Integer id = (Integer) row.getField(1);
        collector.collect(new Event(id, name));
    }
}

class UserCounter extends RichFlatMapFunction<Event, Event> {
    private transient MapState<String, AtomicInteger> state;
    @Override
    public void flatMap(Event event, Collector<Event> collector) throws Exception {
        state.get(event.getName()).addAndGet(1);
        collector.collect(event);
    }
}