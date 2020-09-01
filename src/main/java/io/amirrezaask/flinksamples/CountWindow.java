package io.amirrezaask.flinksamples;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CountWindow {
    public static void main(String[] args) throws Exception {
        // create env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // using RocksDBState as the state backend.
        final StateBackend state = BackendFactory.getRocksDBBackend("file:///home/amirreza/rocksState/");

        // set state backend in env.
        env.setStateBackend(state);

        env
                .fromElements(new TimedEvent(1, "Amirreza"), new TimedEvent(2, "Amirreza"))
                .countWindowAll(2)
                .process(new EventCounter())
                .print();

        env.execute();
    }
}

class EventCounter extends ProcessAllWindowFunction<TimedEvent, Integer, GlobalWindow> {
    private transient MapState<String, Integer> state;

    @Override
    public void open(Configuration parameters) {
        this.state = getRuntimeContext().getMapState(new MapStateDescriptor<>("timed-windows-state", String.class, Integer.class));
    }
    @Override
    public void process(Context context, Iterable<TimedEvent> iterable, Collector<Integer> collector) throws Exception {
        iterable.forEach((event) -> {
            try {
                Integer counter = this.state.get(event.getName());
                if (counter == null) {
                    counter = 0;
                }
                Integer newValue = counter + 1;
                state.put(event.getName(), newValue);
                collector.collect(newValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}

class TimedEvent {


    private final int id;
    private final String name;

    TimedEvent(int id, String name) {
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
