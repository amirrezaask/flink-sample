package io.amirrezaask.flinksamples;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;

public class TimedWindows {
    public static void main(String[] args) throws Exception {
        // create env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // using RocksDBState as the state backend.
        final StateBackend state = BackendFactory.getRocksDBBackend("./state/");

        // set state backend in env.
        env.setStateBackend(state);

        env
                .fromElements(new TimedEvent(1, "Amirreza"))
                .keyBy(TimedEvent::getId)
                .timeWindow(Time.milliseconds(1))
                .process(new EventCounter())
                .print();

        env.execute();
    }
}

class EventCounter extends ProcessWindowFunction<TimedEvent, TimedEvent, Integer, TimeWindow> {
    private transient MapState<String, AtomicInteger> state;

    @Override
    public void process(Integer key, Context context, Iterable<TimedEvent> iterable, Collector<TimedEvent> collector) throws Exception {
        iterable.forEach((event) -> {
            try {
                state.get(event.getName()).addAndGet(1);
                collector.collect(event);
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
