package diskCacheV111.poolManager;

import java.net.UnknownHostException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(value = 2, warmups = 2)
@Measurement(iterations = 2, time = 5)
@Warmup(iterations = 2, time = 2)
public class NetHandlerBenchmark {

    private NetHandler nh;

    @Param({"192.168.1.1", "fe80::9cef:10f5:f2ae:1aa1", "2001:638:700:10c0::1:82"})
    private String client;

    NetUnit n;
    @Setup
    public void setUp() {

        nh = new NetHandler();
        nh.add(new NetUnit("0.0.0.0/0.0.0.0"));
        nh.add(new NetUnit("::/0"));
        n = new NetUnit("2001:638:700:10c0::1:82");
    }

    @Benchmark
    public NetUnit match() throws UnknownHostException {
        return nh.match(client);
    }

    @Benchmark
    public NetUnit find() throws UnknownHostException {
        return nh.find(n);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
              .include(NetHandlerBenchmark.class.getSimpleName())
              .build();

        new Runner(opt).run();
    }
}
