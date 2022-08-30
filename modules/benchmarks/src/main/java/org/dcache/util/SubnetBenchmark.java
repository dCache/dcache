package org.dcache.util;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 *
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
public class SubnetBenchmark {

    private InetAddress address;
    private Subnet subnet;

    @Param({"192.168.1.1", "192.168.1.0/24", "fe80::9cef:10f5:f2ae:1aa1", "fe80::9cef:10f5:f2ae:1aa1/48"})
    private String template;

    @Param({"192.168.1.1", "192.168.5.1", "fe80::9cef:10f5:f2ae:1aa1", "fe80:cd00:0:cde:1257:0:211e:729c"})
    private String client;

    @Setup
    public void setUp() {
        subnet = Subnet.create(template);
        address = InetAddresses.forString(client);
    }

    @Benchmark
    public boolean subnetContains() {
        return subnet.contains(address);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
              .include(SubnetBenchmark.class.getSimpleName())
              .build();

        new Runner(opt).run();
    }

}
