package org.dcache.net;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.stream.Collectors;
import org.dcache.net.FlowMarker.FlowMarkerBuilder;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FlowMarkerTest {

    private static Schema SCHEMA;

    @BeforeClass
    public static void init() throws IOException {

        try (InputStream is = Thread.currentThread().getContextClassLoader()
              .getResourceAsStream("org/dcache/net/firefly.schema.json")) {

            JSONObject rawSchema = new JSONObject(new JSONTokener(is));
            SCHEMA = SchemaLoader.builder()
                  .schemaJson(rawSchema)
                  .draftV7Support()
                  .build().load().build(); // Wow!
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithIncorrectState() {
        new FlowMarkerBuilder()
              .build("foo");
    }


    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithIncorrectProtocol() {
        new FlowMarkerBuilder()
              .withProtocol("foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithIncorrectAFI() {
        new FlowMarkerBuilder()
              .withAFI("foo");
    }

    @Test
    public void shouldAcceptFullyBuildStartMarker() {
        var data = new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withExperimentId(2)
              .withActivityId(1)
              .wittApplication("curl")
              .withProtocol("tcp")
              .withAFI("ipv4")
              .withDestination(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234))
              .withSource(new InetSocketAddress(InetAddress.getLoopbackAddress(), 5678))
              .build("start");

        assertJson(data);
    }

    @Test
    public void shouldAcceptFullyBuildEndMarker() {
        var data = new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withFinishedAt(Instant.now())
              .withExperimentId(2)
              .withActivityId(1)
              .wittApplication("curl")
              .withProtocol("tcp")
              .withAFI("ipv4")
              .withDestination(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234))
              .withSource(new InetSocketAddress(InetAddress.getLoopbackAddress(), 5678))
              .build("end");

        assertJson(data);
    }

    @Test
    public void shouldAcceptFullyBuildOngoingMarker() {
        var data = new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withExperimentId(2)
              .withActivityId(1)
              .wittApplication("curl")
              .withProtocol("tcp")
              .withAFI("ipv4")
              .withDestination(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234))
              .withSource(new InetSocketAddress(InetAddress.getLoopbackAddress(), 5678))
              .build("ongoing");

        assertJson(data);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailWithEmptyMarker() {
        var data = new FlowMarkerBuilder()
              .build("start");

        assertJson(data);
    }


    private void assertJson(JSONObject json) {
        try {
            SCHEMA.validate(json);
        } catch (ValidationException e) {
            var msg = e.getCausingExceptions().stream()
                  .map(ValidationException::getMessage)
                  .collect(Collectors.joining(","));

            Assert.fail(e.getMessage() + " : " + msg);
        }
    }
}