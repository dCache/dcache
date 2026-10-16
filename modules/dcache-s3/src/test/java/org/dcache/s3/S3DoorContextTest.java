package org.dcache.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.env.MapPropertySource;

/**
 * Starts the door's Spring context, outside a cell, and talks HTTP to it.
 */
public class S3DoorContextTest {

    private GenericXmlApplicationContext context;
    private HttpClient client;
    private URI endpoint;

    @Before
    public void setup() throws Exception {
        Path keyFile = Files.createTempFile("s3-keys", ".conf");
        Files.writeString(keyFile, "AKIAEXAMPLE secret\n");
        keyFile.toFile().deleteOnExit();

        context = new GenericXmlApplicationContext();
        context.getEnvironment().getPropertySources()
              .addFirst(new MapPropertySource("test", properties(keyFile)));
        context.load("classpath:org/dcache/s3/s3.xml");
        context.refresh();

        Server jetty = context.getBean("jetty", Server.class);
        int port = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
        endpoint = URI.create("http://127.0.0.1:" + port);
        client = HttpClient.newHttpClient();
    }

    @After
    public void tearDown() {
        context.close();
    }

    @Test
    public void shouldRefuseUnsignedRequest() throws Exception {
        HttpResponse<String> response = get("/bucket/key", Map.of());

        assertThat(response.statusCode(), is(403));
        assertThat(response.body(), containsString("<Code>AccessDenied</Code>"));
    }

    @Test
    public void shouldRefuseUnknownAccessKey() throws Exception {
        String now = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
              .withZone(ZoneOffset.UTC).format(Instant.now());
        HttpResponse<String> response = get("/bucket/key", Map.of(
              "Authorization", "AWS4-HMAC-SHA256 Credential=AKIAUNKNOWN/" + now.substring(0, 8)
                    + "/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000",
              "x-amz-date", now,
              "x-amz-content-sha256", "UNSIGNED-PAYLOAD"));

        assertThat(response.statusCode(), is(403));
        assertThat(response.body(), containsString("<Code>InvalidAccessKeyId</Code>"));
    }

    @Test
    public void shouldAnswerHealthCheck() throws Exception {
        assertThat(get("/healthz", Map.of()).statusCode(), is(200));
    }

    private HttpResponse<String> get(String path, Map<String, String> headers)
          throws Exception {
        var request = HttpRequest.newBuilder(endpoint.resolve(path)).GET();
        headers.forEach(request::header);
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static Map<String, Object> properties(Path keyFile) {
        var p = new HashMap<String, Object>();
        p.put("s3.root", "/data/s3");
        p.put("s3.net.port", "0");
        p.put("s3.net.listen", "127.0.0.1");
        p.put("s3.net.internal", "127.0.0.1");
        p.put("s3.net.virtual-host", "");
        p.put("s3.authn.connector", "PLAIN");
        p.put("s3.authn.signature", "aws-v2-or-v4");
        p.put("s3.authn.max-time-skew", "900");
        p.put("s3.authn.max-time-skew.unit", "SECONDS");
        p.put("s3.authn.keystore.file", keyFile.toString());
        p.put("s3.authn.ciphers", "");
        p.put("s3.authn.capath", "/etc/grid-security/certificates");
        p.put("s3.authn.capath.refresh", "60");
        p.put("s3.authn.capath.refresh.unit", "SECONDS");
        p.put("s3.authn.hostcert.cert", "/etc/grid-security/hostcert.pem");
        p.put("s3.authn.hostcert.key", "/etc/grid-security/hostkey.pem");
        p.put("s3.authn.hostcert.refresh", "60");
        p.put("s3.authn.hostcert.refresh.unit", "SECONDS");
        p.put("s3.authn.namespace-mode", "GLOBUS_EUGRIDPMA");
        p.put("s3.authn.crl-mode", "IF_VALID");
        p.put("s3.authn.ocsp-mode", "IF_AVAILABLE");
        for (String service : new String[]{"pnfsmanager", "poolmanager", "pinmanager",
              "gplazma"}) {
            p.put("s3.service." + service, service);
            p.put("s3.service." + service + ".timeout", "1000");
            p.put("s3.service." + service + ".timeout.unit", "MILLISECONDS");
        }
        p.put("s3.service.pool.timeout", "1000");
        p.put("s3.service.pool.timeout.unit", "MILLISECONDS");
        p.put("s3.service.billing", "billing");
        p.put("s3.service.gplazma.cache.size", "10");
        p.put("s3.service.gplazma.cache.timeout", "60");
        p.put("s3.service.gplazma.cache.timeout.unit", "SECONDS");
        p.put("s3.mover.queue", "");
        for (String timeout : new String[]{"timeout", "read-timeout",
              "transfer-confirmation-timeout", "kill-timeout"}) {
            p.put("s3.mover." + timeout, "1000");
            p.put("s3.mover." + timeout + ".unit", "MILLISECONDS");
        }
        p.put("s3.limits.list.max-entries", "100");
        p.put("s3.limits.multipart.max-age", "7");
        p.put("s3.limits.multipart.max-age.unit", "DAYS");
        p.put("s3.restore.max-days", "30");
        p.put("s3.loginbroker.update-topic", "LoginBrokerTopic");
        p.put("s3.loginbroker.update-period", "5");
        p.put("s3.loginbroker.update-period.unit", "SECONDS");
        p.put("s3.loginbroker.update-threshold", "0.1");
        p.put("s3.loginbroker.version", "1.0.0");
        p.put("s3.loginbroker.family", "s3");
        p.put("s3.loginbroker.address", "");
        p.put("s3.loginbroker.port", "9000");
        p.put("s3.loginbroker.tags", "");
        p.put("s3.loginbroker.root", "/data/s3");
        p.put("s3.limits.threads.max", "20");
        p.put("s3.limits.threads.min", "1");
        p.put("s3.limits.threads.idle-time", "60");
        p.put("s3.limits.threads.idle-time.unit", "SECONDS");
        p.put("s3.limits.queue-length", "100");
        p.put("s3.limits.acceptors", "1");
        p.put("s3.limits.backlog", "16");
        p.put("s3.limits.idle-time", "30");
        p.put("s3.limits.idle-time.unit", "SECONDS");
        p.put("s3.limits.graceful-shutdown", "0");
        p.put("s3.limits.graceful-shutdown.unit", "SECONDS");
        return p;
    }
}
