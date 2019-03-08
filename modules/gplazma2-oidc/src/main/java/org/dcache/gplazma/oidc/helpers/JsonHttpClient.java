package org.dcache.gplazma.oidc.helpers;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

import org.dcache.util.Strings;
import org.dcache.util.TimeUtils;
import org.dcache.util.Version;

public class JsonHttpClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonHttpClient.class);
    private static final Version VERSION = Version.of(JsonHttpClient.class);

    private final HttpClient httpclient;

    public JsonHttpClient(int maxConnTotal, int maxConnPerRoute, int soTimeout)
    {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setDefaultMaxPerRoute(maxConnPerRoute);
        connManager.setMaxTotal(maxConnTotal);
        connManager.setValidateAfterInactivity(60_000);

        SocketConfig.Builder socketOptions = SocketConfig.custom();
        socketOptions.setSoTimeout(soTimeout);
        connManager.setDefaultSocketConfig(socketOptions.build());


        httpclient = HttpClients.custom()
                .setConnectionManager(connManager)
                .setUserAgent("dCache/" + VERSION.getVersion())
                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                .build();
    }

    private static JsonNode responseAsJson(HttpEntity response) throws IOException
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        response.writeTo(os);
        String responseAsJson = new String(os.toByteArray(), Charsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readValue(responseAsJson, JsonNode.class);
        return json;
    }

    public JsonNode doGet(URI url) throws IOException
    {
        return this.doGet(url.toASCIIString(), null);
    }

    private JsonNode doGet(String url, Header header) throws IOException
    {
        HttpGet httpGet = new HttpGet(url);
        if (header != null) {
            httpGet.addHeader(header);
        }
        LOGGER.debug("Making GET request on {}", url);
        Stopwatch httpTiming = Stopwatch.createStarted();
        HttpResponse response = httpclient.execute(httpGet);
        HttpEntity entity = response.getEntity();

        try {
            return entity == null ? null : responseAsJson(entity);
        } finally {
            if (LOGGER.isDebugEnabled()) {
                httpTiming.stop();

                LOGGER.debug("GET {} took {} returning {} entity: {}",
                        url,
                        TimeUtils.describe(httpTiming.elapsed()).orElse("(no time)"),
                        entity == null ? "unknown"
                                : entity.getContentLength() < 0 ? "unknown sized"
                                        : entity.getContentLength() == 0 ? "no"
                                                : Strings.describeSize(entity.getContentLength()),
                        response.getStatusLine());
            }
        }
    }

    public JsonNode doGetWithToken(String url, String token) throws IOException
    {
        if (token != null && !token.isEmpty()) {
            return doGet(url, new BasicHeader("Authorization", "Bearer " + token));
        } else {
            return null;
        }
    }
}
