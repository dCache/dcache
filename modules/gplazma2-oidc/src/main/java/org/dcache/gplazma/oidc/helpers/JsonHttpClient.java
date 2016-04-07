package org.dcache.gplazma.oidc.helpers;

import com.google.common.base.Charsets;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JsonHttpClient
{
    private HttpClient httpclient;

    public JsonHttpClient()
    {
        this.httpclient = HttpClients.createDefault();
    }

    public JsonNode doGet(String url) throws IOException
    {
        return this.doGet(url, null);
    }

    public JsonNode doGet(String url, Header header) throws IOException
    {
        HttpGet httpGet = new HttpGet(url);
        if (header != null) {
            httpGet.addHeader(header);
        }
        HttpResponse response = httpclient.execute(httpGet);
        HttpEntity entity = response.getEntity();

        if (entity != null) {
            return responseAsJson(entity);
        } else {
            return null;
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

    private static JsonNode responseAsJson(HttpEntity response) throws IOException
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        response.writeTo(os);
        String responseAsJson =  new String(os.toByteArray(), Charsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readValue(responseAsJson, JsonNode.class);
        return json;
    }
}
