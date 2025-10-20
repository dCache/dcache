package org.dcache.restful.resources.auth;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Path("/auth")
@Component
@Produces(MediaType.APPLICATION_JSON)
public class OidcCodeFlowCallback {


    private static final Logger LOGGER = LoggerFactory.getLogger(OidcCodeFlowCallback.class);


    public String oidcClientId;
    public String oidcClientSecret;
    public String oidcTokenUrl;
    public String oidcRedirectUrl;
    public String oidcRedirectUrlHost;



    public void setClientId(String oidcClientId) {
        this.oidcClientId = oidcClientId;
    }

    public void setClientSecret(String oidcClientSecret) {
        this.oidcClientSecret = oidcClientSecret;
    }

    public void setTokenUrl(String oidcTokenUrl) {
        this.oidcTokenUrl = oidcTokenUrl;
    }

    public void setRedirectUrl(String oidcRedirectUrl) {
        this.oidcRedirectUrl = oidcRedirectUrl;
    }

    public void setRedirectUrlHost(String oidcRedirectUrlHost) {
        this.oidcRedirectUrlHost = oidcRedirectUrlHost;
    }

    @GET
    @Path("/callback")
    public Response callback(@QueryParam("code") String code) {
        try {
            String body = "&code=" + code +
                  "&grant_type=authorization_code" +
                  "&redirect_uri=" + oidcRedirectUrl;
            URL url = new URL(oidcTokenUrl);

            String basicAuth = Base64.getEncoder().encodeToString(
                  (oidcClientId + ":" + oidcClientSecret).getBytes(
                        StandardCharsets.UTF_8));
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Authorization", "Basic " + basicAuth);
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes());
            }
            // Read response
            StringBuilder responseStr = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                  new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {

                    responseStr.append(line);
                }
            }

            JSONObject resultJson = new JSONObject(responseStr.toString());
            String idToken = (String) resultJson.get("id_token");

            String redirectUrl = oidcRedirectUrlHost+"#/login-success?token=" + idToken;
            LOGGER.info("Redirect URL: " + redirectUrl);
            return Response.seeOther(URI.create(redirectUrl)).build();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            LOGGER.info(e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                  .entity(Map.of("error", e.getMessage()))
                  .build();
        }
    }

}