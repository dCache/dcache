package org.dcache.restful.providers;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.json.JSONObject;

@Provider
public class SuccessfulResponse {

    public static Response successfulResponse(Response.Status status) {
        JSONObject json = new JSONObject();
        json.put("status", "success");

        return Response
              .status(status)
              .entity(json.toString())
              .type(MediaType.APPLICATION_JSON)
              .build();
    }
}
