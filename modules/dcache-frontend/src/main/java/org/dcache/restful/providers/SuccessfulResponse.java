package org.dcache.restful.providers;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
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
