package org.dcache.restful.qos;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.qos.QoSTransitionEngine.Qos;
import org.dcache.restful.util.RequestUser;

import static org.dcache.qos.QoSTransitionEngine.Qos.*;

/**
 * RestFul API for querying and manipulating QoS
 */
@Component
@Api(value = "qos", authorizations = {@Authorization("basicAuth")})
@Path("/qos-management/qos/")
public class QosManagement {

    @Inject
    @Named("geographic-placement")
    private List<String> geographicPlacement;

    @GET
    @ApiOperation("List the available quality of services for a specific object "
            + "type.  Requires authentication.")
    @ApiResponses({
                @ApiResponse(code = 401, message = "Unauthorized."),
                @ApiResponse(code = 403, message = "Forbidden."),
                @ApiResponse(code = 404, message = "Not found."),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Path("{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getQosList(@ApiParam(value = "The kind of object to query.",
                                     allowableValues="file,directory")
                             @PathParam("type") String qosValue) throws CacheException {

        JSONObject json = new JSONObject();


        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            if ("file".equals(qosValue)) {
                JSONArray list = new JSONArray(Arrays.asList(DISK.displayName(),
                                                             TAPE.displayName(),
                                                             DISK_TAPE.displayName(),
                                                             VOLATILE.displayName()));
                json.put("name", list);
            } else if ("directory".equals(qosValue.trim())) {
                JSONArray list = new JSONArray(Arrays.asList(DISK.displayName(),
                                                             TAPE.displayName(),
                                                             DISK_TAPE.displayName(),
                                                             VOLATILE.displayName()));
                json.put("name", list);
            } else {
                throw new NotFoundException();
            }

            json.put("status", "200");
            json.put("message", "successful");

        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        }

        return json.toString();
    }


    @GET
    @ApiOperation("Provide information about a specific file quality of "
            + "services.  Requires authentication.")
    @ApiResponses({
                @ApiResponse(code = 401, message = "Unauthorized"),
                @ApiResponse(code = 403, message = "Forbidden"),
                @ApiResponse(code = 404, message = "Not found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Path("/file/{qos}")
    @Produces(MediaType.APPLICATION_JSON)
    public BackendCapabilityResponse getQueriedQosForFiles(
            @ApiParam("The file quality of service to query.")
            @PathParam("qos") String qosValue) throws CacheException {

        BackendCapabilityResponse backendCapabilityResponse
                        = new BackendCapabilityResponse();

        BackendCapability backendCapability = new BackendCapability();

        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            backendCapabilityResponse.setStatus("200");
            backendCapabilityResponse.setMessage("successful");

            QoSMetadata qoSMetadata;

            switch (Qos.fromDisplayName(qosValue))
            {
                case DISK:
                    qoSMetadata = new QoSMetadata("1",
                                                  geographicPlacement,
                                                  "100");
                    setBackendCapability(backendCapability, DISK.displayName(),
                                         Arrays.asList(TAPE.displayName(),
                                                       DISK_TAPE.displayName()),
                                         qoSMetadata);
                    break;
                case TAPE:
                    qoSMetadata = new QoSMetadata("1",
                                                  geographicPlacement,
                                                  "600000");
                    setBackendCapability(backendCapability, TAPE.displayName(),
                                         Arrays.asList(DISK_TAPE.displayName()),
                                         qoSMetadata);
                    break;
                case DISK_TAPE:
                    qoSMetadata = new QoSMetadata("2",
                                                  geographicPlacement,
                                                  "100");
                    setBackendCapability(backendCapability, DISK_TAPE.displayName(),
                                         Arrays.asList(TAPE.displayName()),
                                         qoSMetadata);
                    break;
                case VOLATILE:
                    qoSMetadata = new QoSMetadata("0",
                                                  geographicPlacement,
                                                  "100");
                    setBackendCapability(backendCapability, VOLATILE.displayName(),
                                         Arrays.asList(DISK.displayName(),
                                                       TAPE.displayName(),
                                                       DISK_TAPE.displayName()),
                                         qoSMetadata);
                    break;
                default:
                    throw new NotFoundException();
            }
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (UnsupportedOperationException e) {
            throw new BadRequestException(e);
        }

        backendCapabilityResponse.setBackendCapability(backendCapability);

        return backendCapabilityResponse;
    }


    @GET
    @ApiOperation("Provides information about a specific directory quality of "
            + "services.  Requires authentication.")
    @ApiResponses({
                @ApiResponse(code = 401, message = "Unauthorized."),
                @ApiResponse(code = 403, message = "Forbidden."),
                @ApiResponse(code = 404, message = "Not found."),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Path("/directory/{qos}")
    @Produces(MediaType.APPLICATION_JSON)
    public BackendCapabilityResponse getQueriedQosForDirectories(
            @ApiParam("The directory quality of service to query.")
            @PathParam("qos") String qosValue) throws CacheException {

        BackendCapabilityResponse backendCapabilityResponse
                        = new BackendCapabilityResponse();

        BackendCapability backendCapability = new BackendCapability();

        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            backendCapabilityResponse.setStatus("200");
            backendCapabilityResponse.setMessage("successful");

            QoSMetadata qoSMetadata;

            switch (Qos.fromDisplayName(qosValue))
            {
                case DISK:
                    qoSMetadata = new QoSMetadata("1",
                                                  geographicPlacement,
                                                  "100");
                    setBackendCapability(backendCapability, DISK.displayName(),
                                         Arrays.asList(TAPE.displayName()),
                                         qoSMetadata);
                    break;
                case TAPE:
                    qoSMetadata = new QoSMetadata("1",
                                                  geographicPlacement,
                                                  "600000");
                    setBackendCapability(backendCapability, TAPE.displayName(),
                                         Arrays.asList(DISK.displayName()),
                                         qoSMetadata);
                    break;
                case DISK_TAPE:
                    qoSMetadata = new QoSMetadata("2",
                                                  geographicPlacement,
                                                  "100");
                    setBackendCapability(backendCapability, DISK_TAPE.displayName(),
                                         Collections.emptyList(),
                                         qoSMetadata);
                    break;
                case VOLATILE:
                    qoSMetadata = new QoSMetadata("0",
                                                  geographicPlacement,
                                                  "100");
                    setBackendCapability(backendCapability, VOLATILE.displayName(),
                                         Collections.emptyList(),
                                         qoSMetadata);
                    break;
                default:
                    throw new NotFoundException();
            }
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (UnsupportedOperationException e) {
            throw new BadRequestException(e);
        }

        backendCapabilityResponse.setBackendCapability(backendCapability);
        return backendCapabilityResponse;
    }


    public void setBackendCapability(BackendCapability backendCapability,
                                     String name,
                                     List<String> transitions,
                                     QoSMetadata qoSMetadata) {

        backendCapability.setName(name);
        backendCapability.setTransition(transitions);
        backendCapability.setMetadata(qoSMetadata);
    }


}
