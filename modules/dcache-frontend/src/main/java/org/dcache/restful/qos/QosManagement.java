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

import org.dcache.restful.util.RequestUser;


/**
 * RestFul API for querying and manipulating QoS
 */
@Component
@Api(value = "qos", authorizations = {@Authorization("basicAuth")})
@Path("/qos-management/qos/")
public class QosManagement {

    public static final String DISK = "disk";
    public static final String TAPE = "tape";
    public static final String DISK_TAPE = "disk+tape";
    public static final String VOLATILE = "volatile";
    public static final String UNAVAILABLE = "unavailable";

    public static List<String> cdmi_geographic_placement_provided = Arrays.asList("DE");


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

            // query the lis of available QoS for file objects
            if ("file".equals(qosValue)) {
                JSONArray list = new JSONArray(Arrays.asList(DISK, TAPE, DISK_TAPE, VOLATILE));
                json.put("name", list);
            }
            // query the lis of available QoS for directory objects
            else if ("directory".equals(qosValue.trim())) {
                JSONArray list = new JSONArray(Arrays.asList(DISK, TAPE, DISK_TAPE, VOLATILE));
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

        BackendCapabilityResponse backendCapabilityResponse = new BackendCapabilityResponse();
        BackendCapability backendCapability = new BackendCapability();

        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            backendCapabilityResponse.setStatus("200");
            backendCapabilityResponse.setMessage("successful");

            // Set data and metadata for "DISK" QoS
            if (DISK.equals(qosValue)) {

                QoSMetadata qoSMetadata = new QoSMetadata("1", cdmi_geographic_placement_provided, "100");
                setBackendCapability(backendCapability, DISK, Arrays.asList(TAPE, DISK_TAPE), qoSMetadata);
            }
            // Set data and metadata for "TAPE" QoS
            else if (TAPE.equals(qosValue)) {

                QoSMetadata qoSMetadata = new QoSMetadata("1", cdmi_geographic_placement_provided, "600000");
                setBackendCapability(backendCapability, TAPE, Arrays.asList(DISK_TAPE), qoSMetadata);

            }
            // Set data and metadata for "Disk & TAPE" QoS
            else if (DISK_TAPE.equals(qosValue)) {

                QoSMetadata qoSMetadata = new QoSMetadata("2", cdmi_geographic_placement_provided, "100");
                setBackendCapability(backendCapability, DISK_TAPE, Arrays.asList(TAPE), qoSMetadata);

            }
            else if (VOLATILE.equals(qosValue)) {
                QoSMetadata qoSMetadata = new QoSMetadata("0", cdmi_geographic_placement_provided, "100");
                setBackendCapability(backendCapability, VOLATILE, Arrays.asList(DISK), qoSMetadata);
            }
            // The QoS is not known or supported.
            else {
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

        BackendCapabilityResponse backendCapabilityResponse = new BackendCapabilityResponse();

        BackendCapability backendCapability = new BackendCapability();

        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            backendCapabilityResponse.setStatus("200");
            backendCapabilityResponse.setMessage("successful");

            // Set data and metadata for "DISK" QoS
            if (DISK.equals(qosValue)) {

                QoSMetadata qoSMetadata = new QoSMetadata("1", cdmi_geographic_placement_provided, "100");
                setBackendCapability(backendCapability, DISK, Arrays.asList(TAPE), qoSMetadata);
            }
            // Set data and metadata for "TAPE" QoS
            else if (TAPE.equals(qosValue)) {

                QoSMetadata qoSMetadata = new QoSMetadata("1", cdmi_geographic_placement_provided, "600000");
                setBackendCapability(backendCapability, TAPE, Arrays.asList(DISK), qoSMetadata);
            }
            // Set data and metadata for "Disk & TAPE" QoS
            else if (DISK_TAPE.equals(qosValue)) {
                QoSMetadata qoSMetadata = new QoSMetadata("2", cdmi_geographic_placement_provided, "100");
                setBackendCapability(backendCapability, DISK_TAPE, Collections.emptyList(), qoSMetadata);
            }
            else if (VOLATILE.equals(qosValue)) {
                QoSMetadata qoSMetadata = new QoSMetadata("0", cdmi_geographic_placement_provided, "100");
                setBackendCapability(backendCapability, VOLATILE, Collections.emptyList(), qoSMetadata);
            }
            // The QoS is not known or supported.
            else {
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
