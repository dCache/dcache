/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.restful.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.ResponseHeader;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.dcache.restful.events.Channel;
import org.dcache.restful.events.SubscriptionResult;
import org.dcache.restful.events.EventStreamRepository;
import org.dcache.restful.events.EventStreamRepository.EventStreamMetadata;
import org.dcache.restful.events.Registrar;
import org.dcache.restful.util.RequestUser;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.restful.util.Preconditions.checkNotForbidden;
import static org.dcache.restful.util.Preconditions.checkRequestNotBad;

/**
 * Class to provide a REST interface for events.  This uses Server Sent Events
 * (SSE) to deliver event notification to clients and a bespoke management
 * interface.
 */
@Component
@Path("events")
@Api(value = "events", authorizations = {@Authorization("basicAuth")})
@Produces(MediaType.APPLICATION_JSON)
public class EventResources
{
    @ApiModel(description = "Metadata supplied when requesting a new channel")
    public static class NewChannelMetadata
    {
        @ApiModelProperty("An identifier the describes the client.  May be "
                + "used to rediscover the channel.  An empty string is "
                + "equivalent to not supplying a channel-id")
        @JsonProperty("client-id")
        private String clientId;
    }

    @ApiModel(description = "Information on closing channels if disconnected.")
    public class DisconnectLifetimeMetadata
    {
        @ApiModelProperty(value = "The minimum duration a client is disconnected "
                + "when a channel is garbage collected.  The value is in seconds.")
        public int getMinimum()
        {
            return MINIMUM_DISCONNECT_TIMEOUT;
        }

        @ApiModelProperty("The maximum duration a client is disconnected when a "
                + "channel is garbage collected.  The value is in seconds.")
        public long getMaximum()
        {
            return maximumDisconnectTimeout;
        }

        @ApiModelProperty("The default duration a client is disconnected when a "
                + "channel is garbage collected.  The value is in seconds.")
        public long getDefault()
        {
            return TimeUnit.MILLISECONDS.toSeconds(registrar.getDefaultDisconnectTimeout());
        }
    }

    @ApiModel(description = "Information about channels.")
    public class ChannelsMetadata
    {
        @ApiModelProperty("The maximum number of concurrent channel "
                + "allowed for any user.  Attempts to exceed this limit result "
                + "in the request failing with a `429` status code.")
        public int getMaximumPerUser()
        {
            return registrar.getMaximumChannelsPerUser();
        }

        @ApiModelProperty("The policy about automatic closing channels that are "
                + "not connected.  All values are in seconds.")
        public final DisconnectLifetimeMetadata lifetimeWhenDisconnected
                = new DisconnectLifetimeMetadata();
    }

    @ApiModel(description = "Information about the event support.")
    public class ServiceMetadata
    {
        @ApiModelProperty("Information about channels.")
        public final ChannelsMetadata channels = new ChannelsMetadata();
    }

    @ApiModel(description = "Subscription independent metadata about a channel")
    public class ChannelMetadata
    {
        @ApiModelProperty("The current disconnect timeout, in seconds.")
        public final long timeout;

        public ChannelMetadata(Channel channel)
        {
            this.timeout = TimeUnit.MILLISECONDS.toSeconds(channel.getTimeout());
        }
    }

    @ApiModel(description = "")
    public static class ChannelModification
    {
        @ApiModelProperty("The new timeout, in seconds.")
        public long timeout;
    }

    private static final int MINIMUM_DISCONNECT_TIMEOUT = 1;
    private static final int MAX_CLIENT_ID_LENGTH = 256;

    private final ServiceMetadata serviceMetadata = new ServiceMetadata();

    private long maximumDisconnectTimeout; // value is in seconds

    @Inject
    private EventStreamRepository repository;

    @Inject
    private Registrar registrar;

    @Required
    public void setMaximumDisconnectTimeout(long timeout)
    {
        checkArgument(timeout > 0, "timeout must be greater than 0");
        maximumDisconnectTimeout = timeout;
    }

    private static String canonicaliseClientId(String in)
    {
        if (in == null || in.isEmpty()) {
            return null;
        }
        return in.substring(0, Math.min(in.length(), MAX_CLIENT_ID_LENGTH));
    }

    private Channel channelForId(String id)
    {
        RequestUser.checkAuthenticated();
        Channel channel = registrar.get(id)
                .orElseThrow(() -> new NotFoundException("No such channel"));
        checkNotForbidden(channel.isAccessAllowed(RequestUser.getSubject()),
                "Access denied");
        return channel;
    }

    @GET
    @ApiOperation(value = "Obtain general information about event support in "
            + "dCache.",
            notes = "This query returns information that applies independent of "
                    + "a specific event-type, and independent of a specific "
                    + "channel.")
    public ServiceMetadata serviceMetadata()
    {
        return serviceMetadata;
    }


    @GET
    @ApiOperation(value = "Obtain a list of the available event types.",
            notes = "Event types are course-grain identifiers that group "
                    + "together broadly similar events.  These identifiers are "
                    + "used for introspection (finding out metadata about these "
                    + "events), adding or modifying a channel's "
                    + "subscription.  The event-type identifiers are also used "
                    + "as the 'event' fields in SSE messages.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
            })
    @Path("/eventTypes")
    public List<String> getEventTypes()
    {
        RequestUser.checkAuthenticated();
        return repository.listEventTypes();
    }


    @GET
    @ApiOperation(value = "Obtain non-schema information about a specific event type.",
            notes = "The information returns general information about a specific "
                    + "event type.  The JSON Schema that describes selectors and "
                    + "events is provided in seperate queries.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 404, message = "No such event type"),
            })
    @Path("/eventTypes/{type}")
    public EventStreamMetadata getEventType(@ApiParam("The specific event type to be described.")
                           @NotNull @PathParam("type") String type)
    {
        RequestUser.checkAuthenticated();
        return repository.metadataForEventType(type)
                .orElseThrow(() -> new NotFoundException("No such event type"));
    }


    @GET
    @ApiOperation(value = "Obtain the JSON schema for this event type's selectors.",
            notes = "The returned information is a JSON Schema that describes "
                    + "the format and semantics of the selector.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 404, message = "No such event type"),
            })
    @Path("/eventTypes/{type}/selector")
    public ObjectNode getSelectorSchema(@ApiParam("The specific event type to be described.")
                           @NotNull @PathParam("type") String type)
    {
        RequestUser.checkAuthenticated();
        return repository.selectorSchemaForEventType(type)
                .orElseThrow(() -> new NotFoundException("No such event type"));
    }

    @GET
    @ApiOperation(value = "Obtain the JSON schema for events of this event type.",
            notes = "The returned information is a JSON Schema that describes "
                    + "the format and semantics of events.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 404, message = "No such event type"),
            })
    @Path("/eventTypes/{type}/event")
    public ObjectNode getEventSchema(@ApiParam("The specific event type to be described.")
                           @NotNull @PathParam("type") String type)
    {
        RequestUser.checkAuthenticated();
        return repository.eventSchemaForEventType(type)
                .orElseThrow(() -> new NotFoundException("No such event type"));
    }

    @GET
    @ApiOperation(value = "Obtain a list of channels.",
            notes = "Provide a list of channel URLs that are the currently active "
                    + "for this user.  Channels that have been close (either "
                    + "manually by the client or automatically through being "
                    + "disconnected for too long) are not shown."
                    + "\n"
                    + "If the client-id query parameter is supplied then the "
                    + "list contains only those channels created with that "
                    + "client-id.  If the channel-id query parameter value is "
                    + "an empty string then list contains only those channels "
                    + "created without a channel-id.  If no client-id "
                    + "query-parameter is supplied then all channels are "
                    + "returned.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
            })
    @Path("/channels")
    public List<String> getChannels(@Context UriInfo uriInfo,
            @ApiParam("Limit channels by client-id")
            @QueryParam("client-id") String clientId)
    {
        RequestUser.checkAuthenticated();

        UriBuilder idToUri = uriInfo.getBaseUriBuilder().path(getClass()).path(getClass(), "events");

        List<String> ids = clientId == null
                ? registrar.idsForUser(RequestUser.getSubject())
                : registrar.idsForUser(RequestUser.getSubject(), canonicaliseClientId(clientId));
        return ids.stream()
                .map(idToUri::build)
                .map(URI::toASCIIString)
                .collect(Collectors.toList());
    }


    @POST
    @ApiOperation(value = "Request a new channel.",
            notes = "A channel is a URL that allows a client to receive "
                    + "events. Each channel is owned by a dCache user and "
                    + "may only be used by that user. Each user is allowed only "
                    + "a finite number of channels."
                    + "\n"
                    + "A client needs only one channel, independent of what "
                    + "events are of interest.  The delivery of events within "
                    + "a channel is controlled though subscriptions. A "
                    + "channel's subscriptions may be modified if the "
                    + "desired events changes over time. A channel is "
                    + "created without any subscriptions, therefore it must "
                    + "be modified by adding subscriptions before the client "
                    + "will receive any events."
                    + "\n"
                    + "Channels must not be shared between different "
                    + "clients.  Any channel left for too long with no client"
                    + "receiving events will be subject to garbage-collection."
                    + "\n"
                    + "When requesting a new channel, the client may supply a "
                    + "client-id value.  The client-id may be supplied when "
                    + "obtaining the list of channels, limiting the result to "
                    + "those channels with the supplied channel-id.  This "
                    + "allows a client to reuse an existing channel without "
                    + "storing that channel's URL.")
    @ApiResponses({
                @ApiResponse(code = 201, message = "Created", responseHeaders={
                    @ResponseHeader(name = "Location", response = URI.class,
                            description = "The URL of the new channel.")
                }),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 429, message = "Too Many Channels"),
            })
    @Path("/channels")
    public Response register(@Context UriInfo uriInfo,
            NewChannelMetadata metadata)
    {
        RequestUser.checkAuthenticated();

        String clientId = metadata == null ? null : canonicaliseClientId(metadata.clientId);

        String id = registrar.newChannel(RequestUser.getSubject(), clientId,
                (c,t,s) -> locationOfSubscription(uriInfo, c,t,s).toASCIIString());
        URI location = uriInfo.getBaseUriBuilder().path(getClass()).path(getClass(), "events").build(id);
        return Response.created(location).build();
    }


    @GET
    @ApiOperation(value = "Receive events for this channel.",
            notes = "This method allows a client to receive events in "
                    + "real-time, following the Server Sent Events (SSE) "
                    + "standard. Any standard-compliant SSE client should be "
                    + "able to use this endpoint to receive events."
                    + "\n"
                    + "The SSE standard includes optional support for reliable "
                    + "event delivery, by allowing events to have a unique id and "
                    + "by allowing clients to provide the id of the last successfully "
                    + "process message when reconnecting, via the `Last-Event-ID` "
                    + "request header.  The server can then send any events "
                    + "that the client missed while disconnected."
                    + "\n"
                    + "dCache provides limited support for this reliable delivery "
                    + "by keeping a cache of the last 16384 events.  If the client "
                    + "reconnects while the last event is in this cache then "
                    + "dCache will deliver any events the client missed while "
                    + "disconnected; if not, a special event-lost event is sent "
                    + "instead."
                    + "\n"
                    + "Multiple concurrent calls to this method are not allowed. "
                    + "If there is a request consuming events and a second "
                    + "request is made then the first request is terminated.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 403, message = "Access denied"),
                @ApiResponse(code = 404, message = "No such channel"),
            })
    @Path("/channels/{id}")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void events(@ApiParam("The ID of the channel.")
                       @NotNull @PathParam("id") String id,
            @ApiParam("The ID of the last event to be processed.   If supplied "
                    + "and this event is in the event cache then any subsequent "
                    + "events are sent to the client.")
            @HeaderParam("Last-Event-ID") String lastId,
            @Context SseEventSink sink,
            @Context Sse sse)
    {
        channelForId(id).acceptConnection(sse, sink, lastId);
    }


    @GET
    @ApiOperation("Obtain metadata about a channel.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 403, message = "Access denied"),
                @ApiResponse(code = 404, message = "No such channel"),
            })
    @Path("/channels/{id}")
    public ChannelMetadata channelMetadata(@NotNull @PathParam("id") String id)
    {
        return new ChannelMetadata(channelForId(id));
    }


    @PATCH
    @ApiOperation(value = "Modify a channel.",
            notes = "This operation allows changes to a channel that are not "
                    + "related to subscriptions.  Currently, this is modifying "
                    + "the disconnection timeout.")
    @ApiResponses({
                @ApiResponse(code = 204, message = "No Content"),
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 401, message = "Unauthorized"),
                @ApiResponse(code = 403, message = "Access denied"),
                @ApiResponse(code = 404, message = "No found"),
            })
    @Path("/channels/{id}")
    public void modify(@NotNull @PathParam("id") String id,
            ChannelModification modification)
    {
        Channel channel = channelForId(id);

        checkRequestNotBad(modification.timeout >= MINIMUM_DISCONNECT_TIMEOUT,
                "Timeout shorter than minimum allowed");
        checkRequestNotBad(modification.timeout <= maximumDisconnectTimeout,
                "Timeout longer than maximum allowed");
        channel.updateTimeout(TimeUnit.SECONDS.toMillis(modification.timeout));
    }


    @DELETE
    @ApiOperation(value = "Cancel a channel.",
            notes="This operation cancels a channel.  All subscriptions are "
                    + "automatically cancelled.  Any connection that is "
                    + "receiving events is closed."
                    + "\n"
                    + "Once cancelled, any subsequent operations involving the "
                    + "channel will fail.")
    @ApiResponses({
                @ApiResponse(code = 204, message = "No Content"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 403, message = "Access denied"),
                @ApiResponse(code = 404, message = "No such channel"),
            })
    @Path("/channels/{id}")
    public void deleteChannel(@NotNull @PathParam("id") String id) throws IOException
    {
        channelForId(id).close();
    }


    @GET
    @ApiOperation("Obtain list a channel's subscriptions.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 403, message = "Access denied"),
                @ApiResponse(code = 404, message = "No such channel"),
            })
    @Path("/channels/{id}/subscriptions")
    public List<URI> channelSubscriptions(
            @Context UriInfo uriInfo,
            @NotNull @PathParam("id") String channalId)
    {
        return channelForId(channalId).getSubscriptions().stream()
                .map(s -> locationOfSubscription(uriInfo, channalId, s.getEventType(), s.getId()))
                .collect(Collectors.toList());
    }


    @POST
    @ApiOperation(value = "Subscribe to events.",
            notes = "Create a new subscription to some events.  The selector "
                    + "allows the client to describe which events are of "
                    + "interest, in effect, filtering which events are received.  "
                    + "The format of valid selectors is described in the event "
                    + "type metadata, using JSON Schema.")
    @ApiResponses({
                @ApiResponse(code = 201, message = "Created", responseHeaders={
                    @ResponseHeader(name = "Location", response = URI.class,
                            description = "The absolute URL of the new subscription.")
                }),
                @ApiResponse(code = 401, message = "Unauthorized"),
                @ApiResponse(code = 403, message = "Access denied"),
            })
    @Path("/channels/{id}/subscriptions/{type}")
    public Response subscribe(
            @Context UriInfo uriInfo,
            @Context HttpServletRequest request,
            @NotNull @PathParam("id") String channelId,
            @NotNull @PathParam("type") String eventType,
            JsonNode selector)
    {
        checkRequestNotBad(selector != null, "Missing JSON entity in POST request");
        Channel channel = channelForId(channelId);
        SubscriptionResult result = channel.subscribe(request, channelId, eventType, selector);

        switch (result.getStatus()) {
        case CREATED:
            URI newLocation = locationOfSubscription(uriInfo, channelId, eventType, result.getId());
            return Response.created(newLocation).build();

        case MERGED:
            URI existingLocation = locationOfSubscription(uriInfo, channelId, eventType, result.getId());
            return Response.seeOther(existingLocation).build();

        case RESOURCE_NOT_FOUND:
            // REVIST should this have 404 in response JSON ?
            throw new BadRequestException("Not found: " + result.getMessage());

        case PERMISSION_DENIED:
            // REVISIT should this have 403 in response JSON ?
            throw new BadRequestException("Permission denied: " + result.getMessage());

        case BAD_SELECTOR:
            throw new BadRequestException("Bad selector: " + result.getMessage());

        case CONDITION_FAILED:
            throw new BadRequestException("Failed condition: " + result.getMessage());

        default:
            throw new InternalServerErrorException("Unexpected status: " + result.getStatus());
        }
    }

    private URI locationOfSubscription(UriInfo info, String channelId, String eventType,
            String subscriptionId)
    {
        return info.getBaseUriBuilder()
                .path(getClass())
                .path(getClass(), "channelSubscription")
                .build(channelId, eventType, subscriptionId);
    }


    @GET
    @ApiOperation("Return the selector of this subscription.")
    @ApiResponses({
                @ApiResponse(code = 200, message = "OK"),
                @ApiResponse(code = 401, message = "anonymous access not allowed"),
                @ApiResponse(code = 403, message = "Access denied"),
                @ApiResponse(code = 404, message = "No such channel"),
            })
    @Path("/channels/{channel_id}/subscriptions/{type}/{subscription_id}")
    public JsonNode channelSubscription(
            @NotNull @PathParam("channel_id") String channelId,
            @NotNull @PathParam("type") String eventType,
            @NotNull @PathParam("subscription_id") String subscriptionId)
    {
        return channelForId(channelId)
                .getSubscription(eventType, subscriptionId)
                .orElseThrow(() -> new NotFoundException("No such subscription"))
                .getSelector();
    }


    @DELETE
    @ApiOperation(value = "Cancel a subscription.",
            notes="This operation cancels a subscription.  After returning, no "
                    + "further events are sent from this subscription.  The "
                    + "subscription is no longer listed when querying the list "
                    + "of all subscriptions, and attempts to fetch metadata "
                    + "about this subscription return a 404 response.")
    @ApiResponses({
                @ApiResponse(code = 204, message = "No Content"),
                @ApiResponse(code = 401, message = "Unauthorized"),
                @ApiResponse(code = 403, message = "Access denied"),
                @ApiResponse(code = 404, message = "Not Found"),
            })
    @Path("/channels/{channel_id}/subscriptions/{type}/{subscription_id}")
    public void deleteSubscription(@NotNull @PathParam("channel_id") String channelId,
            @NotNull @PathParam("type") String eventType,
            @NotNull @PathParam("subscription_id") String subscriptionId) throws IOException
    {
        channelForId(channelId)
                .getSubscription(eventType, subscriptionId)
                .orElseThrow(() -> new NotFoundException("Subscription not found"))
                .close();
    }
}
