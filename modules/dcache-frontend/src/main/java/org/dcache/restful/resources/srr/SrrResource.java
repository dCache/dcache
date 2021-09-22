package org.dcache.restful.resources.srr;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginBrokerSubscriber;
import io.swagger.annotations.Api;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.srr.SrrBuilder;
import org.dcache.restful.srr.SrrRecord;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RestFul API to  provide files/folders manipulation operations.
 */
@Api(value = "srr")
@Component
@Path("/srr")
public class SrrResource {

    @Context
    private HttpServletRequest request;

    private Map<String, List<String>> pgroup2vo = new HashMap<>();
    // info provider properties
    private String name;
    private String id;
    private String architecture;
    private String quality;
    private String doorTag;

    @Inject
    @Named("spacemanager-stub")
    private CellStub spaceManager;

    @Inject
    @Named("pool-monitor")
    private PoolMonitor remotePoolMonitor;

    @Inject
    @Named("pnfs-stub")
    private CellStub namespaceStub;

    @Inject
    @Named("login-broker-source")
    private LoginBrokerSubscriber loginBrokerSubscriber;

    private boolean spaceReservationEnabled;

    public void setQuality(String quality) {
        this.quality = quality;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDoorTag(String doorTag) {
        this.doorTag = doorTag;
    }

    public void setSpaceReservationEnabled(boolean spaceReservationEnabled) {
        this.spaceReservationEnabled = spaceReservationEnabled;
    }

    public void setArchitecture(String architecture) {
        this.architecture = architecture;
    }

    public void setGroupMapping(String mapping) {

        // pgroup=qos-disk:/cms,qos-disk:/atlas

        Splitter.on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(mapping)
                .forEach(
                        s -> {
                            String[] voMap = s.split(":");
                            if (voMap.length != 2) {
                                throw new IllegalArgumentException("Invalid format of poolgroup -> VO mapping");
                            }
                            pgroup2vo.computeIfAbsent(voMap[0], k -> new ArrayList<>()).add(voMap[1]);
                        }

                );

    }

    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("/")
    public Response getSrr() throws InterruptedException, CacheException, NoRouteToCellException {

        InetAddress remoteAddress  = InetAddresses.forString(request.getRemoteAddr());
        if (!remoteAddress.isLoopbackAddress()) {
            throw new ForbiddenException();
        }

        SrrRecord record = SrrBuilder.builder()
                .withLoginBroker(loginBrokerSubscriber)
                .withNamespace(namespaceStub)
                .withPoolMonitor(remotePoolMonitor)
                .withSpaceManagerStub(spaceManager)
                .withSpaceManagerEnaled(spaceReservationEnabled)
                .withId(id)
                .withName(name)
                .withQuality(quality)
                .withArchitecture(architecture)
                .withGroupVoMapping(pgroup2vo)
                .withDoorTag(doorTag)
                .generate();

        return Response.ok(record)
                .header("Link",
                "<https://raw.githubusercontent.com/sjones-hep-ph-liv-ac-uk/json_info_system/master/srr/v4.2/schema/srrschema_4.2.json>; rel=\"describedby\"")
                .build();
    }
}
