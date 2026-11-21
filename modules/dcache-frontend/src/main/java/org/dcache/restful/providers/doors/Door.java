package org.dcache.restful.providers.doors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.net.InetAddresses;
import dmg.cells.services.login.LoginBrokerInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Schema(description = "Description about a specific dCache door.")
public final class Door {

    @Schema(description = "The door cell name. Requires admin role assertion.")
    private String cellName;

    @Schema(description = "The domain name in which the door reside. Requires admin " +
          "role assertion.")
    private String domainName;

    @Schema(description = "The preferred protocol name.")
    private String protocol;

    @Schema(description = "All protocol names supported by this door.")
    private List<String> protocols;

    @Schema(description = "The version number of the protocol.")
    private String version;

    @Schema(description = "The root path of the door.")
    private String root;

    @Schema(description = "The door unique identifier. Requires admin role assertion.")
    private String identifier;

    @Schema(description = "List of addresses on which the door is listening for " +
          "incoming client connections.")
    private List<InetAddress> addresses;

    @Schema(description = "The port number of the door.")
    private int port;

    @Schema(description = "A value between 0 and 1, where 1 indicates maximum load.")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private double load;

    @Schema(description = "Elapsed time (in milliseconds) since the door was started. " +
          "Requires admin role assertion.")
    private long updateTime;

    @Schema(description = "List of tags associated with the door.")
    private Collection<String> tags;

    @Schema(description = "List of read paths.")
    private Collection<String> readPaths;

    @Schema(description = "List of write paths.")
    private Collection<String> writePaths;

    public Door(Boolean isAdmin, LoginBrokerInfo info) {
        cellName = isAdmin ? info.getCellName() : null;
        domainName = isAdmin ? info.getDomainName() : null;
        identifier = isAdmin ? info.getIdentifier() : null;
        updateTime = isAdmin ? info.getUpdateTime() : 0L;

        protocol = info.getPreferredProtocolFamily();
        protocols = info.getProtocolFamilies();
        version = info.getProtocolVersion();
        root = info.getRoot();
        addresses = info.getAddresses().stream()
              .filter(a -> !InetAddresses.isInetAddress(a.getHostName()))
              .collect(Collectors.toList());
        port = info.getPort();
        load = info.getLoad();
        tags = info.getTags();
        readPaths = info.getReadPaths();
        writePaths = info.getWritePaths();
    }

    public String getCellName() {
        return cellName;
    }

    public String getDomainName() {
        return domainName;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getVersion() {
        return version;
    }

    public String getRoot() {
        return root;
    }

    public String getIdentifier() {
        return identifier;
    }

    public List<InetAddress> getAddresses() {
        return addresses;
    }

    public int getPort() {
        return port;
    }

    public double getLoad() {
        return load;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public Collection<String> getTags() {
        return tags;
    }

    public Collection<String> getReadPaths() {
        return readPaths;
    }

    public Collection<String> getWritePaths() {
        return writePaths;
    }
}
