/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 - 2024 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.movers;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.InetAddresses.forString;

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import diskCacheV111.vehicles.ProtocolInfo;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.Map;
import java.util.HashMap;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.net.FlowMarker.FlowMarkerBuilder;
import org.dcache.util.IPMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferLifeCycle {

    private final static Logger LOGGER = LoggerFactory.getLogger(TransferLifeCycle.class);

    /**
     * The UDP firefly default port as described in
     * <a href="https://docs.google.com/document/d/1x9JsZ7iTj44Ta06IHdkwpv5Q2u4U2QGLWnUeN2Zf5ts">document</a>.
     */
    private final static int UDP_PORT = 10514;

    // by default, send the firefly to the destination host.
    private Function<InetSocketAddress, InetSocketAddress> toFireflyDestination = a -> new InetSocketAddress(
          a.getAddress(), UDP_PORT);

    // tests whatever the provided IP belongs to the sites internal network
    private Predicate<InetAddress> localSubnet = a -> false;

    private boolean enabled;

    private Map<String, Integer> voToExpId = new HashMap<>();

    /**
     * Mark transfer start.
     * @param src remote client endpoint
     * @param dst local pool endpoint
     * @param protocolInfo access protocol information
     * @param subject associated with the transfer
     */
    public void onStart(InetSocketAddress src, InetSocketAddress dst, ProtocolInfo protocolInfo,
          Subject subject) {

        if (!enabled) {
            return;
        }

        if (isLocalTransfer(src)) {
            return;
        }

        if (!needMarker(protocolInfo)) {
            return;
        }

        int expId = getExperimentId(protocolInfo, subject);
        if (expId == -1) {
            return;
        }

        var data = new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withExperimentId(expId)
              .withActivityId(getActivity(protocolInfo))
              .wittApplication(getApplication(protocolInfo))
              .withProtocol("tcp")
              .withAFI(toAFI(dst))
              .withDestination(dst)
              .withSource(src)
              .build("start");

        send(toFireflyDestination.apply(src), data);
    }

    /**
     * Mark transfer end.
     * @param src remote client endpoint
     * @param dst local pool endpoint
     * @param protocolInfo access protocol information
     * @param subject associated with the transfer
     */
    public void onEnd(InetSocketAddress src, InetSocketAddress dst, ProtocolInfo protocolInfo,
          Subject subject) {

        if (!enabled) {
            return;
        }

        if (isLocalTransfer(src)) {
            return;
        }

        if (!needMarker(protocolInfo)) {
            return;
        }

        int expId = getExperimentId(protocolInfo, subject);
        if (expId == -1) {
            return;
        }

        var data = new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withFinishedAt(Instant.now())
              .withExperimentId(expId)
              .withActivityId(getActivity(protocolInfo))
              .wittApplication(getApplication(protocolInfo))
              .withProtocol("tcp")
              .withAFI(toAFI(dst))
              .withDestination(dst)
              .withSource(src)
              .build("end");

        send(toFireflyDestination.apply(src), data);
    }

    public void setFireflyDestination(String addr) {

        if (!Strings.isNullOrEmpty(addr)) {
            var destination = HostAndPort.fromString(addr);
            var destinationAddr = new InetSocketAddress(destination.getHost(),
                  destination.getPortOrDefault(UDP_PORT));
            toFireflyDestination = a -> destinationAddr;
        }
    }

    public void setExcludes(String[] localSubnets) {
        for (var s : localSubnets) {
            localSubnet = localSubnet.or(new InetAdressPredicate(s));
        }
    }

    public void setEnabled(boolean isEnabled) {
        enabled = isEnabled;
    }

    /**
     * Configures VO (Virtual Organization) to Experiment ID mapping.
     *       
     * @param voMap A comma-separated string of VO mapping entries in the format
     *              "voName:expId".
     */
    public void setVoMapping(String voMap) {
        voToExpId.clear();

        for (String entry : voMap.split(",")) {
            String[] parts = entry.split(":");
            if (parts.length == 2) {
                try {
                    voToExpId.put(parts[0].trim().toLowerCase(), Integer.parseInt(parts[1].trim()));
                } catch (NumberFormatException e) {
                    LOGGER.warn("Invalid VO mapping entry: {}", entry);
                }        
            } else {
                LOGGER.warn("Invalid VO mapping entry: {}", entry);
            }
        }
    }

    /**
     * Send flow marker.
     *
     * @param dst     Inet address where to flow markers should be sent.
     * @param payload the marker
     * @throws IllegalStateException if flow marker ist not build.
     */
    private void send(InetSocketAddress dst, @Nonnull String payload)
          throws IllegalStateException {

        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        DatagramPacket p = new DatagramPacket(data, data.length);
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(dst);
            socket.send(p);
        } catch (IOException e) {
            LOGGER.warn("Failed to send flow marker to {}: {}", dst, e.getMessage());
        }
    }

    private boolean needMarker(ProtocolInfo protocolInfo) {

        switch (protocolInfo.getProtocol().toLowerCase()) {
            case "xrootd":
            case "http":
                return true;
            default:
                return false;
        }
    }

    private String getApplication(ProtocolInfo protocolInfo) {
        // REVISIT: the application should come from protocol info
        return protocolInfo.getProtocol().toLowerCase();
    }

    /**
     * Determine experiment ID, initially from the ProtocolInfo (xroot/http), 
     * if that fails then fallback to the Subject's primary FQAN. 
     *
     * @param protocolInfo the ProtocolInfo object containing transfer-related metadata
     * @param subject the Subject representing the user or entity associated with the transfer
     * @return the experiment ID, or -1 if it cannot be determined
     */
    private int getExperimentId(ProtocolInfo protocolInfo, Subject subject) {
        if (protocolInfo.getTransferTag() != null && !protocolInfo.getTransferTag().isEmpty()) {
            try {
                int transferTag = Integer.parseInt(protocolInfo.getTransferTag());
                if (transferTag <= 64 || transferTag >= 65536) {
                    LOGGER.warn("Invalid integer range for transfer tag: {}", protocolInfo.getTransferTag());
                    return -1;
                }
                // scitag = exp_id << 6 | act_id
                return transferTag >> 6;
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid transfer tag: {}", protocolInfo.getTransferTag());
                return -1;
            }
        }

        var vo = Subjects.getPrimaryFqan(subject);
        if (vo == null) {
            return -1;
        }

        return voToExpId.getOrDefault(vo.getGroup().toLowerCase(), -1);
    }

    private boolean isLocalTransfer(InetSocketAddress dst) {
        InetAddress addr = dst.getAddress();
        return localSubnet.test(addr);
    }

    private int getActivity(ProtocolInfo protocolInfo) {
        if (!protocolInfo.getTransferTag().isEmpty()) {
            // scitag = exp_id << 6 | act_id
            return Integer.parseInt(protocolInfo.getTransferTag()) & 0x3F;
        } else {
            // default activity id = 1
            return 1;
        }
    }

    private String toAFI(InetSocketAddress dst) {

        var addr = dst.getAddress();
        if (addr instanceof Inet6Address) {
            return "ipv6";
        } else if (addr instanceof Inet4Address) {
            return "ipv4";
        } else {
            throw new IllegalArgumentException("Illegal address type: " + addr.getClass());
        }
    }

    private class InetAdressPredicate implements Predicate<InetAddress> {

        private final InetAddress net;
        private final int mask;

        public InetAdressPredicate(String s) {

            String hostAndMask[] = s.split("/");
            checkArgument(hostAndMask.length < 3, "Invalid host specification: " + s);

            net = forString(hostAndMask[0]);
            int fullMask = net.getAddress().length * Byte.SIZE;
            if (hostAndMask.length == 1) {
                mask = fullMask;
            } else {
                mask = Integer.parseInt(hostAndMask[1]);
            }
            checkArgument(mask <= fullMask,
                  "Netmask can't be bigger than full mask " + fullMask);
        }

        @Override
        public boolean test(InetAddress inetAddress) {
            return IPMatcher.match(inetAddress, net, mask);
        }
    }
}
