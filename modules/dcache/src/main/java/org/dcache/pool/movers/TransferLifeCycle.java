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

import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;

import diskCacheV111.vehicles.MoverInfoMessage;
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
import java.util.OptionalInt;
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
    private final static Logger SCITAGS_LOGGER = LoggerFactory.getLogger("org.dcache.scitags");
    private static final int MIN_VALID_TRANSFER_TAG = 64;
    private static final int MAX_VALID_TRANSFER_TAG = 65535;
    private static final int EXPERIMENT_ID_BIT_SHIFT = 6;
    private static final int ACTIVITY_ID_MASK = 0x3F;
    private static final int DEFAULT_ACTIVITY_ID = 1;
    private static final Splitter FQAN_GROUP_SPLITTER = Splitter.on('/')
          .trimResults()
          .omitEmptyStrings()
          .limit(2);

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

    // optional additional collector destination for firefly markers
    private InetSocketAddress fireflyCollector;
    private boolean enabled;
    private boolean storageStatisticsEnabled;

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
                        logSkippedMarker("disabled", src, dst, protocolInfo);
            return;
        }

        if (isExcludedTransfer(src, dst)) {
            logSkippedMarker("excluded", src, dst, protocolInfo);
            return;
        }

        if (!needMarker(protocolInfo)) {
            logSkippedMarker("unsupported-protocol", src, dst, protocolInfo);
            return;
        }

          var optionalExpId = getExperimentId(protocolInfo, subject);
          if (optionalExpId.isEmpty()) {
            logSkippedMarker("no-experiment-id", src, dst, protocolInfo);
            return;
          }

          int activityId = getActivity(protocolInfo);

          var data = new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withExperimentId(optionalExpId.getAsInt())
              .withActivityId(activityId)
              .wittApplication(getApplication(protocolInfo))
              .withProtocol("tcp")
              .withAFI(toAFI(dst))
              .withDestination(dst)
              .withSource(src)
              .build("start");

          InetSocketAddress fireflyDestination = toFireflyDestination.apply(src);
          sendToMultipleDestinations(fireflyDestination, data);
          logMarkerEvent("start", src, dst, protocolInfo, optionalExpId.getAsInt(), activityId,
              null, null, fireflyDestination);
    }

    /**
     * Mark transfer end.
     * @param src remote client endpoint
     * @param dst local pool endpoint
     * @param protocolInfo access protocol information
     * @param subject associated with the transfer
     */
    public void onEnd(InetSocketAddress src, InetSocketAddress dst, MoverInfoMessage mover) {
        ProtocolInfo protocolInfo = mover.getProtocolInfo();
        Subject subject = mover.getSubject();


        if (!enabled) {
            logSkippedMarker("disabled", src, dst, protocolInfo);
            return;
        }

        if (isExcludedTransfer(src, dst)) {
            logSkippedMarker("excluded", src, dst, protocolInfo);
            return;
        }

        if (!needMarker(protocolInfo)) {
            logSkippedMarker("unsupported-protocol", src, dst, protocolInfo);
            return;
        }

        var optionalExpId = getExperimentId(protocolInfo, subject);
        if (optionalExpId.isEmpty()) {
            logSkippedMarker("no-experiment-id", src, dst, protocolInfo);
            return;
        }

        int activityId = getActivity(protocolInfo);
        long transferDurationMillis = Math.max(0L, mover.getConnectionTime());
        Instant startedAt = Instant.ofEpochMilli(mover.getTimestamp());
        Instant finishedAt = startedAt.plusMillis(transferDurationMillis);

        var data = new FlowMarkerBuilder()
              .withStartedAt(startedAt)
              .withFinishedAt(finishedAt)
              .withExperimentId(optionalExpId.getAsInt())
              .withActivityId(activityId)
              .wittApplication(getApplication(protocolInfo))
              .withUsage(mover.getBytesRead(), mover.getBytesWritten())
              .withProtocol("tcp")
              .withAFI(toAFI(dst))
              .withDestination(dst)
              .withSource(src);
        if (storageStatisticsEnabled) {
            data.withStats(mover);
        }
        var firefly = data.build("end");

        InetSocketAddress fireflyDestination = toFireflyDestination.apply(src);
        sendToMultipleDestinations(fireflyDestination, firefly);
        logMarkerEvent("end", src, dst, protocolInfo, optionalExpId.getAsInt(), activityId,
              mover.getBytesRead(), mover.getBytesWritten(), fireflyDestination);
    }

    /**
     * Configures optional additional firefly collector destination.
     *
     * If not configured, fireflies are sent only to the data-flow destination.
     * If configured, fireflies are sent to both the data-flow destination and this collector.
     *
     * Accepted formats:
     * - host
     * - host:port
     * - [ipv6-address]
     * - [ipv6-address]:port
     *
     * IPv6 literals with a port must use the bracketed form (for example,
     * {@code [2001:db8::1]:10514}), as required by {@link HostAndPort#fromString(String)}.
     *
     * If no port is provided, the default firefly UDP port (10514) is used.
     */
    public void setFireflyDestination(String addr) {
        if (addr != null && !addr.isBlank()) {
            var destination = HostAndPort.fromString(addr.trim());
            fireflyCollector = new InetSocketAddress(destination.getHost(),
                  destination.getPortOrDefault(UDP_PORT));
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

    public void setStorageStatisticsEnabled(boolean isEnabled) {
        storageStatisticsEnabled = isEnabled;
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
     * Send flow marker using an existing datagram socket.
     *
     * @param socket  Datagram socket to use for sending.
     * @param dst     Inet address where the flow marker should be sent.
     * @param payload the marker
     */
    private void send(DatagramSocket socket, InetSocketAddress dst, @Nonnull String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        DatagramPacket p = new DatagramPacket(data, data.length, dst);
        try {
            socket.send(p);
        } catch (IOException e) {
            LOGGER.warn("Failed to send flow marker to {}: {}", dst, e.getMessage());
        }
    }

    /**
     * Send flow marker to the primary destination and optional configured collector.
     *
     * @param primaryDst Primary destination (based on flow/peer)
     * @param payload    the marker
     */
    private void sendToMultipleDestinations(InetSocketAddress primaryDst, @Nonnull String payload) {
        try (DatagramSocket socket = new DatagramSocket()) {
            send(socket, primaryDst, payload);
            if (fireflyCollector != null && !fireflyCollector.equals(primaryDst)) {
                send(socket, fireflyCollector, payload);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to send flow marker: {}", e.getMessage());
        }
    }

    private boolean needMarker(ProtocolInfo protocolInfo) {

        switch (protocolInfo.getProtocol().toLowerCase()) {
            case "xrootd":
            case "http":
            case "https":
            case "remotehttpdatatransfer":
            case "remotehttpsdatatransfer":
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
    private OptionalInt getExperimentId(ProtocolInfo protocolInfo, Subject subject) {
        if (hasTransferTag(protocolInfo)) {
            try {
                int transferTag = Integer.parseInt(protocolInfo.getTransferTag());
                if (transferTag < MIN_VALID_TRANSFER_TAG || transferTag > MAX_VALID_TRANSFER_TAG) {
                    LOGGER.warn("Invalid integer range for transfer tag: {}", protocolInfo.getTransferTag());
                    return OptionalInt.empty();
                }
                // scitag = exp_id << 6 | act_id
                return OptionalInt.of(transferTag >> EXPERIMENT_ID_BIT_SHIFT);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid transfer tag: {}", protocolInfo.getTransferTag());
                return OptionalInt.empty();
            }
        }

        var vo = Subjects.getPrimaryFqan(subject);
        if (vo == null) {
            return OptionalInt.empty();
        }

        String groupPath = vo.getGroup();
        if (groupPath == null || groupPath.isBlank()) {
            return OptionalInt.empty();
        }

        groupPath = groupPath.toLowerCase();
        String voName = FQAN_GROUP_SPLITTER.splitToList(groupPath).get(0);

        return voToExpId.containsKey(voName)
                ? OptionalInt.of(voToExpId.get(voName))
                : OptionalInt.empty();
    }

    private boolean isExcludedTransfer(InetSocketAddress src, InetSocketAddress dst) {
        InetAddress srcAddress = src.getAddress();
        InetAddress dstAddress = dst.getAddress();
        return srcAddress != null && dstAddress != null
              && localSubnet.test(srcAddress)
              && localSubnet.test(dstAddress);
    }

    private int getActivity(ProtocolInfo protocolInfo) {
        if (hasTransferTag(protocolInfo)) {
            // scitag = exp_id << 6 | act_id
            return Integer.parseInt(protocolInfo.getTransferTag()) & ACTIVITY_ID_MASK;
        } else {
            return DEFAULT_ACTIVITY_ID;
        }
    }

    private boolean hasTransferTag(ProtocolInfo protocolInfo) {
        String transferTag = protocolInfo.getTransferTag();
        return transferTag != null && !transferTag.isEmpty();
    }

    private void logSkippedMarker(String reason, InetSocketAddress src, InetSocketAddress dst,
          ProtocolInfo protocolInfo) {
        if (SCITAGS_LOGGER.isDebugEnabled()) {
            SCITAGS_LOGGER.debug(
                  "scitags event=marker-skip reason={} protocol={} source={} sourcePort={} destination={} destinationPort={} transferTag={}",
                  reason,
                  protocolInfo.getProtocol().toLowerCase(),
                  formatAddress(src),
                  formatPort(src),
                  formatAddress(dst),
                  formatPort(dst),
                  formatTransferTag(protocolInfo));
        }
    }

    private void logMarkerEvent(String event, InetSocketAddress src, InetSocketAddress dst,
            ProtocolInfo protocolInfo, int experimentId, int activityId, Double bytesRead,
            Double bytesWritten, InetSocketAddress fireflyDestination) {
        if (SCITAGS_LOGGER.isDebugEnabled()) {
            String classification = hasTransferTag(protocolInfo) ? "transfer-tag" : "fqan";
            if (bytesRead == null || bytesWritten == null) {
                SCITAGS_LOGGER.debug(
                      "scitags event=marker state={} protocol={} source={} sourcePort={} destination={} destinationPort={} transferTag={} experimentId={} activityId={} classifier={} fireflyDestination={} collector={}",
                      event,
                      protocolInfo.getProtocol().toLowerCase(),
                      formatAddress(src),
                      formatPort(src),
                      formatAddress(dst),
                      formatPort(dst),
                      formatTransferTag(protocolInfo),
                      experimentId,
                      activityId,
                      classification,
                      formatSocketAddress(fireflyDestination),
                      formatCollectorDestination());
            } else {
                SCITAGS_LOGGER.debug(
                      "scitags event=marker state={} protocol={} source={} sourcePort={} destination={} destinationPort={} transferTag={} experimentId={} activityId={} classifier={} bytesRead={} bytesWritten={} fireflyDestination={} collector={}",
                      event,
                      protocolInfo.getProtocol().toLowerCase(),
                      formatAddress(src),
                      formatPort(src),
                      formatAddress(dst),
                      formatPort(dst),
                      formatTransferTag(protocolInfo),
                      experimentId,
                      activityId,
                      classification,
                      bytesRead,
                      bytesWritten,
                      formatSocketAddress(fireflyDestination),
                      formatCollectorDestination());
            }
        }
    }

    private String formatSocketAddress(InetSocketAddress address) {
        return formatAddress(address) + ":" + formatPort(address);
    }

    private String formatCollectorDestination() {
        return fireflyCollector == null
              ? "-"
              : formatAddress(fireflyCollector) + ":" + fireflyCollector.getPort();
    }

    private String formatTransferTag(ProtocolInfo protocolInfo) {
        String transferTag = protocolInfo.getTransferTag();
        return transferTag == null || transferTag.isEmpty() ? "-" : transferTag;
    }

    private String formatAddress(InetSocketAddress address) {
        if (address == null) {
            return "-";
        }

        InetAddress inetAddress = address.getAddress();
        return inetAddress == null ? address.getHostString() : inetAddress.getHostAddress();
    }

    private String formatPort(InetSocketAddress address) {
        return address == null ? "-" : Integer.toString(address.getPort());
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
