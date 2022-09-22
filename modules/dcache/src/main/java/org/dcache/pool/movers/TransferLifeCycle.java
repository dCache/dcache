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
import javax.annotation.Nonnull;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.net.FlowMarker.FlowMarkerBuilder;
import org.dcache.util.IPMatcher;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferLifeCycle {

    private final static Logger LOGGER = LoggerFactory.getLogger(TransferLifeCycle.class);

    /**
     * The UDP firefly default port as described in
     * https://docs.google.com/document/d/1x9JsZ7iTj44Ta06IHdkwpv5Q2u4U2QGLWnUeN2Zf5ts
     */
    private final static int UDP_PORT = 10514;

    // by default, send the firefly to the destination host.
    private Function<InetSocketAddress, InetSocketAddress> toFireflyDestination = a -> new InetSocketAddress(
          a.getAddress(), UDP_PORT);

    // tests whatever the provided IP belongs to the sites internal network
    private Predicate<InetAddress> localSubnet = a -> false;

    private boolean enabled;

    public void onStart(InetSocketAddress src, InetSocketAddress dst, ProtocolInfo protocolInfo,
          Subject subject) {

        if (!enabled) {
            return;
        }

        if (isLocalTransfer(dst)) {
            return;
        }

        if (!needMarker(protocolInfo)) {
            return;
        }

        var data = new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withExperimentId(getExperimentId(subject))
              .withActivityId(getActivity(protocolInfo))
              .wittApplication(getApplication(protocolInfo))
              .withProtocol("tcp")
              .withAFI(toAFI(dst))
              .withDestination(dst)
              .withSource(src)
              .build("start");

        send(toFireflyDestination.apply(dst), data);
    }

    public void onEnd(InetSocketAddress src, InetSocketAddress dst, ProtocolInfo protocolInfo,
          Subject subject) {

        if (!enabled) {
            return;
        }

        if (isLocalTransfer(dst)) {
            return;
        }

        if (!needMarker(protocolInfo)) {
            return;
        }

        var data = new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withFinishedAt(Instant.now())
              .withExperimentId(getExperimentId(subject))
              .withActivityId(getActivity(protocolInfo))
              .wittApplication(getApplication(protocolInfo))
              .withProtocol("tcp")
              .withAFI(toAFI(dst))
              .withDestination(dst)
              .withSource(src)
              .build("end");

        send(toFireflyDestination.apply(dst), data);
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
     * Send flow marker.
     *
     * @param dst     Inet address where to flow markers should be sent.
     * @param payload the marker
     * @throws IllegalStateException if flow marker ist not build.
     */
    private void send(InetSocketAddress dst, @Nonnull JSONObject payload)
          throws IllegalStateException {

        byte[] data = payload.toString().getBytes(StandardCharsets.UTF_8);
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

    private int getExperimentId(Subject subject) {

        var vo = Subjects.getPrimaryFqan(subject);
        if (vo == null) {
            return 0;
        }

        switch (vo.getGroup().toLowerCase()) {
            case "atlas":
                return 16;
            case "cms":
                return 23;
            default:
                return 0;
        }
    }

    private boolean isLocalTransfer(InetSocketAddress dst) {
        InetAddress addr = dst.getAddress();
        return localSubnet.test(addr);
    }

    private int getActivity(ProtocolInfo protocolInfo) {
        // REVISIT: the activity should come from protocol info
        return 14; // production
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
