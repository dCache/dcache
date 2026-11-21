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
package org.dcache.net;

import com.google.common.net.InetAddresses;

import diskCacheV111.vehicles.MoverInfoMessage;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.dcache.util.Version;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * See: https://docs.google.com/document/d/1x9JsZ7iTj44Ta06IHdkwpv5Q2u4U2QGLWnUeN2Zf5ts/edit#
 */
public class FlowMarker {

    private final static Logger LOGGER = LoggerFactory.getLogger(FlowMarker.class);

    public static class FlowMarkerBuilder {

        private final static int VERSION = 1;

        /**
         * The dCache release that sends the firefly, so that a consumer can tell which
         * conventions the firefly follows.
         */
        private final static String DCACHE_VERSION = Version.of(FlowMarker.class).getVersion();

        private final JSONObject payload = new JSONObject();
        private final JSONObject lifecycle = new JSONObject();
        private final JSONObject context = new JSONObject();
        private final JSONObject flow = new JSONObject();
        private final JSONObject usage = new JSONObject();
        private final JSONObject stats = new JSONObject();

        /**
         * RFC 5424 NILVALUE, used when the syslog HOSTNAME is unknown.
         */
        private static final String SYSLOG_NILVALUE = "-";

        private String hostname = SYSLOG_NILVALUE;

        /**
         * Set the syslog HOSTNAME of the marker, i.e. the identity of the
         * system emitting the firefly. This should identify the dCache node
         * (preferably by its FQDN, as recommended by RFC 5424), not the
         * remote peer of the flow.
         */
        public FlowMarkerBuilder withHostname(String hostname) {
            this.hostname = hostname == null || hostname.isBlank()
                  ? SYSLOG_NILVALUE
                  : hostname;
            return this;
        }

        public FlowMarkerBuilder withStartedAt(Instant startTime) {
            lifecycle.put("start-time", DateTimeFormatter.ISO_INSTANT.format(startTime));
            return this;
        }

        public FlowMarkerBuilder withFinishedAt(Instant startTime) {
            lifecycle.put("end-time", DateTimeFormatter.ISO_INSTANT.format(startTime));
            return this;
        }

        public FlowMarkerBuilder withExperimentId(int id) {
            context.put("experiment-id", id);
            return this;
        }

        public FlowMarkerBuilder withActivityId(int id) {
            context.put("activity-id", id);
            return this;
        }

        public FlowMarkerBuilder wittApplication(String app) {
            context.put("application", app);
            return this;
        }

        public FlowMarkerBuilder withAFI(String afi) {
            switch (afi) {
                case "ipv6":
                case "ipv4":
                    break;
                default:
                    throw new IllegalArgumentException("AFI can be 'ipv4' or 'ipv6'");
            }

            flow.put("afi", afi);
            return this;
        }

        public FlowMarkerBuilder withSource(InetSocketAddress addr) {
            flow.put("src-ip", InetAddresses.toAddrString(addr.getAddress()));
            flow.put("src-port", addr.getPort());
            return this;
        }

        public FlowMarkerBuilder withDestination(InetSocketAddress addr) {
            flow.put("dst-ip", InetAddresses.toAddrString(addr.getAddress()));
            flow.put("dst-port", addr.getPort());
            return this;
        }

        public FlowMarkerBuilder withProtocol(String proto) {
            switch (proto) {
                case "tcp":
                case "udp":
                    break;
                default:
                    throw new IllegalArgumentException("Protocol can be 'tcp' or 'udp'");
            }

            flow.put("protocol", proto);
            return this;
        }

        /**
         * The bytes that crossed the flow, counted from the side of its source.
         *
         * @param sent     bytes the source sent to the destination: the data of the transfer
         * @param received bytes the source received from the destination
         */
        public FlowMarkerBuilder withUsage(double sent, double received) {
            usage.put("received", Double.isFinite(received) ? (long) received : 0L);
            usage.put("sent", Double.isFinite(sent) ? (long) sent : 0L);
            return this;
        }

        public FlowMarkerBuilder withStats(MoverInfoMessage message) {
            stats.put("bytes-transferred", message.getDataTransferred());
            stats.put("connection-time", message.getConnectionTime());
            stats.put("read-bw", Double.isFinite(message.getMeanReadBandwidth()) ? (long) message.getMeanReadBandwidth() : 0L);
            stats.put("write-bw", Double.isFinite(message.getMeanWriteBandwidth()) ? (long) message.getMeanWriteBandwidth() : 0L);
            stats.put("read-active", message.getReadActive().isPresent() ? message.getReadActive().get().toString() : "0");
            stats.put("read-idle", message.getReadIdle().isPresent() ? message.getReadIdle().get().toString() : "0");  
            stats.put("write-active", message.getWriteActive().isPresent() ? message.getWriteActive().get().toString() : "0");  
            stats.put("write-idle", message.getWriteIdle().isPresent() ? message.getWriteIdle().get().toString() : "0");
            return this;
        }

        public FlowMarkerBuilder withFlowId(String id) {
            flow.put("flow-id", id);
            return this;
        }

        public String build(String state) {

            switch (state) {
                case "start":
                case "end":
                case "ongoing":
                    break;
                default:
                    throw new IllegalArgumentException("State can be 'start', 'ongoing' or 'end'");
            }

            payload.put("version", VERSION);
            payload.put("dcache-version", DCACHE_VERSION);
            payload.put("flow-lifecycle", lifecycle);
            payload.put("context", context);
            payload.put("flow-id", flow);
            payload.put("usage", usage);
            if(!stats.isEmpty())
                payload.put("storage-stats", stats);

            lifecycle.put("state", state);
            lifecycle.put("current-time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

            String jsonPayload = payload.toString();
            String time = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
            String syslogHeader = String.format("<134>1 %s %s dCache - firefly-json - ", time,
                  hostname);

            return syslogHeader + jsonPayload;
        }
    }
}
