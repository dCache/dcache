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
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
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

        private final JSONObject payload = new JSONObject();
        private final JSONObject lifecycle = new JSONObject();
        private final JSONObject context = new JSONObject();
        private final JSONObject flow = new JSONObject();

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
            payload.put("flow-lifecycle", lifecycle);
            payload.put("context", context);
            payload.put("flow-id", flow);

            lifecycle.put("state", state);
            lifecycle.put("current-time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

            String jsonPayload = payload.toString();
            String time = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
            String host = flow.has("src-ip") ? flow.getString("src-ip") : "localhost";
            String syslogHeader = String.format("<134>1 %s %s dCache - firefly-json - ", time, host);

            return syslogHeader + jsonPayload;
        }
    }
}
