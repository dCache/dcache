/* dCache - http://www.dcache.org/
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
package org.dcache.notification;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfoMessage;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StorageInfoMessageSerializer implements Serializer<StorageInfoMessage> {

    @Override
    public byte[] serialize(String topic, StorageInfoMessage data) {

        JSONObject o = new JSONObject();
        o.put("version", "1.0");
        o.put("msgType", data.getMessageType());
        o.put("date", DateTimeFormatter.ISO_OFFSET_DATE_TIME
                .format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(data.getTimestamp()), ZoneId.systemDefault())));
        o.put("queuingTime", data.getTimeQueued());
        o.put("transaction", data.getTransaction());
        o.put("cellName", data.getCellAddress().getCellName());
        o.put("cellType", data.getCellType());
        o.put("cellDomain", data.getCellAddress().getCellDomainName());

        JSONObject status = new JSONObject();
        status.put("code", data.getResultCode());
        status.put("msg", data.getMessage());

        o.put("status", status);

        o.put("session", data.getTransaction());

        JSONObject hsm = new JSONObject();
        hsm.put("type", data.getHsmType());
        hsm.put("instance", data.getHsmInstance());
        hsm.put("provider", data.getHsmProvider());
        o.put("hsm", hsm);

        o.put("locations", buildLocations(data));

        o.put("pnfsid", data.getPnfsId());
        o.put("billingPath", data.getBillingPath());
        o.put("fileSize", data.getFileSize());
        o.put("storageInfo", data.getStorageInfo().getStorageClass() + "@" + data.getStorageInfo().getHsm());

        o.put("transferTime", data.getTransferTime());

        return o.toString().getBytes(UTF_8);
    }

    private List<URI> buildLocations(StorageInfoMessage data)
    {
        StorageInfo si = data.getStorageInfo();

        switch (data.getMessageType()) {
        case StorageInfoMessage.STORE_MSG_TYPE:
            return si.isSetAddLocation() ? si.locations() : Collections.emptyList();

        case StorageInfoMessage.RESTORE_MSG_TYPE:
            String hsmType = data.getHsmType();
            String hsmInstance = data.getHsmInstance();

            // REVISIT this is (more-or-less) a copy-n-paste from
            // AbstractBlockingNearlineStorage#getLocations and follows
            // similar assumptions in HsmSet#getInstanceName.
            return si.locations().stream()
                .filter(uri -> Objects.equals(uri.getScheme(), hsmType))
                .filter(uri -> Objects.equals(uri.getAuthority(), hsmInstance))
                .collect(Collectors.toList());

        default:
            throw new IllegalArgumentException("Unexpected message type \""
                    + data.getMessageType() + "\"");
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
