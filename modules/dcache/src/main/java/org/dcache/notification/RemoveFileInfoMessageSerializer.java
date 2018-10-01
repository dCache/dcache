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
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

import diskCacheV111.vehicles.RemoveFileInfoMessage;

import static java.nio.charset.StandardCharsets.UTF_8;


public class RemoveFileInfoMessageSerializer implements Serializer<RemoveFileInfoMessage> {

    @Override
    public byte[] serialize(String topic, RemoveFileInfoMessage data) {

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

        JSONArray subject = new JSONArray();
        data.getSubject().getPrincipals().forEach(s -> subject.put(s));
        o.put("subject", subject);

        o.put("pnfsid", data.getPnfsId());
        o.put("billingPath", data.getBillingPath());
        o.put("fileSize", data.getFileSize());
        o.put("storageInfo", data.getStorageInfo().getStorageClass() + "@" + data.getStorageInfo().getHsm());


        return o.toString().getBytes(UTF_8);

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
