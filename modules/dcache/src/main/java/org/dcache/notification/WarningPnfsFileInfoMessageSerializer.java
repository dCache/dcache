package org.dcache.notification;

import static java.nio.charset.StandardCharsets.UTF_8;

import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

public class WarningPnfsFileInfoMessageSerializer implements
      Serializer<diskCacheV111.vehicles.WarningPnfsFileInfoMessage> {

    @Override
    public byte[] serialize(String topic, WarningPnfsFileInfoMessage data) {
        JSONObject o = new JSONObject();

        o.put("version", "1.0");
        o.put("msgType", data.getMessageType());
        o.put("date", DateTimeFormatter.ISO_OFFSET_DATE_TIME
              .format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(data.getTimestamp()),
                    ZoneId.systemDefault())));
        o.put("cellName", data.getCellAddress().getCellName());
        o.put("cellType", data.getCellType());
        o.put("cellDomain", data.getCellAddress().getCellDomainName());

        JSONObject status = new JSONObject();
        status.put("code", data.getResultCode());
        status.put("msg", data.getMessage());
        o.put("status", status);

        return o.toString().getBytes(UTF_8);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
