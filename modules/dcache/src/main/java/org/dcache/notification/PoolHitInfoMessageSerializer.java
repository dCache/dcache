package org.dcache.notification;

import static java.nio.charset.StandardCharsets.UTF_8;

import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

public class PoolHitInfoMessageSerializer implements Serializer<PoolHitInfoMessage> {

    @Override
    public byte[] serialize(String topic, PoolHitInfoMessage data) {
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

        o.put("pnfsid", data.getPnfsId());
        o.put("filesize", data.getFileSize());
        o.put("billingPath", data.getBillingPath());
        o.put("protocol", data.getProtocolInfo());
        o.put("cached", data.getFileCached());

        StorageInfo info = data.getStorageInfo();
        if (info != null) {
            o.put("storageInfo", info.getStorageClass() + "@" + info.getHsm());
        }

        return o.toString().getBytes(UTF_8);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
