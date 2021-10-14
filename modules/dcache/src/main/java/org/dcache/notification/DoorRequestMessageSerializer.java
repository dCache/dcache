package org.dcache.notification;

import static java.nio.charset.StandardCharsets.UTF_8;

import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONArray;
import org.json.JSONObject;

public class DoorRequestMessageSerializer implements Serializer<DoorRequestInfoMessage> {

    private static final String[] REDUNDANT_MOVER_DATA_KEYS
          = {
          "subject",
          "pnfsid",
          "fileSize",
          "initiator",
          "billingPath",
          "storageInfo"
    };

    @Override
    public byte[] serialize(String topic, DoorRequestInfoMessage data) {

        JSONObject o = new JSONObject();
        o.put("VERSION", "1.0");
        o.put("msgType", data.getMessageType());
        o.put("date", DateTimeFormatter.ISO_OFFSET_DATE_TIME
              .format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(data.getTimestamp()),
                    ZoneId.systemDefault())));
        o.put("queuingTime", data.getTimeQueued());
        o.put("cellName", data.getCellAddress().getCellName());
        o.put("cellType", data.getCellType());
        o.put("cellDomain", data.getCellAddress().getCellDomainName());

        JSONObject status = new JSONObject();
        status.put("code", data.getResultCode());
        status.put("msg", data.getMessage());
        o.put("status", status);

        o.put("session", data.getTransaction());
        o.put("sessionDuration", data.getTransactionDuration());
        o.put("transferPath", data.getTransferPath());

        JSONArray subject = new JSONArray();
        data.getSubject().getPrincipals().forEach(s -> subject.put(s));
        o.put("subject", subject);
        o.put("client", data.getClient());
        o.put("clientChain", data.getClientChain());
        o.put("mappedUID", data.getUid());
        o.put("mappedGID", data.getGid());
        o.put("owner", data.getOwner());
        o.put("pnfsid", data.getPnfsId());
        o.put("billingPath", data.getBillingPath());
        o.put("fileSize", data.getFileSize());
        StorageInfo info = data.getStorageInfo();
        if (info != null) {
            o.put("storageInfo", info.getStorageClass() + "@" + info.getHsm());
        }

        MoverInfoMessage moverInfoMessage = data.getMoverInfo();

        if (moverInfoMessage != null) {
            JSONObject moverInfo = BillingMessageSerializer.transform(moverInfoMessage);
            Arrays.stream(REDUNDANT_MOVER_DATA_KEYS).forEach(moverInfo::remove);
            o.put("moverInfo", moverInfo);
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






