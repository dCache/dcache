package org.dcache.notification;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;

import static java.nio.charset.StandardCharsets.UTF_8;

public class BillingMessageSerializer implements Serializer<MoverInfoMessage> {

    @Override
    public byte[] serialize(String topic, MoverInfoMessage data) {

        JSONObject o = new JSONObject();
        o.put("version", "1.0");
        o.put("msgType", data.getMessageType());
        o.put("date", DateTimeFormatter.ISO_OFFSET_DATE_TIME
                .format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(data.getTimestamp()), ZoneId.systemDefault())));
        o.put("queuingTime", data.getTimeQueued());
        o.put("cellName", data.getCellAddress().getCellName());
        o.put("cellType", data.getCellType());
        o.put("cellDomain", data.getCellAddress().getCellDomainName());

        if (!Double.isNaN(data.getMeanReadBandwidth())) {
            o.put("meanReadBandwidth", data.getMeanReadBandwidth());
        }
        if (!Double.isNaN(data.getMeanWriteBandwidth())) {
            o.put("meanWriteBandwidth", data.getMeanWriteBandwidth());
        }

        data.getReadIdle().ifPresent(d -> o.put("readIdle", d.toString()));
        data.getWriteIdle().ifPresent(d -> o.put("writeIdle", d.toString()));
        data.getReadActive().ifPresent(d -> o.put("readActive", d.toString()));
        data.getWriteActive().ifPresent(d -> o.put("writeActive", d.toString()));

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
        o.put("transferSize", data.getDataTransferred());
        o.put("transferTime", data.getConnectionTime());
        o.put("isWrite", data.isFileCreated() ? "write" : "read");

        InetSocketAddress remoteHost = ((IpProtocolInfo)data.getProtocolInfo()).getSocketAddress();

        JSONObject protocolInfo = new JSONObject();
        protocolInfo.put("protocol", data.getProtocolInfo().getProtocol());
        protocolInfo.put("versionMajor", data.getProtocolInfo().getMajorVersion());
        protocolInfo.put("versionMinor", data.getProtocolInfo().getMinorVersion());
        protocolInfo.put("port", remoteHost.getPort());
        protocolInfo.put("host", remoteHost.getAddress().getHostAddress());
        o.put("protocolInfo", protocolInfo);

        o.put("initiator", data.getInitiator());
        o.put("isP2p", data.isP2P());
        o.put("transferPath", data.getTransferPath());
        return o.toString().getBytes(UTF_8);

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}






