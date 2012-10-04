package org.dcache.services.billing.mongodb;

import org.dcache.cells.CellMessageReceiver;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.Date;

/**
 * Mongodb Logger for billing cell
 */
public class Logger implements CellMessageReceiver {

    private Mongo _mongo;
    private String _dbHost;
    private int _dbPort;
    private String _dbName;
    private DB _db;

    public void init() throws UnknownHostException {
        _mongo = new Mongo(_dbHost, _dbPort);
        _db = _mongo.getDB(_dbName);
    }

    public void messageArrived(MoverInfoMessage info) {

        DBCollection collection = _db.getCollection(info.getCellType());

        String[] clients = {"<unknown>", null};
        String protocol = "<unknown>";

        if (info.getProtocolInfo() instanceof IpProtocolInfo) {
            clients = ((IpProtocolInfo) info.getProtocolInfo()).getHosts();
            protocol = ((IpProtocolInfo) info.getProtocolInfo()).getVersionString();
        }

        BasicDBObjectBuilder subjectBuilder = BasicDBObjectBuilder.start();
        for (Principal p : info.getSubject().getPrincipals()) {
            subjectBuilder.add(p.getClass().getSimpleName(), p.getName());
        }

        BasicDBObjectBuilder objectBuilder = BasicDBObjectBuilder.start()
                .add("ts", new Date(info.getTimestamp()))
                .add("cell", info.getCellName())
                .add("cellType", info.getCellType())
                .add("messageType", info.getMessageType())
                .add("errorCode", info.getResultCode())
                .add("errorMessage", info.getMessage())
                .add("txn", info.getTransaction())
                .add("timeQeued", info.getTimeQueued())
                .add("initiator", info.getInitiator())
                .add("pnfsid", info.getPnfsId().getId())
                .add("fileSize", info.getFileSize())
                .add("bytesTransfered", info.getDataTransferred())
                .add("storageInfo", info.getStorageInfo().getStorageClass() + "@" + info.getStorageInfo().getHsm())
                .add("created", info.isFileCreated())
                .add("client", clients[0])
                .add("transferTime", info.getConnectionTime())
                .add("protocol", protocol)
                .add("subject", subjectBuilder.get());

        collection.insert(objectBuilder.get());
    }

    public void setDbHost(String dbHost) {
        _dbHost = dbHost;
    }

    public void setDbPort(int dbPort) {
        _dbPort = dbPort;
    }

    public void setDbName(String dbName) {
        _dbName = dbName;
    }
}
