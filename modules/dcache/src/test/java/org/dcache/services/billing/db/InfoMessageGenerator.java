package org.dcache.services.billing.db;

import javax.security.auth.Subject;

import java.net.InetSocketAddress;
import java.util.Random;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfoMessage;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PnfsBaseInfo;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.StorageData;

/**
 * Generates InfoMessage with randomized values.
 *
 * @author arossi
 */
public class InfoMessageGenerator {

    final String CELL_PATH = "testCell@testDomain";
    final Random r = new Random(System.currentTimeMillis());

    PnfsBaseInfo newPnfsInfo(int random) {
        switch (random) {
        case 0:
            return new MoverData(newMoverMessage());
        case 1:
            return new DoorRequestData(newDoorMessage());
        case 2:
            return new StorageData(newStorageMessage());
        case 3:
            return new PoolHitData(newHitMessage());
        }
        return null;
    }

    DoorRequestInfoMessage newDoorMessage() {
        DoorRequestInfoMessage info = new DoorRequestInfoMessage(CELL_PATH, "transfer");
        info.setFileSize(Math.abs(r.nextLong()) % 10000000000000L);
        info.setPath(getFilePath());
        info.setTransaction(getTransaction());
        info.setPnfsId(new PnfsId(getPnsfId()));
        Subject subject = new Subject();
        subject.getPrincipals().add(new UserNamePrincipal(System.getProperty("user.name")));
        subject.getPrincipals().add(new UidPrincipal(0));
        subject.getPrincipals().add(new GidPrincipal(0, true));
        info.setSubject(subject);
        info.setClient("<unknown>");
        info.setTransactionDuration(1000L * (Math.abs(r.nextInt())));
        info.setTimeQueued(1000L * (Math.abs(r.nextInt())));
        return info;
    }

    PoolHitInfoMessage newHitMessage() {
        PoolHitInfoMessage info = new PoolHitInfoMessage(CELL_PATH, new PnfsId(getPnsfId()));
        info.setFileCached(r.nextInt() % 17 == 0);
        info.setFileSize(Math.abs(r.nextLong()) % 10000000000000L);
        info.setPath(getFilePath());
        info.setTransaction(getTransaction());
        return info;
    }

    StorageInfoMessage newStorageMessage() {
        StorageInfoMessage info =
                new StorageInfoMessage(CELL_PATH, new PnfsId(getPnsfId()), r.nextInt() % 2 != 0);
        info.setFileSize(Math.abs(r.nextLong()) % 10000000000000L);
        info.setTransaction(getTransaction());
        info.setFileSize(Math.abs(r.nextLong()) % 10000000000000L);
        info.setStorageInfo(newStorageInfo());
        info.setTransferTime(Math.abs(r.nextLong()) % 1000000000L);
        info.setTimeQueued(1000L * (Math.abs(r.nextInt())));
        return info;
    }

    MoverInfoMessage newMoverMessage() {
        MoverInfoMessage info = new MoverInfoMessage(CELL_PATH, new PnfsId(
                        getPnsfId()));
        info.setTransaction(getTransaction());
        info.setFileSize(Math.abs(r.nextLong()) % 10000000000000L);
        info.setTransferAttributes(info.getFileSize(),
                        Math.abs(r.nextLong()) % 1000000000L, newProtocolInfo());
        info.setStorageInfo(newStorageInfo());
        info.setFileCreated(r.nextInt() % 29 == 0);
        info.setInitiator("door:testDoor@testDomain:0000000000000-0");
        return info;
    }

    StorageInfo newStorageInfo() {
        StorageInfo si = new GenericStorageInfo();
        si.setStorageClass("test@localhost");
        return si;
    }

    ProtocolInfo newProtocolInfo() {
        return new IpProtocolInfo() {

            private final long serialVersionUID = -6947189307164375728L;

            @Override
            public String getVersionString() {
                return "2.0";
            }

            @Override
            public String getProtocol() {
                return "file";
            }

            @Override
            public int getMinorVersion() {
                return 2;
            }

            @Override
            public int getMajorVersion() {
                return 0;
            }

            @Override
            public InetSocketAddress getSocketAddress() {
                return new InetSocketAddress("localhost", 0);
            }
        };
    }

    private String getTransaction() {
        return "pool:testPool@localhost:" + Math.abs(r.nextLong()) + "-1";
    }

    private String getPnsfId() {
        return "00000000000000" + (Math.abs(r.nextInt()) % 10000);
    }

    private String getFilePath() {
        return "/tmp/test_" + Math.abs(r.nextLong());
    }
}
