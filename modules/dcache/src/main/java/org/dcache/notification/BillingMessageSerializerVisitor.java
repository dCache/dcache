package org.dcache.notification;

import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.InfoMessageVisitor;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.RemoveFileInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;

public class BillingMessageSerializerVisitor implements InfoMessageVisitor {

    private byte[] data;

    @Override
    public void visit(DoorRequestInfoMessage message) {
        try (DoorRequestMessageSerializer serializer = new DoorRequestMessageSerializer()) {
            data = serializer.serialize("", message);
        }
    }

    @Override
    public void visit(MoverInfoMessage message) {
        try (MoverInfoMessageSerializer serializer = new MoverInfoMessageSerializer()) {
            data = serializer.serialize("", message);
        }
    }

    @Override
    public void visit(PoolHitInfoMessage message) {
        try (PoolHitInfoMessageSerializer serializer = new PoolHitInfoMessageSerializer()) {
            data = serializer.serialize("", message);
        }
    }

    @Override
    public void visit(RemoveFileInfoMessage message) {
        try (RemoveFileInfoMessageSerializer serializer = new RemoveFileInfoMessageSerializer()) {
            data = serializer.serialize("", message);
        }
    }

    @Override
    public void visit(StorageInfoMessage message) {
        try (StorageInfoMessageSerializer serializer = new StorageInfoMessageSerializer()) {
            data = serializer.serialize("", message);
        }
    }

    @Override
    public void visit(WarningPnfsFileInfoMessage message) {
        try (WarningPnfsFileInfoMessageSerializer serializer = new WarningPnfsFileInfoMessageSerializer()) {
            data = serializer.serialize("", message);
        }
    }

    public byte[] getData() {
        return data;
    }
}
