package org.dcache.webadmin.controller.util;

import java.util.Date;
import java.util.List;
import java.util.Map;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.NamedPoolQueueInfo;
import diskCacheV111.pools.PoolCostInfo.PoolQueueInfo;
import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;
import diskCacheV111.util.UserInfo;
import diskCacheV111.util.VOInfo;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.poolmanager.Partition;
import org.dcache.util.TransferCollector;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.util.AccessLatency;
import org.dcache.webadmin.model.util.RetentionPolicy;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;
import org.dcache.webadmin.view.beans.CellServicesBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolRequestQueue;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.pages.poolselectionsetup.beans.PartitionsBean;
import org.dcache.webadmin.view.pages.spacetokens.beans.LinkGroupBean;
import org.dcache.webadmin.view.pages.spacetokens.beans.SpaceReservationBean;
import org.dcache.webadmin.view.pages.tapetransferqueue.beans.RestoreBean;

/**
 * Does the mapping between modelobjects and viewobjects
 * @author jans
 */
public class BeanDataMapper {

    public static PoolSpaceBean poolModelToView(Pool poolBusinessObject,
            Map<String, List<String>> domainMap) {
        PoolSpaceBean returnPoolBean = poolModelToView(poolBusinessObject);
        returnPoolBean.setDomainName(
                NamedCellUtil.findDomainOfUniqueCell(
                domainMap, returnPoolBean.getName()));
        return returnPoolBean;
    }

    private static PoolSpaceBean poolModelToView(Pool poolBusinessObject) {
        PoolSpaceBean returnPoolBean = new PoolSpaceBean();
        returnPoolBean.setEnabled(poolBusinessObject.getSelectionPool().isEnabled());
        returnPoolBean.setPoolMode(poolBusinessObject.getSelectionPool().getPoolMode());
        returnPoolBean.setName(poolBusinessObject.getName());
        PoolCostInfo poolCostInfo = poolBusinessObject.getCostinfo();
        returnPoolBean.setFreeSpace(poolCostInfo.getSpaceInfo().getFreeSpace());
        returnPoolBean.setPreciousSpace(poolCostInfo.getSpaceInfo().getPreciousSpace());
        returnPoolBean.setUsedSpace(poolCostInfo.getSpaceInfo().getUsedSpace());
        returnPoolBean.setRemovableSpace(poolCostInfo.getSpaceInfo().getRemovableSpace());
        returnPoolBean.setTotalSpace(poolCostInfo.getSpaceInfo().getTotalSpace());
        return returnPoolBean;
    }

    public static PoolQueueBean poolQueueModelToView(Pool poolBusinessObject,
            Map<String, List<String>> domainMap) {
        PoolQueueBean returnPoolQueueBean = poolQueueModelToView(poolBusinessObject);
        returnPoolQueueBean.setDomainName(
                NamedCellUtil.findDomainOfUniqueCell(
                domainMap, returnPoolQueueBean.getName()));

        return returnPoolQueueBean;
    }

    private static PoolQueueBean poolQueueModelToView(Pool poolBusinessObject) {
        PoolCostInfo costInfo = poolBusinessObject.getCostinfo();
        PoolQueueBean returnPoolQueueBean = new PoolQueueBean();
        returnPoolQueueBean.setName(poolBusinessObject.getName());
        returnPoolQueueBean.addRequestQueue(queuesModelToView("Stores", costInfo.getStoreQueue(), -1));
        returnPoolQueueBean.addRequestQueue(queuesModelToView("Restores", costInfo.getRestoreQueue(), -1));
        returnPoolQueueBean.addRequestQueue(queuesModelToView("Movers", costInfo.getMoverQueue()));
        returnPoolQueueBean.addRequestQueue(queuesModelToView("P2P-Server", costInfo.getP2pQueue()));
        returnPoolQueueBean.addRequestQueue(queuesModelToView("P2P-Client", costInfo.getP2pClientQueue(), -1));
        for (NamedPoolQueueInfo info : costInfo.getExtendedMoverHash().values() ) {
            if (info != null) {
                returnPoolQueueBean.addRequestQueue(queuesModelToView(info));
            }
        }
        return returnPoolQueueBean;
    }

    private static PoolRequestQueue queuesModelToView(NamedPoolQueueInfo info) {
        return queuesModelToView(info.getName(), info);
    }

    private static PoolRequestQueue queuesModelToView(String name, PoolQueueInfo info) {
        if (info == null) {
            return queuesModelToView(name, info, -1);
        }
        return queuesModelToView(name, info, info.getMaxActive());
    }

    private static PoolRequestQueue queuesModelToView(String name, PoolQueueInfo info, int max) {
        PoolRequestQueue queue = new PoolRequestQueue();
        queue.setName(name);
        queue.setMax(max);
        if (info == null) {
            queue.setActive(-1);
            queue.setQueued(-1);
        } else {
            queue.setActive(info.getActive());
            queue.setQueued(info.getQueued());
        }
        return queue;
    }

    public static CellServicesBean cellModelToView(CellStatus cellBusinessObject) {
        CellServicesBean cellService = new CellServicesBean();
        cellService.setCreatedDateTime(cellBusinessObject.getCreatedDateTime());
        cellService.setDomainName(cellBusinessObject.getDomainName());
        cellService.setEventQueueSize(cellBusinessObject.getEventQueueSize());
        cellService.setName(cellBusinessObject.getCellName());
        cellService.setPing(cellBusinessObject.getPing());
        cellService.setThreadCount(cellBusinessObject.getThreadCount());
        cellService.setVersion(cellBusinessObject.getVersion());
        return cellService;
    }

    public static ActiveTransfersBean moverModelToView(TransferCollector.Transfer transfer) {
        ActiveTransfersBean bean = new ActiveTransfersBean();
        bean.setDomainName(transfer.door().getDomainName());
        bean.setCellName(transfer.door().getCellName());
        if (transfer.session().getSubject() == null) {
            bean.setUserInfo(new UserInfo());
        } else {
            bean.setSubject(transfer.session().getSubject());
        }
        if (transfer.session().getPnfsId() == null) {
            bean.setPnfsId("");
        } else {
            bean.setPnfsId(transfer.session().getPnfsId().toString());
        }
        if (transfer.session().getPool() != null) {
            bean.setPool(transfer.session().getPool());
        }
        bean.setProcess(transfer.door().getProcess());
        bean.setProtocol(transfer.door().getProtocolFamily(),
                        transfer.door().getProtocolVersion());
        bean.setReplyHost(transfer.session().getReplyHost());
        bean.setSerialId(transfer.session().getSerialId());
        if (transfer.session().getStatus() == null) {
            bean.setSessionStatus("");
        } else {
            String status = transfer.session().getStatus();
            status = status.replace(" ", "\n");
            bean.setSessionStatus(status);
        }
        bean.setWaitingSince(transfer.session().getWaitingSince());
        IoJobInfo mover = transfer.mover();
        if (mover != null) {
            bean.setBytesTransferred(mover.getBytesTransferred());
            bean.setMoverId(mover.getJobId());
            bean.setMoverStatus(mover.getStatus());
            bean.setTransferTime(mover.getTransferTime());
        }
        return bean;
    }

    public static LinkGroupBean linkGroupModelToView(LinkGroup linkGroup) {
        LinkGroupBean newBean = new LinkGroupBean();
        newBean.setId(linkGroup.getId());
        newBean.setAllowed(mapLinkGroupAllowanceFlags(linkGroup));
        newBean.setFree(linkGroup.getFreeSpace());
        newBean.setName(linkGroup.getName());
        newBean.setAvailable(linkGroup.getAvailableSpace());
        newBean.setReserved(linkGroup.getReservedSpace());
        if (linkGroup.getVOs() != null) {
            newBean.setVos(extractVos(linkGroup.getVOs()));
        }
        return newBean;
    }

    private static String mapLinkGroupAllowanceFlags(LinkGroup linkGroup) {
        return (linkGroup.isOnlineAllowed()
                ? AccessLatency.ONLINE.getShortcut().toUpperCase()
                : AccessLatency.ONLINE.getShortcut().toLowerCase())
               + (linkGroup.isNearlineAllowed()
                ? AccessLatency.NEARLINE.getShortcut().toUpperCase()
                : AccessLatency.NEARLINE.getShortcut().toLowerCase())
               + (linkGroup.isReplicaAllowed()
                ? RetentionPolicy.REPLICA.getShortcut().toUpperCase()
                : RetentionPolicy.REPLICA.getShortcut().toLowerCase())
               + (linkGroup.isCustodialAllowed()
                ? RetentionPolicy.CUSTODIAL.getShortcut().toUpperCase()
                : RetentionPolicy.CUSTODIAL.getShortcut().toLowerCase());
    }

    public static SpaceReservationBean spaceReservationModelToView(Space reservation) {
        SpaceReservationBean newReservation = new SpaceReservationBean();
        newReservation.setAllocatedSpace(reservation.getAllocatedSpaceInBytes());
        newReservation.setCreated(new Date(reservation.getCreationTime()).toString());
        newReservation.setDescription(reservation.getDescription());
        newReservation.setExpiration(reservation.getExpirationTime());
        newReservation.setId(Long.toString(reservation.getId()));
        newReservation.setLifetime((reservation.getExpirationTime() == null) ? -1 : reservation.getCreationTime() + reservation.getExpirationTime());
        newReservation.setLinkGroupRef(reservation.getLinkGroupId());
        newReservation.setState(reservation.getState().toString());
        newReservation.setStorage(mapReservationAllowanceFlags(reservation));
        newReservation.setUsedSpace(reservation.getUsedSizeInBytes());
        newReservation.setSize(reservation.getSizeInBytes());
        newReservation.setVogroup(reservation.getVoGroup() + ":" + reservation.getVoRole());
        return newReservation;
    }

    public static RestoreBean restoreInfoModelToView(RestoreInfo info) {
        RestoreHandlerInfo handler = info.getRestoreHandler();
        RestoreBean bean = new RestoreBean(handler.getName(),
                handler.getErrorCode(), handler.getErrorMessage());
        bean.setClients(handler.getClientCount());
        bean.setPool(handler.getPool());
        bean.setRetries(handler.getRetryCount());
        bean.setStartTime(handler.getStartTime());
        bean.setStatus(handler.getStatus());
        return bean;
    }

    public static PartitionsBean partitionModelToView(String name, Partition partition) {
        PartitionsBean bean = new PartitionsBean();
        bean.setPartitionName(name);
        bean.setProperties(partition.getAllProperties());
        return bean;
    }

    private static String mapReservationAllowanceFlags(Space reservation) {
        return mapAccessLatency(reservation.getAccessLatency())
               + mapRetentionPolicy(reservation.getRetentionPolicy());
    }

    private static String mapAccessLatency(diskCacheV111.util.AccessLatency accessLatency) {
        if (accessLatency != null) {
            return AccessLatency.parseStringValue(accessLatency.toString()).getShortcut();
        }
        return "";
    }

    private static String mapRetentionPolicy(diskCacheV111.util.RetentionPolicy retentionPolicy) {
        if (retentionPolicy != null) {
            return RetentionPolicy.parseStringValue(retentionPolicy.toString()).getShortcut();
        }
        return "";
    }

    private static String extractVos(VOInfo[] Vos) {
        StringBuilder builder = new StringBuilder();
        for (VOInfo vo : Vos) {
            builder.append(vo.toString()).append(";");
        }
        return builder.toString();
    }
}
