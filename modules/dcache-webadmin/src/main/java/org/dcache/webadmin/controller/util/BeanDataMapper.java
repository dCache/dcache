package org.dcache.webadmin.controller.util;

import java.util.Date;
import java.util.List;
import java.util.Map;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;
import diskCacheV111.util.VOInfo;
import diskCacheV111.vehicles.RestoreHandlerInfo;

import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.admin.webadmin.datacollector.datatypes.MoverInfo;
import org.dcache.poolmanager.Partition;
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
        PoolQueueBean returnPoolQueueBean = new PoolQueueBean();
        returnPoolQueueBean.setName(poolBusinessObject.getName());
        for (PoolCostInfo.NamedPoolQueueInfo queue :
                poolBusinessObject.getCostinfo().getMoverQueues().values()) {
            returnPoolQueueBean.addRequestQueue(queuesModelToView(queue));
        }
        return returnPoolQueueBean;
    }

    private static PoolRequestQueue queuesModelToView(PoolCostInfo.NamedPoolQueueInfo moverQueue) {
        PoolRequestQueue queue = new PoolRequestQueue();
        queue.setName(moverQueue.getName());
        queue.setActive(moverQueue.getActive());
        queue.setMax(moverQueue.getMaxActive());
        queue.setQueued(moverQueue.getQueued());
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

    public static ActiveTransfersBean moverModelToView(MoverInfo moverInfo) {
        ActiveTransfersBean transfer = new ActiveTransfersBean();
        transfer.setCellDomainName(moverInfo.getIoDoorInfo().getDomainName());
        transfer.setCellName(moverInfo.getIoDoorInfo().getCellName());
        transfer.setOwner(moverInfo.getIoDoorInfo().getOwner());
        if (moverInfo.getIoDoorEntry().getPnfsId() == null) {
            transfer.setPnfsId("");
        } else {
            transfer.setPnfsId(moverInfo.getIoDoorEntry().getPnfsId().toString());
        }
        if (moverInfo.getIoDoorEntry().getPool() != null) {
            transfer.setPool(moverInfo.getIoDoorEntry().getPool());
        }
        transfer.setProcess(moverInfo.getIoDoorInfo().getProcess());
        transfer.setProtocolFamily(moverInfo.getIoDoorInfo().getProtocolFamily());
        transfer.setProtocolVersion(moverInfo.getIoDoorInfo().getProtocolVersion());
        transfer.setReplyHost(moverInfo.getIoDoorEntry().getReplyHost());
        transfer.setSerialId(moverInfo.getIoDoorEntry().getSerialId());
        if (moverInfo.getIoDoorEntry().getStatus() == null) {
            transfer.setStatus("");
        } else {
            String status = moverInfo.getIoDoorEntry().getStatus();
            status = status.replace(" ", "\n");
            transfer.setStatus(status);
        }
        transfer.setWaitingSince(moverInfo.getIoDoorEntry().getWaitingSince());
        if (moverInfo.hasJobInfo()) {
            transfer.setBytesTransferred(moverInfo.getIoJobInfo().getBytesTransferred());
            transfer.setJobId(moverInfo.getIoJobInfo().getJobId());
            transfer.setLastTransferred(moverInfo.getIoJobInfo().getLastTransferred());
            transfer.setState(moverInfo.getIoJobInfo().getStatus());
            transfer.setTransferTime(moverInfo.getIoJobInfo().getTransferTime());
        }
        return transfer;
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
        String result = (linkGroup.isOnlineAllowed()
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
        return result;
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
        String result = mapAccessLatency(reservation.getAccessLatency())
                + mapRetentionPolicy(reservation.getRetentionPolicy());
        return result;
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
