package org.dcache.webadmin.controller.util;

import java.util.List;
import java.util.Map;
import org.dcache.admin.webadmin.datacollector.datatypes.MoverInfo;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.businessobjects.MoverQueue;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;
import org.dcache.webadmin.view.beans.CellServicesBean;
import org.dcache.webadmin.view.beans.PoolSpaceBean;
import org.dcache.webadmin.view.beans.PoolQueueBean;
import org.dcache.webadmin.view.beans.PoolRequestQueue;

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
        returnPoolBean.setEnabled(poolBusinessObject.isEnabled());
        returnPoolBean.setFreeSpace(poolBusinessObject.getFreeSpace());
        returnPoolBean.setName(poolBusinessObject.getName());
        returnPoolBean.setPreciousSpace(poolBusinessObject.getPreciousSpace());
        returnPoolBean.setUsedSpace(poolBusinessObject.getUsedSpace());
        returnPoolBean.setRemovableSpace(poolBusinessObject.getRemovableSpace());
        returnPoolBean.setTotalSpace(poolBusinessObject.getTotalSpace());
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
        for (MoverQueue queue : poolBusinessObject.getMoverQueues()) {
            returnPoolQueueBean.addRequestQueue(queueModelToView(queue));
        }
        return returnPoolQueueBean;
    }

    private static PoolRequestQueue queueModelToView(MoverQueue moverQueue) {
        PoolRequestQueue queue = new PoolRequestQueue();
        queue.setName(moverQueue.getName());
        queue.setActive(moverQueue.getActive());
        queue.setMax(moverQueue.getMax());
        queue.setQueued(moverQueue.getQueued());
        return queue;
    }

    public static CellServicesBean cellModelToView(CellStatus cellBusinessObject) {
        CellServicesBean cellService = new CellServicesBean();
        cellService.setCreatedDateTime(cellBusinessObject.getCreatedDateTime());
        cellService.setDomainName(cellBusinessObject.getDomainName());
        cellService.setEventQueueSize(cellBusinessObject.getEventQueueSize());
        cellService.setName(cellBusinessObject.getName());
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
        transfer.setPool(moverInfo.getIoDoorEntry().getPool());
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
}
