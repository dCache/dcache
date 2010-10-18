/*
 * ConvertUtil.java
 *
 * Created on January 5, 2005, 12:42 PM
 */

package org.dcache.srm.client;

/**
 *
 * @author  timur
 */
public class ConvertUtil {

    /** Creates a new instance of ConvertUtil */
    public ConvertUtil() {
    }

    public static diskCacheV111.srm.RequestStatus axisRS2RS(org.dcache.srm.client.axis.RequestStatus axisrs) {
        if(axisrs == null) {
            return null;
        }
        diskCacheV111.srm.RequestStatus rs = new diskCacheV111.srm.RequestStatus();
        org.dcache.srm.client.axis.RequestFileStatus[] axisrfss = axisrs.getFileStatuses();
        if(axisrfss != null) {
            rs.fileStatuses = new diskCacheV111.srm.RequestFileStatus[axisrfss.length];
            for ( int i = 0; i<rs.fileStatuses.length ;++i) {
                rs.fileStatuses[i] = axisRFS2RFS(axisrfss[i]);
            }
        }
        rs.estTimeToStart =  axisrs.getEstTimeToStart();
        rs.requestId =   axisrs.getRequestId() ;
        rs.retryDeltaTime =  axisrs.getRetryDeltaTime();
        rs.errorMessage = axisrs.getErrorMessage();
        rs.state = axisrs.getState();
        rs.type = axisrs.getType();
        java.util.Calendar cal = axisrs.getFinishTime();
        if(cal != null) {
            rs.finishTime = cal.getTime();
        }
        cal = axisrs.getStartTime();
        if(cal != null) {
            rs.startTime = cal.getTime();
        }
        cal = axisrs.getSubmitTime();
        if(cal != null) {
            rs.submitTime = cal.getTime();
        }
        return rs;
    }

    public static org.dcache.srm.client.axis.RequestStatus RS2axisRS(diskCacheV111.srm.RequestStatus rs) {
        if(rs == null) {
            return null;
        }
        org.dcache.srm.client.axis.RequestStatus axisrs = new org.dcache.srm.client.axis.RequestStatus();
        diskCacheV111.srm.RequestFileStatus[] rfss = rs.fileStatuses;
        if(rfss != null) {
            org.dcache.srm.client.axis.RequestFileStatus[] axisrfss =
                new org.dcache.srm.client.axis.RequestFileStatus[rfss.length];
            for ( int i = 0; i<rfss.length ;++i) {
                axisrfss[i] = RFS2axisRFS(rfss[i]);
            }
            axisrs.setFileStatuses(axisrfss);
        }
        axisrs.setEstTimeToStart(rs.estTimeToStart);
        axisrs.setRequestId(rs.requestId ) ;
        axisrs.setRetryDeltaTime(rs.retryDeltaTime );
        axisrs.setErrorMessage(rs.errorMessage);
        axisrs.setState(rs.state );
        axisrs.setType(rs.type );
        if(rs.finishTime != null) {
            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.setTime(rs.finishTime);
            axisrs.setFinishTime(cal);

        }
        if(rs.startTime != null) {
            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.setTime(rs.startTime);
            axisrs.setStartTime(cal);

        }
        if(rs.submitTime != null) {
            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.setTime(rs.submitTime);
            axisrs.setSubmitTime(cal);

        }

        return axisrs;
    }

    public static diskCacheV111.srm.RequestFileStatus axisRFS2RFS(org.dcache.srm.client.axis.RequestFileStatus axisrfs) {
        if(axisrfs == null) {
            return null;
        }
        diskCacheV111.srm.RequestFileStatus rfs = new diskCacheV111.srm.RequestFileStatus();
        rfs.isCached = axisrfs.isIsCached();
        rfs.isPermanent = axisrfs.isIsPermanent();
        rfs.isPinned = axisrfs.isIsPinned();
        rfs.estSecondsToStart = axisrfs.getEstSecondsToStart();
        rfs.fileId = axisrfs.getFileId();
        rfs.permMode = axisrfs.getPermMode();
        rfs.queueOrder = axisrfs.getQueueOrder();
        rfs.size = axisrfs.getSize();
        rfs.SURL = axisrfs.getSURL();
        rfs.TURL = axisrfs.getTURL();
        rfs.checksumType = axisrfs.getChecksumType();
        rfs.checksumValue = axisrfs.getChecksumValue();
        rfs.destFilename = axisrfs.getDestFilename();
        rfs.group = axisrfs.getGroup();
        rfs.owner = axisrfs.getOwner();
        rfs.sourceFilename = axisrfs.getSourceFilename();
        rfs.state = axisrfs.getState();

        return rfs;
    }

    public static org.dcache.srm.client.axis.RequestFileStatus RFS2axisRFS( diskCacheV111.srm.RequestFileStatus rfs) {
        if(rfs == null) {
            return null;
        }
        org.dcache.srm.client.axis.RequestFileStatus axisrfs = new org.dcache.srm.client.axis.RequestFileStatus();
        axisrfs.setIsCached(rfs.isCached);
        axisrfs.setIsPermanent(rfs.isPermanent);
        axisrfs.setIsPinned(rfs.isPinned);
        axisrfs.setEstSecondsToStart(rfs.estSecondsToStart);
        axisrfs.setFileId(rfs.fileId);
        axisrfs.setPermMode(rfs.permMode);
        axisrfs.setQueueOrder(rfs.queueOrder);
        axisrfs.setSize(rfs.size);
        axisrfs.setSURL(rfs.SURL);
        axisrfs.setTURL(rfs.TURL);
        axisrfs.setChecksumType(rfs.checksumType);
        axisrfs.setChecksumValue(rfs.checksumValue);
        axisrfs.setDestFilename(rfs.destFilename);
        axisrfs.setGroup(rfs.group);
        axisrfs.setOwner(rfs.owner);
        axisrfs.setSourceFilename(rfs.sourceFilename);
        axisrfs.setState(rfs.state);

        return axisrfs;
    }

    public static diskCacheV111.srm.FileMetaData axisFMD2FMD(org.dcache.srm.client.axis.FileMetaData axisfmd) {
        if(axisfmd == null) {
            return null;
        }

        diskCacheV111.srm.RequestFileStatus fmd = new diskCacheV111.srm.RequestFileStatus();
        fmd.isCached = axisfmd.isIsCached();
        fmd.isPermanent = axisfmd.isIsPermanent();
        fmd.isPinned = axisfmd.isIsPinned();
        fmd.permMode = axisfmd.getPermMode();
        fmd.size = axisfmd.getSize();
        fmd.SURL = axisfmd.getSURL();
        fmd.checksumType = axisfmd.getChecksumType();
        fmd.checksumValue = axisfmd.getChecksumValue();
        fmd.group = axisfmd.getGroup();
        fmd.owner = axisfmd.getOwner();

        return fmd;
    }

    public static org.dcache.srm.client.axis.FileMetaData FMD2AxisFMD(diskCacheV111.srm.FileMetaData fmd) {
        if(fmd == null) {
            return null;
        }
        org.dcache.srm.client.axis.FileMetaData axisfmd =
            new org.dcache.srm.client.axis.FileMetaData();
        axisfmd.setIsCached(fmd.isCached );
        axisfmd.setIsPermanent(fmd.isPermanent);
        axisfmd.setIsPinned(fmd.isPinned);
        axisfmd.setPermMode(fmd.permMode);
        axisfmd.setSize(fmd.size);
        axisfmd.setSURL(fmd.SURL);
        axisfmd.setChecksumType(fmd.checksumType);
        axisfmd.setChecksumValue(fmd.checksumValue);
        axisfmd.setGroup(fmd.group);
        axisfmd.setOwner(fmd.owner);

        return axisfmd;
    }

    public static diskCacheV111.srm.FileMetaData[] axisFMDs2FMDs(
                                                                 org.dcache.srm.client.axis.FileMetaData[] axisfmds) {
        if(axisfmds == null) {
            return null;
        }
        diskCacheV111.srm.FileMetaData[] fmds = new diskCacheV111.srm.FileMetaData[axisfmds.length];
        for ( int i = 0; i<fmds.length ;++i) {
            fmds[i] = axisFMD2FMD(axisfmds[i]);
        }
        return fmds;
    }

    public static org.dcache.srm.client.axis.FileMetaData[] FMDs2AxisFMDs(
                                                                          diskCacheV111.srm.FileMetaData[] fmds) {
        if(fmds == null) {
            return null;
        }
        org.dcache.srm.client.axis.FileMetaData[] axisfmds =
            new org.dcache.srm.client.axis.FileMetaData[fmds.length];
        for ( int i = 0; i<fmds.length ;++i) {
            axisfmds[i] = FMD2AxisFMD(fmds[i]);
        }
        return axisfmds;
    }

}
