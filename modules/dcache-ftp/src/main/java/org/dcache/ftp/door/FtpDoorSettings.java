package org.dcache.ftp.door;

import java.io.File;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.PnfsHandler;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;
import org.dcache.util.Option;
import org.dcache.util.OptionParser;
import org.dcache.util.PortRange;

/**
 * Object holding configuration options for FTP doors.
 *
 * Settings can be injected using {@link OptionParser}.
 */
public class FtpDoorSettings
{
    @Option(name = "poolManager",
            description = "Well known name of the pool manager",
            defaultValue = "PoolManager")
    protected CellPath poolManager;

    @Option(name = "pnfsManager",
            description = "Well known name of the PNFS manager",
            defaultValue = "PnfsManager")
    protected CellPath pnfsManager;

    @Option(name = "gplazma",
            description = "Cell path to gPlazma",
            defaultValue = "gPlazma")
    protected CellPath gPlazma;

    @Option(name = "billing",
            description = "Cell path to billing",
            defaultValue = "billing")
    protected CellPath billing;

    @Option(name = "clientDataPortRange",
            defaultValue = "0")
    protected PortRange portRange;

    /**
     * Name or IP address of the interface on which we listen for
     * connections from the pool in case an adapter is used.
     */
    @Option(name = "ftp-adapter-internal-interface",
            description = "Interface to bind to")
    protected String internalAddress;

    @Option(name = "read-only",
            description = "Whether to mark the FTP door read only",
            defaultValue = "false")
    protected boolean readOnly;

    @Option(name = "maxRetries",
            defaultValue = "3")
    protected int maxRetries;

    @Option(name = "poolManagerTimeout",
            defaultValue = "1500")
    protected int poolManagerTimeout;

    @Option(name = "poolManagerTimeoutUnit",
            defaultValue = "SECONDS")
    protected TimeUnit poolManagerTimeoutUnit;

    @Option(name = "pnfsTimeout",
            defaultValue = "60")
    protected int pnfsTimeout;

    @Option(name = "pnfsTimeoutUnit",
            defaultValue = "SECONDS")
    protected TimeUnit pnfsTimeoutUnit;

    @Option(name = "poolTimeout",
            defaultValue = "300")
    protected int poolTimeout;

    @Option(name = "poolTimeoutUnit",
            defaultValue = "SECONDS")
    protected TimeUnit poolTimeoutUnit;

    @Option(name = "retryWait",
            defaultValue = "30",
            unit = "seconds")
    protected int retryWait;

    /**
     * Size of the largest block used in the socket adapter in mode
     * E. Blocks larger than this are divided into smaller blocks.
     */
    @Option(name = "maxBlockSize",
            defaultValue = "131072",
            unit = "bytes")
    protected int maxBlockSize;

    @Option(name = "deleteOnConnectionClosed",
            description = "Whether to remove files on incomplete transfers",
            defaultValue = "false")
    protected boolean removeFileOnIncompleteTransfer;

    /**
     * True if passive transfers have to be relayed through the door,
     * i.e., the client must not connect directly to the pool.
     */
    @Option(name = "proxyPassive",
            description = "Whether proxy is required for passive transfers",
            defaultValue = "false")
    protected boolean isProxyRequiredOnPassive;

    /**
     * True if active transfers have to be relayed through the door.
     */
    @Option(name = "proxyActive",
            description = "Whether proxy is required for active transfers",
            defaultValue = "false")
    protected boolean isProxyRequiredOnActive;

    /**
     * File (StageConfiguration.conf) containing DNs and FQANs whose owner are allowed to STAGE files
     * (i.e. allowed to copy file from dCache in case file is stored on tape but not on disk).
     * /opt/d-cache/config/StageConfiguration.conf
     * By default, such file does not exist, so that tape protection feature is not in use.
     */
    @Option(name = "stageConfigurationFilePath",
            description = "File containing DNs and FQANs for which STAGING is allowed",
            defaultValue = "")
    protected String stageConfigurationFilePath;

    /**
     * transferTimeout (in seconds)
     * <p>
     * Is used for waiting for the end of transfer after the pool
     * already notified us that the file transfer is finished. This is
     * needed because we are using adapters.  If timeout is 0, there is
     * no timeout.
     */
    @Option(name = "transfer-timeout",
            description = "Transfer timeout",
            defaultValue = "0",
            unit = "seconds")
    protected int transferTimeout;

    @Option(name = "tlog",
            description = "Path to FTP transaction log")
    protected String tLogRoot;

    /**
     * wlcg demands that support for overwrite in srm and gridftp
     * be off by default.
     */
    @Option(name = "overwrite",
            defaultValue = "false")
    protected boolean overwrite;

    @Option(name = "io-queue")
    protected String ioQueueName;

    @Option(name = "maxStreamsPerClient",
            description = "Maximum allowed streams per client in mode E",
            defaultValue = "-1",                   // -1 = unlimited
            unit = "streams")
    protected int maxStreamsPerClient;

    @Option(name = "defaultStreamsPerClient",
            description = "Default number of streams per client in mode E",
            defaultValue = "1",
            unit = "streams")
    protected int defaultStreamsPerClient;

    @Option(name = "perfMarkerPeriod",
            description = "Performance marker period",
            defaultValue = "90")
    protected long performanceMarkerPeriod;

    @Option(name = "perfMarkerPeriodUnit",
            description = "Performance marker period unit",
            defaultValue = "SECONDS")
    protected TimeUnit performanceMarkerPeriodUnit;

    @Option(name = "root",
            description = "Root path")
    protected String root;

    @Option(name = "upload",
            description = "Upload directory")
    protected File uploadPath;

    public PortRange getPortRange()
    {
        return portRange;
    }

    public String getInternalAddress()
    {
        return internalAddress;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    public int getPoolManagerTimeout()
    {
        return poolManagerTimeout;
    }

    public TimeUnit getPoolManagerTimeoutUnit()
    {
        return poolManagerTimeoutUnit;
    }

    public int getPnfsTimeout()
    {
        return pnfsTimeout;
    }

    public TimeUnit getPnfsTimeoutUnit()
    {
        return pnfsTimeoutUnit;
    }

    public int getPoolTimeout()
    {
        return poolTimeout;
    }

    public TimeUnit getPoolTimeoutUnit()
    {
        return poolTimeoutUnit;
    }

    public int getRetryWait()
    {
        return retryWait;
    }

    public int getMaxBlockSize()
    {
        return maxBlockSize;
    }

    public boolean isRemoveFileOnIncompleteTransfer()
    {
        return removeFileOnIncompleteTransfer;
    }

    public boolean isProxyRequiredOnPassive()
    {
        return isProxyRequiredOnPassive;
    }

    public boolean isProxyRequiredOnActive()
    {
        return isProxyRequiredOnActive;
    }

    public String getStageConfigurationFilePath()
    {
        return stageConfigurationFilePath;
    }

    public String getTlogRoot()
    {
        return tLogRoot;
    }

    public boolean isOverwrite()
    {
        return overwrite;
    }

    public String getIoQueueName()
    {
        return ioQueueName;
    }

    public int getMaxStreamsPerClient()
    {
        return maxStreamsPerClient;
    }

    public int getDefaultStreamsPerClient()
    {
        return defaultStreamsPerClient;
    }

    public long getPerformanceMarkerPeriod()
    {
        return performanceMarkerPeriod;
    }

    public TimeUnit getPerformanceMarkerPeriodUnit()
    {
        return performanceMarkerPeriodUnit;
    }

    public String getRoot()
    {
        return root;
    }

    public File getUploadPath()
    {
        return uploadPath;
    }

    public CellStub createBillingStub(CellEndpoint cellEndpoint)
    {
        return new CellStub(cellEndpoint, billing);
    }

    public CellStub createGplazmaStub(CellEndpoint cellEndpoint)
    {
        return new CellStub(cellEndpoint, gPlazma, 30000);
    }

    public CellStub createPoolStub(CellEndpoint cellEndpoint)
    {
        return new CellStub(cellEndpoint, null, poolTimeout, poolTimeoutUnit);
    }

    public CellStub createPoolManagerStub(CellEndpoint cellEndpoint)
    {
        return new CellStub(cellEndpoint, poolManager, poolManagerTimeout, poolManagerTimeoutUnit);
    }

    public PnfsHandler createPnfsHandler(CellEndpoint cellEndpoint)
    {
        return new PnfsHandler(new CellStub(cellEndpoint, pnfsManager, pnfsTimeout, pnfsTimeoutUnit));
    }
}