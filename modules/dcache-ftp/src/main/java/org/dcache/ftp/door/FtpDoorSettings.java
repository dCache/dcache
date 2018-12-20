package org.dcache.ftp.door;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.PnfsHandler;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolManagerHandler;
import org.dcache.poolmanager.PoolManagerStub;
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

    @Option(name = "kafka",
            description = "Kafka service enabled",
            defaultValue = "false")
    protected boolean isKafkaEnabled;

    @Option(name = "bootstrap-server-kafka")
    protected String kafkaBootstrapServer;

    @Option(name = "kafka-max-block",
            defaultValue = "1")
    protected long kafkaMaxBlock;

    @Option(name = "kafka-max-block-units",
            defaultValue = "SECONDS")
    protected TimeUnit kafkaMaxBlockUnits;

    @Option(name = "retries-kafka")
    protected String kafkaRetries;


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

    @Option(name = "allowAnonymousStaging",
            description = "Whether anonymous users are allowed to stage files",
            defaultValue = "true")
    protected boolean allowAnonymousStaging;

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

    @Option(name = "list-format",
            description = "Output format for the LIST command",
            defaultValue = "ls-l")
    protected String listFormat;

    @Option(name = "log-aborted-transfers",
            description = "If enabled, the state of a transfer is logged when the client aborts.",
            defaultValue = "false")
    protected boolean logAbortedTransfers;

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

    public boolean logAbortedTransfers()
    {
        return logAbortedTransfers;
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

    public boolean isAnonymousStagingAllowed()
    {
        return allowAnonymousStaging;
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

    public String getListFormat()
    {
        return listFormat;
    }

    /**
     *  Returns Kafka service enabled
     *  If enabled, the various dCache services, like pools and doors will publish messages to
     *   a Kafka cluster after each transfer.
     *
     * @return true if the user wants to send messages to Kafka
     */
    public boolean isKafkaEnabled() {
        return isKafkaEnabled;
    }

    /**
     * Returns a list of host/port pairs (brokers) to use for establishing the initial connection to the Kafka cluster.
     * This list is just used to discover the rest of the brokers in the cluster and should be in the form
     * host1:port1,host2:port2,....
     *
     * @return    the list of  of host/port pairs
     */
    public String getKafkaBootstrapServer() {
        return kafkaBootstrapServer;
    }

    /**
     * Returns the parameter that controls how long
     * how long the producer will block when calling send(). By default set to 60000.
     *
     * @retrun a timeframe during which producer will block when calling send()
     */
    public String getKafkaMaxBlockMs() {
        return String.valueOf(TimeUnit.MILLISECONDS.convert(kafkaMaxBlock, kafkaMaxBlockUnits));
    }

    /**
     * Returns the number of retries that the producer will retry sending the messages before failing it.
     *
     *  @return number of retries, set to 0 by default
     */
    public String getKafkaRetries() {
        return kafkaRetries;
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

    public PoolManagerStub createPoolManagerStub(CellEndpoint cellEndpoint, CellAddressCore cellAddress, PoolManagerHandler handler)
    {
        PoolManagerStub stub = new PoolManagerStub();
        stub.setCellEndpoint(cellEndpoint);
        stub.setCellAddress(cellAddress);
        stub.setHandler(handler);
        stub.setMaximumPoolManagerTimeout(poolManagerTimeout);
        stub.setMaximumPoolManagerTimeoutUnit(poolManagerTimeoutUnit);
        stub.setMaximumPoolTimeout(poolTimeout);
        stub.setMaximumPoolTimeoutUnit(poolTimeoutUnit);
        return stub;
    }

    public KafkaProducer createKafkaProducer(String bootstrap_server,
                                             String client_id,
                                             String max_block_ms,
                                             String retries)
    {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, client_id);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.dcache.notification.DoorRequestMessageSerializer");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, max_block_ms);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);

        return new KafkaProducer<>(props);
    }

    public PnfsHandler createPnfsHandler(CellEndpoint cellEndpoint)
    {
        return new PnfsHandler(new CellStub(cellEndpoint, pnfsManager, pnfsTimeout, pnfsTimeoutUnit));
    }
}