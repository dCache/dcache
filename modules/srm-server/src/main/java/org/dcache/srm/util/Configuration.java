// $Id$

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

/*
 * Configuration.java
 *
 * Created on April 23, 2003, 10:19 AM
 */

package org.dcache.srm.util;

import com.google.common.base.Strings;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMUserPersistenceManager;
import org.dcache.srm.client.Transport;


/**
 *
 * @author  timur
 */
public class Configuration {

    private static final String INFINITY = "infinity";

    public static final String LS_PARAMETERS = "ls";
    public static final String PUT_PARAMETERS = "put";
    public static final String GET_PARAMETERS = "get";
    public static final String COPY_PARAMETERS = "copy";
    public static final String BRINGONLINE_PARAMETERS = "bringonline";
    public static final String RESERVE_PARAMETERS = "reserve";

    protected boolean debug = false;

    protected String urlcopy="../scripts/urlcopy.sh";

    protected String gsiftpclinet = "globus-url-copy";

    protected boolean gsissl = true;


    protected int buffer_size=2048;
    protected int tcp_buffer_size;
    private int parallel_streams=10;

    protected int port=8443;
    protected long authzCacheLifetime = 180;
    protected String srm_root="/";
    protected String proxies_directory = "../proxies";
    protected int timeout=60*60; //one hour
    protected String timeout_script="../scripts/timeout.sh";
    /**
     * Host to use in the surl (srm url) of the
     * local file, when giving the info (metadata) to srm clients
     */
    private String srmHost;
    /**
     * A host part of the srm url (surl) is used to determine if the surl
     * references file in this storage system.
     * In case of the copy operation, srm needs to be able to tell the
     * local surl from the remote one.
     * Also SRM needs to  refuse to perform operations on non local srm urls
     * This collection cosists of hosts that are cosidered local by this srm server.
     * This parameter has to be a collection because in case of the multihomed
     * or distributed server it may have more than one network name.
     *
     */
    private final Set<String> localSrmHosts=new HashSet<>();
    private SRMAuthorization authorization;

    // scheduler parameters

    protected int getMaxNumOfRetries=10;
    protected long getRetryTimeout=60000;
    private long getSwitchToAsynchronousModeDelay = 0;

    protected int lsMaxNumOfRetries=10;
    protected long lsRetryTimeout=60000;
    private long lsSwitchToAsynchronousModeDelay = 0;

    protected long bringOnlineRetryTimeout=60000;
    protected int bringOnlineMaxNumOfRetries=10;
    private long bringOnlineSwitchToAsynchronousModeDelay = 0;

    protected int putMaxNumOfRetries=10;
    protected long putRetryTimeout=60000;
    private long putSwitchToAsynchronousModeDelay = 0;

    protected int reserveSpaceMaxNumOfRetries=10;
    protected long reserveSpaceRetryTimeout=60000;

    protected int copyMaxNumOfRetries=10;
    protected long copyRetryTimeout=60000;


    protected long getLifetime = 24*60*60*1000;
    protected long bringOnlineLifetime = 24*60*60*1000;
    protected long putLifetime = 24*60*60*1000;
    protected long copyLifetime = 24*60*60*1000;
    protected long reserveSpaceLifetime = 24*60*60*1000;
    protected long defaultSpaceLifetime = 24*60*60*1000;

    protected boolean useUrlcopyScript=false;
    protected boolean useDcapForSrmCopy=false;
    protected boolean useGsiftpForSrmCopy=true;
    protected boolean useHttpForSrmCopy=true;
    protected boolean useFtpForSrmCopy=true;
    protected boolean recursiveDirectoryCreation=false;
    protected boolean advisoryDelete=false;
    protected String nextRequestIdStorageTable = "srmnextrequestid";
    protected long storage_info_update_period = TimeUnit.SECONDS.toMillis(30);
    protected String qosPluginClass = null;
    protected String qosConfigFile = null;
    private String getPriorityPolicyPlugin="DefaultJobAppraiser";
    private String bringOnlinePriorityPolicyPlugin="DefaultJobAppraiser";
    private String putPriorityPolicyPlugin="DefaultJobAppraiser";
    private String lsPriorityPolicyPlugin="DefaultJobAppraiser";
    private String reserveSpacePriorityPolicyPlugin="DefaultJobAppraiser";
    private Integer maxQueuedJdbcTasksNum ; //null by default
    private Integer jdbcExecutionThreadNum;//null by default
    private String credentialsDirectory="/opt/d-cache/credentials";
    private boolean overwrite = false;
    private boolean overwrite_by_default = false;
    private int sizeOfSingleRemoveBatch = 100;
    private SRMUserPersistenceManager srmUserPersistenceManager;
    private int maxNumberOfLsEntries = 1000;
    private int maxNumberOfLsLevels = 100;
    private boolean clientDNSLookup=false;
    private String counterRrdDirectory = null;
    private String gaugeRrdDirectory = null;
    protected String clientTransport = Transport.GSI.name();
    private DataSource dataSource;
    private PlatformTransactionManager transactionManager;

    private Map<String,DatabaseParameters> databaseParameters =
        new HashMap<>();

    /** Creates a new instance of Configuration */
    public Configuration() {
        databaseParameters.put(PUT_PARAMETERS, new DatabaseParameters("Put"));
        databaseParameters.put(GET_PARAMETERS, new DatabaseParameters("Get"));
        databaseParameters.put(LS_PARAMETERS, new DatabaseParameters("Ls"));
        databaseParameters.put(COPY_PARAMETERS, new DatabaseParameters("Copy"));
        databaseParameters.put(BRINGONLINE_PARAMETERS, new DatabaseParameters("Bring Online"));
        databaseParameters.put(RESERVE_PARAMETERS, new DatabaseParameters("Reserve Space"));
    }

    /** Getter for property urlcopy.
     * @return Value of property urlcopy.
     */
    public String getUrlcopy() {
        return urlcopy;
    }

    /** Setter for property urlcopy.
     * @param urlcopy New value of property urlcopy.
     */
    public void setUrlcopy(String urlcopy) {
        this.urlcopy = urlcopy;
    }

    /** Getter for property gsiftpclinet.
     * @return Value of property gsiftpclinet.
     */
    public String getGsiftpclinet() {
        return gsiftpclinet;
    }

    /** Setter for property gsiftpclinet.
     * @param gsiftpclinet New value of property gsiftpclinet.
     */
    public void setGsiftpclinet(String gsiftpclinet) {
        this.gsiftpclinet = gsiftpclinet;
    }

    /** Getter for property gsissl.
     * @return Value of property gsissl.
     */
    public boolean isGsissl() {
        return gsissl;
    }

    /** Setter for property gsissl.
     * @param gsissl New value of property gsissl.
     */
    public void setGsissl(boolean gsissl) {
        this.gsissl = gsissl;
    }

    /** Getter for property debug.
     * @return Value of property debug.
     */
    public boolean isDebug() {
        return debug;
    }

    /** Setter for property debug.
     * @param debug New value of property debug.
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }


    /** Getter for property buffer_size.
     * @return Value of property buffer_size.
     */
    public int getBuffer_size() {
        return buffer_size;
    }

    /** Setter for property buffer_size.
     * @param buffer_size New value of property buffer_size.
     */
    public void setBuffer_size(int buffer_size) {
        this.buffer_size = buffer_size;
    }

    /** Getter for property tcp_buffer_size.
     * @return Value of property tcp_buffer_size.
     */
    public int getTcp_buffer_size() {
        return tcp_buffer_size;
    }

    /** Setter for property tcp_buffer_size.
     * @param tcp_buffer_size New value of property tcp_buffer_size.
     */
    public void setTcp_buffer_size(int tcp_buffer_size) {
        this.tcp_buffer_size = tcp_buffer_size;
    }

    /** Getter for property port.
     * @return Value of property port.
     */
    public int getPort() {
        return port;
    }

    /** Setter for property port.
     * @param port New value of property port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Getter for property authzCacheLifetime.
     * @return Value of property authzCacheLifetime.
     */
    public long getAuthzCacheLifetime() {
        return authzCacheLifetime;
    }

    /** Setter for property authzCacheLifetime.
     * @param authzCacheLifetime New value of property authzCacheLifetime.
     */
    public void setAuthzCacheLifetime(long authzCacheLifetime) {
        this.authzCacheLifetime = authzCacheLifetime;
    }

    /** Setter for property srm_root.
     * @param srm_root New value of property srm_root.
     */
    public void setSrm_root(String srm_root) {
        this.srm_root = srm_root;
    }

    /** Getter for property srm_root.
     * @return Value of property srm_root.
     */
    public String getSrm_root() {
        return srm_root;
    }

    /** Getter for property proxies_directory.
     * @return Value of property proxies_directory.
     */
    public String getProxies_directory() {
        return proxies_directory;
    }

    /** Setter for property proxies_directory.
     * @param proxies_directory New value of property proxies_directory.
     */
    public void setProxies_directory(String proxies_directory) {
        this.proxies_directory = proxies_directory;
    }

    /** Getter for property timeout.
     * @return Value of property timeout.
     */
    public int getTimeout() {
        return timeout;
    }

    /** Setter for property timeout.
     * @param timeout New value of property timeout.
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /** Getter for property timeout_script.
     * @return Value of property timeout_script.
     */
    public String getTimeout_script() {
        return timeout_script;
    }

    /** Setter for property timeout_script.
     * @param timeout_script New value of property timeout_script.
     */
    public void setTimeout_script(String timeout_script) {
        this.timeout_script = timeout_script;
    }

    /**
     * this method returns collection of the local srm hosts.
     * A host part of the srm url (surl) is used to determine if the surl
     * references file in this storage system.
     * In case of the copy operation, srm needs to be able to tell the
     * local surl from the remote one.
     * Also SRM needs to  refuse to perform operations on non local srm urls
     * This collection cosists of hosts that are cosidered local by this srm server.
     * This parameter has to be a collection because in case of the multihomed
     * or distributed server it may have more than one network name.
     *
     * @return set of local srmhosts.
     */
    public Set<String> getSrmHosts() {
        synchronized(localSrmHosts) {
            Set<String> srmhostsCopy = new HashSet<>(localSrmHosts);
            return srmhostsCopy;
        }
    }

    /**
     * This method adds values to the collection of the local srm hosts.
     * A host part of the srm url (surl) is used to determine if the surl
     * references file in this storage system.
     * In case of the copy operation, srm needs to be able to tell the
     * local surl from the remote one.
     * Also SRM needs to  refuse to perform operations on non local srm urls
     * This collection cosists of hosts that are cosidered local by this srm server.
     * This parameter has to be a collection because in case of the multihomed
     * or distributed server it may have more than one network name.
     *
     * @param srmhost additional value of srmhost.
     */
    public void addSrmHost(String srmhost) {
        synchronized(localSrmHosts) {
            localSrmHosts.add(srmhost);
        }
    }

    /**
     * Sets the set of local srm hosts. See addSrmHost for details.
     */
    public void setSrmHostsAsArray(String[] hosts) {
        synchronized(localSrmHosts) {
            localSrmHosts.clear();
            localSrmHosts.addAll(Arrays.asList(hosts));
        }
    }

    /** Getter for property authorization.
     * @return Value of property authorization.
     */
    public SRMAuthorization getAuthorization() {
        return authorization;
    }

    /** Setter for property authorization.
     * @param authorization New value of property authorization.
     */
    public void setAuthorization(SRMAuthorization authorization) {
        this.authorization = authorization;
    }

    private String timeToString(long value)
    {
        return (value == Long.MAX_VALUE) ? INFINITY : String.valueOf(value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SRM Configuration:");
        sb.append("\n\t\"defaultSpaceLifetime\"  request lifetime: ").append(this.defaultSpaceLifetime );
        sb.append("\n\t\"get\"  request lifetime: ").append(this.getLifetime );
        sb.append("\n\t\"bringOnline\"  request lifetime: ").append(this.bringOnlineLifetime );
        sb.append("\n\t\"put\"  request lifetime: ").append(this.putLifetime );
        sb.append("\n\t\"copy\" request lifetime: ").append(this.copyLifetime);
        sb.append("\n\tdebug=").append(this.debug);
        sb.append("\n\tgsissl=").append(this.gsissl);
        sb.append("\n\tgridftp buffer_size=").append(this.buffer_size);
        sb.append("\n\tgridftp tcp_buffer_size=").append(this.tcp_buffer_size);
        sb.append("\n\tgridftp parallel_streams=").append(this.parallel_streams);
        sb.append("\n\tgsiftpclinet=").append(this.gsiftpclinet);
        sb.append("\n\turlcopy=").append(this.urlcopy);
        sb.append("\n\tsrm_root=").append(this.srm_root);
        sb.append("\n\ttimeout_script=").append(this.timeout_script);
        sb.append("\n\turlcopy timeout in seconds=").append(this.timeout);
        sb.append("\n\tproxies directory=").append(this.proxies_directory);
        sb.append("\n\tport=").append(this.port);
        sb.append("\n\tsrmHost=").append(getSrmHost());
        sb.append("\n\tlocalSrmHosts=");
        for(String host:this.getSrmHosts()) {
            sb.append(host).append(", ");
        }
        sb.append("\n\tuseUrlcopyScript=").append(this.useUrlcopyScript);
        sb.append("\n\tuseGsiftpForSrmCopy=").append(this.useGsiftpForSrmCopy);
        sb.append("\n\tuseHttpForSrmCopy=").append(this.useHttpForSrmCopy);
        sb.append("\n\tuseDcapForSrmCopy=").append(this.useDcapForSrmCopy);
        sb.append("\n\tuseFtpForSrmCopy=").append(this.useFtpForSrmCopy);
        sb.append("\n\t\t *** GetRequests Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.getLifetime);
        sb.append("\n\t\t maximum number of retries = ").append(this.getMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.getRetryTimeout);
        sb.append("\n\t\t switch to async mode delay=").append(timeToString(this.getSwitchToAsynchronousModeDelay));

        sb.append("\n\t\t *** BringOnlineRequests Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.bringOnlineLifetime);
        sb.append("\n\t\t maximum number of retries = ").append(this.bringOnlineMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.bringOnlineRetryTimeout);
        sb.append("\n\t\t switch to async mode delay=").append(timeToString(this.bringOnlineSwitchToAsynchronousModeDelay));

        sb.append("\n\t\t *** LsRequests Parameters **");
        sb.append("\n\t\t maximum number of retries = ").append(this.lsMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.lsRetryTimeout);
        sb.append("\n\t\t switch to async mode delay=").append(timeToString(this.lsSwitchToAsynchronousModeDelay));

        sb.append("\n\t\t *** PutRequests Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.putLifetime);
        sb.append("\n\t\t maximum number of retries = ").append(this.putMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.putRetryTimeout);
        sb.append("\n\t\t switch to async mode delay=").append(timeToString(this.putSwitchToAsynchronousModeDelay));

        sb.append("\n\t\t *** ReserveSpaceRequests Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.reserveSpaceLifetime);
        sb.append("\n\t\t maximum number of retries = ").append(this.reserveSpaceMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.reserveSpaceRetryTimeout);

        sb.append("\n\t\t *** CopyRequests Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.copyLifetime);
        sb.append("\n\t\t maximum number of retries = ").append(this.copyMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.copyRetryTimeout);

        for (DatabaseParameters parameters: databaseParameters.values()) {
            sb.append(parameters);
        }

        sb.append("\n\tstorage_info_update_period=").append(this.storage_info_update_period);
        sb.append("\n\tqosPluginClass=").append(this.qosPluginClass);
        sb.append("\n\tqosConfigFile=").append(this.qosConfigFile);
        sb.append("\n\tclientDNSLookup=").append(this.clientDNSLookup);
        sb.append( "\n\tclientTransport=").append(clientTransport);
        return sb.toString();
    }

    /** Getter for property parallel_streams.
     * @return Value of property parallel_streams.
     */
    public int getParallel_streams() {
        return parallel_streams;
    }

    /** Setter for property parallel_streams.
     * @param parallel_streams New value of property parallel_streams.
     */
    public void setParallel_streams(int parallel_streams) {
        this.parallel_streams = parallel_streams;
    }


    /** Getter for property getLifetime.
     * @return Value of property getLifetime.
     *
     */
    public long getGetLifetime() {
        return getLifetime;
    }

    /** Setter for property getLifetime.
     * @param getLifetime New value of property getLifetime.
     *
     */
    public void setGetLifetime(long getLifetime) {
        this.getLifetime = getLifetime;
    }

    /** Getter for property putLifetime.
     * @return Value of property putLifetime.
     *
     */
    public long getPutLifetime() {
        return putLifetime;
    }

    /** Setter for property putLifetime.
     * @param putLifetime New value of property putLifetime.
     *
     */
    public void setPutLifetime(long putLifetime) {
        this.putLifetime = putLifetime;
    }

    /** Getter for property copyLifetime.
     * @return Value of property copyLifetime.
     *
     */
    public long getCopyLifetime() {
        return copyLifetime;
    }

    /** Setter for property copyLifetime.
     * @param copyLifetime New value of property copyLifetime.
     *
     */
    public void setCopyLifetime(long copyLifetime) {
        this.copyLifetime = copyLifetime;
    }

    /** Getter for property useUrlcopyScript.
     * @return Value of property useUrlcopyScript.
     *
     */
    public boolean isUseUrlcopyScript() {
        return useUrlcopyScript;
    }

    /** Setter for property useUrlcopyScript.
     * @param useUrlcopyScript New value of property useUrlcopyScript.
     *
     */
    public void setUseUrlcopyScript(boolean useUrlcopyScript) {
        this.useUrlcopyScript = useUrlcopyScript;
    }

    /** Getter for property useDcapForSrmCopy.
     * @return Value of property useDcapForSrmCopy.
     *
     */
    public boolean isUseDcapForSrmCopy() {
        return useDcapForSrmCopy;
    }

    /** Setter for property useDcapForSrmCopy.
     * @param useDcapForSrmCopy New value of property useDcapForSrmCopy.
     *
     */
    public void setUseDcapForSrmCopy(boolean useDcapForSrmCopy) {
        this.useDcapForSrmCopy = useDcapForSrmCopy;
    }

    /** Getter for property useGsiftpForSrmCopy.
     * @return Value of property useGsiftpForSrmCopy.
     *
     */
    public boolean isUseGsiftpForSrmCopy() {
        return useGsiftpForSrmCopy;
    }

    /** Setter for property useGsiftpForSrmCopy.
     * @param useGsiftpForSrmCopy New value of property useGsiftpForSrmCopy.
     *
     */
    public void setUseGsiftpForSrmCopy(boolean useGsiftpForSrmCopy) {
        this.useGsiftpForSrmCopy = useGsiftpForSrmCopy;
    }

    /** Getter for property useHttpForSrmCopy.
     * @return Value of property useHttpForSrmCopy.
     *
     */
    public boolean isUseHttpForSrmCopy() {
        return useHttpForSrmCopy;
    }

    /** Setter for property useHttpForSrmCopy.
     * @param useHttpForSrmCopy New value of property useHttpForSrmCopy.
     *
     */
    public void setUseHttpForSrmCopy(boolean useHttpForSrmCopy) {
        this.useHttpForSrmCopy = useHttpForSrmCopy;
    }

    /** Getter for property useFtpForSrmCopy.
     * @return Value of property useFtpForSrmCopy.
     *
     */
    public boolean isUseFtpForSrmCopy() {
        return useFtpForSrmCopy;
    }

    /** Setter for property useFtpForSrmCopy.
     * @param useFtpForSrmCopy New value of property useFtpForSrmCopy.
     *
     */
    public void setUseFtpForSrmCopy(boolean useFtpForSrmCopy) {
        this.useFtpForSrmCopy = useFtpForSrmCopy;
    }

    /** Getter for property recursiveDirectoryCreation.
     * @return Value of property recursiveDirectoryCreation.
     *
     */
    public boolean isRecursiveDirectoryCreation() {
        return recursiveDirectoryCreation;
    }

    /** Setter for property recursiveDirectoryCreation.
     * @param recursiveDirectoryCreation New value of property recursiveDirectoryCreation.
     *
     */
    public void setRecursiveDirectoryCreation(boolean recursiveDirectoryCreation) {
        this.recursiveDirectoryCreation = recursiveDirectoryCreation;
    }

    /** Getter for property advisoryDelete.
     * @return Value of property advisoryDelete.
     *
     */
    public boolean isAdvisoryDelete() {
        return advisoryDelete;
    }

    /** Setter for property advisoryDelete.
     * @param advisoryDelete New value of property advisoryDelete.
     *
     */
    public void setAdvisoryDelete(boolean advisoryDelete) {
        this.advisoryDelete = advisoryDelete;
    }

    public void setDataSource(DataSource ds) {
        this.dataSource = ds;
    }

    public DataSource getDataSource()
    {
        return dataSource;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager)
    {
        this.transactionManager = transactionManager;
    }

    public PlatformTransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    /**
     * Getter for property nextRequestIdStorageTable.
     * @return Value of property nextRequestIdStorageTable.
     */
    public String getNextRequestIdStorageTable() {
        return nextRequestIdStorageTable;
    }

    /**
     * Setter for property nextRequestIdStorageTable.
     * @param nextRequestIdStorageTable New value of property nextRequestIdStorageTable.
     */
    public void setNextRequestIdStorageTable(String nextRequestIdStorageTable) {
        this.nextRequestIdStorageTable = nextRequestIdStorageTable;
    }

    /**
     * Getter for property getMaxNumOfRetries.
     * @return Value of property getMaxNumOfRetries.
     */
    public int getGetMaxNumOfRetries() {
        return getMaxNumOfRetries;
    }

    /**
     * Setter for property getMaxNumOfRetries.
     * @param getMaxNumOfRetries New value of property getMaxNumOfRetries.
     */
    public void setGetMaxNumOfRetries(int getMaxNumOfRetries) {
        this.getMaxNumOfRetries = getMaxNumOfRetries;
    }

    /**
     * Getter for property getRetryTimeout.
     * @return Value of property getRetryTimeout.
     */
    public long getGetRetryTimeout() {
        return getRetryTimeout;
    }

    /**
     * Setter for property getRetryTimeout.
     * @param getRetryTimeout New value of property getRetryTimeout.
     */
    public void setGetRetryTimeout(long getRetryTimeout) {
        this.getRetryTimeout = getRetryTimeout;
    }

    /**
     * Getter for property putMaxNumOfRetries.
     * @return Value of property putMaxNumOfRetries.
     */
    public int getPutMaxNumOfRetries() {
        return putMaxNumOfRetries;
    }

    /**
     * Setter for property putMaxNumOfRetries.
     * @param putMaxNumOfRetries New value of property putMaxNumOfRetries.
     */
    public void setPutMaxNumOfRetries(int putMaxNumOfRetries) {
        this.putMaxNumOfRetries = putMaxNumOfRetries;
    }

    /**
     * Getter for property putRetryTimeout.
     * @return Value of property putRetryTimeout.
     */
    public long getPutRetryTimeout() {
        return putRetryTimeout;
    }

    /**
     * Setter for property putRetryTimeout.
     * @param putRetryTimeout New value of property putRetryTimeout.
     */
    public void setPutRetryTimeout(long putRetryTimeout) {
        this.putRetryTimeout = putRetryTimeout;
    }

    /**
     * Getter for property copyMaxNumOfRetries.
     * @return Value of property copyMaxNumOfRetries.
     */
    public int getCopyMaxNumOfRetries() {
        return copyMaxNumOfRetries;
    }

    /**
     * Setter for property copyMaxNumOfRetries.
     * @param copyMaxNumOfRetries New value of property copyMaxNumOfRetries.
     */
    public void setCopyMaxNumOfRetries(int copyMaxNumOfRetries) {
        this.copyMaxNumOfRetries = copyMaxNumOfRetries;
    }

    /**
     * Getter for property copyRetryTimeout.
     * @return Value of property copyRetryTimeout.
     */
    public long getCopyRetryTimeout() {
        return copyRetryTimeout;
    }

    /**
     * Setter for property copyRetryTimeout.
     * @param copyRetryTimeout New value of property copyRetryTimeout.
     */
    public void setCopyRetryTimeout(long copyRetryTimeout) {
        this.copyRetryTimeout = copyRetryTimeout;
    }

    /**
     * Getter for property reserveSpaceMaxNumOfRetries.
     * @return Value of property reserveSpaceMaxNumOfRetries.
     */
    public int getReserveSpaceMaxNumOfRetries() {
        return reserveSpaceMaxNumOfRetries;
    }

    /**
     * Setter for property reserveSpaceMaxNumOfRetries.
     * @param reserveSpaceMaxNumOfRetries New value of property reserveSpaceMaxNumOfRetries.
     */
    public void setReserveSpaceMaxNumOfRetries(int reserveSpaceMaxNumOfRetries) {
        this.reserveSpaceMaxNumOfRetries = reserveSpaceMaxNumOfRetries;
    }

    /**
     * Getter for property reserveSpaceRetryTimeout.
     * @return Value of property reserveSpaceRetryTimeout.
     */
    public long getReserveSpaceRetryTimeout() {
        return reserveSpaceRetryTimeout;
    }

    /**
     * Setter for property reserveSpaceRetryTimeout.
     * @param reserveSpaceRetryTimeout New value of property reserveSpaceRetryTimeout.
     */
    public void setReserveSpaceRetryTimeout(long reserveSpaceRetryTimeout) {
        this.reserveSpaceRetryTimeout = reserveSpaceRetryTimeout;
    }

    /**
     * Getter for property storage_info_update_period.
     * @return Value of property storage_info_update_period.
     */
    public long getStorage_info_update_period() {
        return storage_info_update_period;
    }

    /**
     * Setter for property storage_info_update_period.
     * @param storage_info_update_period New value of property storage_info_update_period.
     */
    public void setStorage_info_update_period(long storage_info_update_period) {
        this.storage_info_update_period = storage_info_update_period;
    }


    public String getQosPluginClass() {
        return qosPluginClass;
    }
    public void setQosPluginClass(String qosPluginClass) {
        this.qosPluginClass = Strings.emptyToNull(qosPluginClass);
    }
    public String getQosConfigFile() {
    	return qosConfigFile;
    }
    public void setQosConfigFile(String qosConfigFile) {
    	this.qosConfigFile = Strings.emptyToNull(qosConfigFile);
    }

    public long getDefaultSpaceLifetime() {
        return defaultSpaceLifetime;
    }

    public void setDefaultSpaceLifetime(long defaultSpaceLifetime) {
        this.defaultSpaceLifetime = defaultSpaceLifetime;
    }

    public void setGetPriorityPolicyPlugin(String txt) {
        getPriorityPolicyPlugin=txt;
    }

    public String getGetPriorityPolicyPlugin() {
        return getPriorityPolicyPlugin;
    }

    public void setPutPriorityPolicyPlugin(String txt) {
        putPriorityPolicyPlugin=txt;
    }

    public String getPutPriorityPolicyPlugin() {
        return putPriorityPolicyPlugin;
    }

    public void setCopyPriorityPolicyPlugin(String txt) {
        putPriorityPolicyPlugin=txt;
    }

    public String getCopyPriorityPolicyPlugin() {
        return putPriorityPolicyPlugin;
    }

    public void setReserveSpacePriorityPolicyPlugin(String txt) {
        putPriorityPolicyPlugin=txt;
    }

    public String getReserveSpacePriorityPolicyPlugin() {
        return putPriorityPolicyPlugin;
    }

     public Integer getJdbcExecutionThreadNum() {
        return jdbcExecutionThreadNum;
    }

    public void setJdbcExecutionThreadNum(Integer jdbcExecutionThreadNum) {
        this.jdbcExecutionThreadNum = jdbcExecutionThreadNum;
    }

     public Integer getMaxQueuedJdbcTasksNum() {
        return maxQueuedJdbcTasksNum;
    }

    public void setMaxQueuedJdbcTasksNum(Integer maxQueuedJdbcTasksNum) {
        this.maxQueuedJdbcTasksNum = maxQueuedJdbcTasksNum;
    }

    public String getCredentialsDirectory() {
        return credentialsDirectory;
    }

    public void setCredentialsDirectory(String credentialsDirectory) {
        this.credentialsDirectory = credentialsDirectory;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public int getSizeOfSingleRemoveBatch() {
	    return sizeOfSingleRemoveBatch;
    }

    public void setSizeOfSingleRemoveBatch(int size) {
	    sizeOfSingleRemoveBatch=size;
    }

    public long getGetSwitchToAsynchronousModeDelay()
    {
        return getSwitchToAsynchronousModeDelay;
    }

    public void setGetSwitchToAsynchronousModeDelay(long time)
    {
        getSwitchToAsynchronousModeDelay = time;
    }

    public long getPutSwitchToAsynchronousModeDelay()
    {
        return putSwitchToAsynchronousModeDelay;
    }

    public void setPutSwitchToAsynchronousModeDelay(long time)
    {
        putSwitchToAsynchronousModeDelay = time;
    }

    public long getLsSwitchToAsynchronousModeDelay()
    {
        return lsSwitchToAsynchronousModeDelay;
    }

    public void setLsSwitchToAsynchronousModeDelay(long time)
    {
        lsSwitchToAsynchronousModeDelay = time;
    }

    public long getBringOnlineSwitchToAsynchronousModeDelay()
    {
        return bringOnlineSwitchToAsynchronousModeDelay;
    }

    public void setBringOnlineSwitchToAsynchronousModeDelay(long time)
    {
        bringOnlineSwitchToAsynchronousModeDelay = time;
    }

    public int getMaxNumberOfLsLevels() {
	    return maxNumberOfLsLevels;
    }

    public void setMaxNumberOfLsLevels(int max_ls_levels) {
	    maxNumberOfLsLevels=max_ls_levels;
    }

    public int getMaxNumberOfLsEntries() {
	    return maxNumberOfLsEntries;
    }

    public void setMaxNumberOfLsEntries(int max_ls_entries) {
	   maxNumberOfLsEntries=max_ls_entries;
    }

    public boolean isOverwrite_by_default() {
        return overwrite_by_default;
    }

    public void setOverwrite_by_default(boolean overwrite_by_default) {
        this.overwrite_by_default = overwrite_by_default;
    }


    public SRMUserPersistenceManager getSrmUserPersistenceManager() {
        return srmUserPersistenceManager;
    }

    public void setSrmUserPersistenceManager(SRMUserPersistenceManager srmUserPersistenceManager) {
        this.srmUserPersistenceManager = srmUserPersistenceManager;
    }

    public int getBringOnlineMaxNumOfRetries() {
        return bringOnlineMaxNumOfRetries;
    }

    public void setBringOnlineMaxNumOfRetries(int bringOnlineMaxNumOfRetries) {
        this.bringOnlineMaxNumOfRetries = bringOnlineMaxNumOfRetries;
    }

    public long getBringOnlineRetryTimeout() {
        return bringOnlineRetryTimeout;
    }

    public void setBringOnlineRetryTimeout(long bringOnlineRetryTimeout) {
        this.bringOnlineRetryTimeout = bringOnlineRetryTimeout;
    }

    public long getBringOnlineLifetime() {
        return bringOnlineLifetime;
    }

    public void setBringOnlineLifetime(long bringOnlineLifetime) {
        this.bringOnlineLifetime = bringOnlineLifetime;
    }

    public String getBringOnlinePriorityPolicyPlugin() {
        return bringOnlinePriorityPolicyPlugin;
    }

    public void setBringOnlinePriorityPolicyPlugin(String bringOnlinePriorityPolicyPlugin) {
        this.bringOnlinePriorityPolicyPlugin = bringOnlinePriorityPolicyPlugin;
    }

    public int getLsMaxNumOfRetries() {
        return lsMaxNumOfRetries;
    }

    public void setLsMaxNumOfRetries(int lsMaxNumOfRetries) {
        this.lsMaxNumOfRetries = lsMaxNumOfRetries;
    }

    public long getLsRetryTimeout() {
        return lsRetryTimeout;
    }

    public void setLsRetryTimeout(long lsRetryTimeout) {
        this.lsRetryTimeout = lsRetryTimeout;
    }

    public String getLsPriorityPolicyPlugin() {
        return lsPriorityPolicyPlugin;
    }

    public void setLsPriorityPolicyPlugin(String lsPriorityPolicyPlugin) {
        this.lsPriorityPolicyPlugin = lsPriorityPolicyPlugin;
    }

    /**
     * @return the clientDNSLookup
     */
    public boolean isClientDNSLookup() {
        return clientDNSLookup;
    }

    /**
     * @param clientDNSLookup the clientDNSLookup to set
     */
    public void setClientDNSLookup(boolean clientDNSLookup) {
        this.clientDNSLookup = clientDNSLookup;
    }

    /**
     * @return the rrdDirectory
     */
    public String getCounterRrdDirectory() {
        return counterRrdDirectory;
    }

    /**
     * @param rrdDirectory the rrdDirectory to set
     */
    public void setCounterRrdDirectory(String rrdDirectory) {
        this.counterRrdDirectory = rrdDirectory;
    }

    /**
     * @return the gaugeRrdDirectory
     */
    public String getGaugeRrdDirectory() {
        return gaugeRrdDirectory;
    }

    /**
     * @param gaugeRrdDirectory the gaugeRrdDirectory to set
     */
    public void setGaugeRrdDirectory(String gaugeRrdDirectory) {
        this.gaugeRrdDirectory = gaugeRrdDirectory;
    }

    /**
     * @return the srmHost
     */
    public String getSrmHost() {
        return srmHost;
    }

    /**
     * @param srmHost the srmHost to set
     */
    public void setSrmHost(String srmHost) {
        this.srmHost = srmHost;
    }

    public Transport getClientTransport() {
        return Transport.transportFor(clientTransport);
    }

    public void setClientTransport(Transport transport) {
        clientTransport = transport.name();
    }

    public void setClientTransportByName(String name) {
        clientTransport = Transport.transportFor(name).name();
    }

    public DatabaseParameters getDatabaseParametersForList() {
        return databaseParameters.get(LS_PARAMETERS);
    }

    public DatabaseParameters getDatabaseParametersForGet() {
        return databaseParameters.get(GET_PARAMETERS);
    }

    public DatabaseParameters getDatabaseParametersForPut() {
        return databaseParameters.get(PUT_PARAMETERS);
    }

    public DatabaseParameters getDatabaseParametersForBringOnline() {
        return databaseParameters.get(BRINGONLINE_PARAMETERS);
    }

    public DatabaseParameters getDatabaseParametersForCopy() {
        return databaseParameters.get(COPY_PARAMETERS);
    }

    public DatabaseParameters getDatabaseParametersForReserve() {
        return databaseParameters.get(RESERVE_PARAMETERS);
    }

    public DatabaseParameters getDatabaseParameters(String name) {
        return databaseParameters.get(name);
    }

    public class DatabaseParameters
    {
        private final String name;
        private boolean databaseEnabled = true;
        private boolean requestHistoryDatabaseEnabled = false;
        private boolean storeCompletedRequestsOnly = false;
        private int keepRequestHistoryPeriod = 30;
        private long expiredRequestRemovalPeriod = 3600;
        private boolean cleanPendingRequestsOnRestart = false;

        public DatabaseParameters(String name)
        {
            this.name = name;
        }

        public boolean isDatabaseEnabled() {
            return databaseEnabled;
        }

        public void setDatabaseEnabled(boolean value) {
            databaseEnabled = value;
        }

        public boolean getStoreCompletedRequestsOnly() {
            return storeCompletedRequestsOnly;
        }

        public void setStoreCompletedRequestsOnly(boolean value) {
            storeCompletedRequestsOnly = value;
        }

        public boolean isRequestHistoryDatabaseEnabled() {
            return requestHistoryDatabaseEnabled;
        }

        public void setRequestHistoryDatabaseEnabled(boolean value) {
            requestHistoryDatabaseEnabled = value;
        }

        public int getKeepRequestHistoryPeriod() {
            return keepRequestHistoryPeriod;
        }

        public void setKeepRequestHistoryPeriod(int value) {
            keepRequestHistoryPeriod = value;
        }

        public long getExpiredRequestRemovalPeriod() {
            return expiredRequestRemovalPeriod;
        }

        public void setExpiredRequestRemovalPeriod(long value) {
            expiredRequestRemovalPeriod = value;
        }

        public boolean isCleanPendingRequestsOnRestart() {
            return cleanPendingRequestsOnRestart;
        }

        public void setCleanPendingRequestsOnRestart(boolean value) {
            cleanPendingRequestsOnRestart = value;
        }

        public SRMUserPersistenceManager getSrmUserPersistenceManager() {
            return Configuration.this.getSrmUserPersistenceManager();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n\t\t*** ").append(name).append(" Store Parameters ***");
            sb.append("\n\t\tdatabaseEnabled=").append(databaseEnabled);
            sb.append("\n\t\tstoreCompletedRequestsOnly=").append(storeCompletedRequestsOnly);
            sb.append("\n\t\trequestHistoryDatabaseEnabled=").append(requestHistoryDatabaseEnabled);
            sb.append("\n\t\tcleanPendingRequestsOnRestart=").append(cleanPendingRequestsOnRestart);
            sb.append("\n\t\tkeepRequestHistoryPeriod=").append(keepRequestHistoryPeriod).append(" days");
            sb.append("\n\t\texpiredRequestRemovalPeriod=").append(expiredRequestRemovalPeriod).append(" seconds");
            return sb.toString();
        }

        public DataSource getDataSource()
        {
            return Configuration.this.getDataSource();
        }

        public PlatformTransactionManager getTransactionManager()
        {
            return Configuration.this.getTransactionManager();
        }
    }
}
