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
import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMUserPersistenceManager;
import org.dcache.srm.client.Transport;


/**
 *
 * @author  timur
 */
public class Configuration {

    private static final String XML_LABEL_TRANSPORT_CLIENT = "client_transport";

    private static final String INFINITY = "infinity";

    public static final String LS_PARAMETERS = "ls";
    public static final String PUT_PARAMETERS = "put";
    public static final String GET_PARAMETERS = "get";
    public static final String COPY_PARAMETERS = "copy";
    public static final String BRINGONLINE_PARAMETERS = "bringonline";
    public static final String RESERVE_PARAMETERS = "reserve";

    private boolean debug = false;

    private String urlcopy="../scripts/urlcopy.sh";

    private String gsiftpclinet = "globus-url-copy";

    private boolean gsissl = true;


    private int buffer_size=2048;
    private int tcp_buffer_size;
    private int parallel_streams=10;

    private int port=8443;
    private long authzCacheLifetime = 180;
    private String srm_root="/";
    private String proxies_directory = "../proxies";
    private int timeout=60*60; //one hour
    private String timeout_script="../scripts/timeout.sh";
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
    private AbstractStorageElement storage;
    private SRMAuthorization authorization;

    // scheduler parameters

    private int getReqTQueueSize=1000;
    private int getThreadPoolSize=30;
    private int getMaxWaitingRequests=1000;
    private int getReadyQueueSize=1000;
    private int getMaxReadyJobs=60;
    private int getMaxNumOfRetries=10;
    private long getRetryTimeout=60000;
    private int getMaxRunningBySameOwner=10;
    private long getSwitchToAsynchronousModeDelay = 0;

    private int lsReqTQueueSize=1000;
    private int lsThreadPoolSize=30;
    private int lsMaxWaitingRequests=1000;
    private int lsReadyQueueSize=1000;
    private int lsMaxReadyJobs=60;
    private int lsMaxNumOfRetries=10;
    private long lsRetryTimeout=60000;
    private int lsMaxRunningBySameOwner=10;
    private long lsSwitchToAsynchronousModeDelay = 0;

    private int bringOnlineReqTQueueSize=1000;
    private int bringOnlineThreadPoolSize=30;
    private int bringOnlineMaxWaitingRequests=1000;
    private int bringOnlineReadyQueueSize=1000;
    private int bringOnlineMaxReadyJobs=60;
    private int bringOnlineMaxNumOfRetries=10;
    private long bringOnlineRetryTimeout=60000;
    private int bringOnlineMaxRunningBySameOwner=10;
    private long bringOnlineSwitchToAsynchronousModeDelay = 0;

    private int putReqTQueueSize=1000;
    private int putThreadPoolSize=30;
    private int putMaxWaitingRequests=1000;
    private int putReadyQueueSize=1000;
    private int putMaxReadyJobs=60;
    private int putMaxNumOfRetries=10;
    private long putRetryTimeout=60000;
    private int putMaxRunningBySameOwner=10;
    private long putSwitchToAsynchronousModeDelay = 0;

    private int reserveSpaceReqTQueueSize=1000;
    private int reserveSpaceThreadPoolSize=30;
    private int reserveSpaceMaxWaitingRequests=1000;
    private int reserveSpaceReadyQueueSize=1000;
    private int reserveSpaceMaxReadyJobs=60;
    private int reserveSpaceMaxNumOfRetries=10;
    private long reserveSpaceRetryTimeout=60000;
    private int reserveSpaceMaxRunningBySameOwner=10;

    private int copyReqTQueueSize=1000;
    private int copyThreadPoolSize=30;
    private int copyMaxWaitingRequests=1000;
    private int copyMaxNumOfRetries=10;
    private long copyRetryTimeout=60000;
    private int copyMaxRunningBySameOwner=10;


    private long getLifetime = 24*60*60*1000;
    private long bringOnlineLifetime = 24*60*60*1000;
    private long putLifetime = 24*60*60*1000;
    private long copyLifetime = 24*60*60*1000;
    private long reserveSpaceLifetime = 24*60*60*1000;
    private long defaultSpaceLifetime = 24*60*60*1000;

    private boolean useUrlcopyScript=false;
    private boolean useDcapForSrmCopy=false;
    private boolean useGsiftpForSrmCopy=true;
    private boolean useHttpForSrmCopy=true;
    private boolean useFtpForSrmCopy=true;
    private boolean recursiveDirectoryCreation=false;
    private boolean advisoryDelete=false;
    private String nextRequestIdStorageTable = "srmnextrequestid";
    private boolean reserve_space_implicitely;
    private boolean space_reservation_strict;
    private long storage_info_update_period = TimeUnit.SECONDS.toMillis(30);
    private String qosPluginClass = null;
    private String qosConfigFile = null;
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
    private String clientTransport = Transport.GSI.name();
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

    public Configuration(String configuration_file) throws Exception {
        if (configuration_file != null && !configuration_file.isEmpty()) {
            read(configuration_file);
        }
    }


    public final void read(String file) throws Exception {
        DocumentBuilderFactory factory =
                DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(file);
        Node root =document.getFirstChild();
        for(;root != null && !"srm-configuration".equals(root.getNodeName());
        root = document.getNextSibling()) {
        }
        if(root == null) {
            System.err.println(" error, root element \"srm-configuration\" is not found");
            throw new IOException();
        }


        if(root != null && root.getNodeName().equals("srm-configuration")) {

            Node node = root.getFirstChild();
            for(;node != null; node = node.getNextSibling()) {
                if(node.getNodeType()!= Node.ELEMENT_NODE) {
                    continue;
                }

                Node child = node.getFirstChild();
                for(;child != null; child = node.getNextSibling()) {
                    if(child.getNodeType() == Node.TEXT_NODE) {
                        break;
                    }
                }
                if(child == null) {
                    continue;
                }
                Text t  = (Text)child;
                String node_name = node.getNodeName();
                String text_value = t.getData().trim();
                if(text_value != null && text_value.equalsIgnoreCase("null")) {
                    text_value = null;
                }
                set(node_name.trim(), text_value);
            }
        }
        synchronized(localSrmHosts) {
            try {
                localSrmHosts.add(
                        InetAddress.getLocalHost().
                        getCanonicalHostName());
            } catch(IOException ioe) {
                localSrmHosts.add("localhost");
            }
        }

    }

    protected static void put(Document document,Node root,String elem_name,String value, String comment_str) {
        //System.out.println("put elem_name="+elem_name+" value="+value+" comment="+comment_str);
        Text t = document.createTextNode("\n\n\t");
        root.appendChild(t);
        Comment comment = document.createComment(comment_str);
        root.appendChild(comment);
        t = document.createTextNode("\n\t");
        root.appendChild(t);
        Element element = document.createElement(elem_name);
        t = document.createTextNode(" "+value+" ");
        element.appendChild(t);
        root.appendChild(element);
    }

    public void write(String file) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.newDocument();
        //System.out.println("document is instanceof "+document.getClass().getName());
        Element root = document.createElement("srm-configuration");
        write(document, root);
        Text t = document.createTextNode("\n");
        root.appendChild(t);
        document.appendChild(root);

        TransformerFactory tFactory = TransformerFactory.newInstance();
        Transformer transformer = tFactory.newTransformer();
        DOMSource source = new DOMSource(document);
        StreamResult result = new StreamResult(new FileWriter(file));
        transformer.transform(source, result);
    }

    protected void write(Document document, Element root) {
        put(document,root,"debug",Boolean.toString(debug)," true or false");
        put(document,root,"urlcopy",urlcopy," path to the urlcopy script ");
        put(document,root,"gsiftpclient",gsiftpclinet," \"globus-url-copy\" or \"kftp\"");
        put(document,root,"gsissl",Boolean.toString(gsissl),"true if use http over gsi over ssl for SOAP invocations \n\t"+
                "or false to use plain http (no authentication or encryption)");
        put(document,root,"buffer_size",Integer.toString(buffer_size),
                "nonnegative integer, 2048 by default");
        put(document,root,"tcp_buffer_size",Integer.toString(tcp_buffer_size),
                "integer, 0 by default (which means do not set tcp_buffer_size at all)");
        put(document,root,"port",Integer.toString(port),
                "port on which to publish the srm service");
        put(document,root,"srmAuthzCacheLifetime", Long.toString(authzCacheLifetime),
                "time in seconds to cache authorizations ");
        put(document,root,"srm_root", srm_root,
                "root of the srm within the file system, nothing outside the root is accessible to the users");
        put(document,root,"proxies_directory", proxies_directory,
                "directory where deligated credentials will be temporarily stored, if external client is to be utilized");
        put(document,root,"timeout",Integer.toString(timeout),
                "timeout in seconds, how long to wait for the completeon of the transfer via external client, should the external client be used for the MSS to MSS transfers");
        put(document,root,"timeout_script",timeout_script ,
                "location of the timeout script");
        put(document,root,"getReqTQueueSize",Integer.toString(getReqTQueueSize),
                "getReqTQueueSize");
        put(document,root,"getThreadPoolSize",Integer.toString(getThreadPoolSize),
                "getThreadPoolSize");
        put(document,root,"getMaxWaitingRequests",Integer.toString(getMaxWaitingRequests),
                "getMaxWaitingRequests");
        put(document,root,"getReadyQueueSize",Integer.toString(getReadyQueueSize),
                "getReadyQueueSize");
        put(document,root,"getMaxReadyJobs",Integer.toString(getMaxReadyJobs),
                "getMaxReadyJobs");
        put(document,root,"getMaxNumOfRetries",Integer.toString(getMaxNumOfRetries),
                "Maximum Number Of Retries for get file request");
        put(document,root,"getRetryTimeout",Long.toString(getRetryTimeout),
                "get request Retry Timeout in milliseconds");

        put(document,root,"getMaxRunningBySameOwner",Integer.toString(getMaxRunningBySameOwner),
                "getMaxRunningBySameOwner");


        put(document,root,"bringOnlineReqTQueueSize",Integer.toString(bringOnlineReqTQueueSize),
                "bringOnlineReqTQueueSize");
        put(document,root,"bringOnlineThreadPoolSize",Integer.toString(bringOnlineThreadPoolSize),
                "bringOnlineThreadPoolSize");
        put(document,root,"bringOnlineMaxWaitingRequests",Integer.toString(bringOnlineMaxWaitingRequests),
                "bringOnlineMaxWaitingRequests");
        put(document,root,"bringOnlineReadyQueueSize",Integer.toString(bringOnlineReadyQueueSize),
                "bringOnlineReadyQueueSize");
        put(document,root,"bringOnlineMaxReadyJobs",Integer.toString(bringOnlineMaxReadyJobs),
                "bringOnlineMaxReadyJobs");
        put(document,root,"bringOnlineMaxNumOfRetries",Integer.toString(bringOnlineMaxNumOfRetries),
                "Maximum Number Of Retries for bringOnline file request");
        put(document,root,"bringOnlineRetryTimeout",Long.toString(bringOnlineRetryTimeout),
                "bringOnline request Retry Timeout in milliseconds");

        put(document,root,"bringOnlineMaxRunningBySameOwner",Integer.toString(bringOnlineMaxRunningBySameOwner),
                "bringOnlineMaxRunningBySameOwner");

        put(document,root,"lsReqTQueueSize",Integer.toString(lsReqTQueueSize),
                "lsReqTQueueSize");
        put(document,root,"lsThreadPoolSize",Integer.toString(lsThreadPoolSize),
                "lsThreadPoolSize");
        put(document,root,"lsMaxWaitingRequests",Integer.toString(lsMaxWaitingRequests),
                "lsMaxWaitingRequests");
        put(document,root,"lsReadyQueueSize",Integer.toString(lsReadyQueueSize),
                "lsReadyQueueSize");
        put(document,root,"lsMaxReadyJobs",Integer.toString(lsMaxReadyJobs),
                "lsMaxReadyJobs");
        put(document,root,"lsMaxNumOfRetries",Integer.toString(lsMaxNumOfRetries),
                "Maximum Number Of Retries for ls file request");
        put(document,root,"lsRetryTimeout",Long.toString(lsRetryTimeout),
                "ls request Retry Timeout in milliseconds");

        put(document,root,"lsMaxRunningBySameOwner",Integer.toString(lsMaxRunningBySameOwner),
                "lsMaxRunningBySameOwner");


        put(document,root,"putReqTQueueSize",Integer.toString(putReqTQueueSize),
                "putReqTQueueSize");
        put(document,root,"putThreadPoolSize",Integer.toString(putThreadPoolSize),
                "putThreadPoolSize");
        put(document,root,"putMaxWaitingRequests",Integer.toString(putMaxWaitingRequests),
                "putMaxWaitingRequests");
        put(document,root,"putReadyQueueSize",Integer.toString(putReadyQueueSize),
                "putReadyQueueSize");
        put(document,root,"putMaxReadyJobs",Integer.toString(putMaxReadyJobs),
                "putMaxReadyJobs");
        put(document,root,"putMaxNumOfRetries",Integer.toString(putMaxNumOfRetries),
                "Maximum Number Of Retries for put file request");
        put(document,root,"putRetryTimeout",Long.toString(putRetryTimeout),
                "put request Retry Timeout in milliseconds");

        put(document,root,"putMaxRunningBySameOwner",Integer.toString(putMaxRunningBySameOwner),
                "putMaxRunningBySameOwner");


        put(document,root,"reserveSpaceReqTQueueSize",Integer.toString(reserveSpaceReqTQueueSize),
                "reserveSpaceReqTQueueSize");
        put(document,root,"reserveSpaceThreadPoolSize",Integer.toString(reserveSpaceThreadPoolSize),
                "reserveSpaceThreadPoolSize");
        put(document,root,"reserveSpaceMaxWaitingRequests",Integer.toString(reserveSpaceMaxWaitingRequests),
                "reserveSpaceMaxWaitingRequests");
        put(document,root,"reserveSpaceReadyQueueSize",Integer.toString(reserveSpaceReadyQueueSize),
                "reserveSpaceReadyQueueSize");
        put(document,root,"reserveSpaceMaxReadyJobs",Integer.toString(reserveSpaceMaxReadyJobs),
                "reserveSpaceMaxReadyJobs");
        put(document,root,"reserveSpaceMaxNumOfRetries",Integer.toString(reserveSpaceMaxNumOfRetries),
                "Maximum Number Of Retries for reserveSpace file request");
        put(document,root,"reserveSpaceRetryTimeout",Long.toString(reserveSpaceRetryTimeout),
                "reserveSpace request Retry Timeout in milliseconds");

        put(document,root,"reserveSpaceMaxRunningBySameOwner",Integer.toString(reserveSpaceMaxRunningBySameOwner),
                "reserveSpaceMaxRunningBySameOwner");


        put(document,root,"copyReqTQueueSize",Integer.toString(copyReqTQueueSize),
                "copyReqTQueueSize");
        put(document,root,"copyThreadPoolSize",Integer.toString(copyThreadPoolSize),
                "copyThreadPoolSize");
        put(document,root,"copyMaxWaitingRequests",Integer.toString(copyMaxWaitingRequests),
                "copyMaxWaitingRequests");
        put(document,root,"copyMaxNumOfRetries",Integer.toString(copyMaxNumOfRetries),
                "Maximum Number Of Retries for copy file request");
        put(document,root,"copyRetryTimeout",Long.toString(copyRetryTimeout),
                "copy request Retry Timeout in milliseconds");

        put(document,root,"copyMaxRunningBySameOwner",Integer.toString(copyMaxRunningBySameOwner),
                "copyMaxRunningBySameOwner");


        put(document,root,"getLifetime",Long.toString(getLifetime),
                "getLifetime");
        put(document,root,"bringOnlineLifetime",Long.toString(bringOnlineLifetime),
                "bringOnlineLifetime");
        put(document,root,"putLifetime",Long.toString(putLifetime),
                "putLifetime");
        put(document,root,"copyLifetime",Long.toString(copyLifetime),
                "copyLifetime");
        put(document,root,"reserveSpaceLifetime",Long.toString(reserveSpaceLifetime),
                "reserveSpaceLifetime");
        put(document,root,"defaultSpaceLifetime",Long.toString(defaultSpaceLifetime),
                "defaultSpaceLifetime");
        put(document,root,"useUrlcopyScript", Boolean.toString(useUrlcopyScript),
                "useUrlcopyScript");
        put(document,root,"useDcapForSrmCopy", Boolean.toString(useDcapForSrmCopy),
                "useDcapForSrmCopy");
        put(document,root,"useGsiftpForSrmCopy", Boolean.toString(useGsiftpForSrmCopy),
                "useGsiftpForSrmCopy");
        put(document,root,"useHttpForSrmCopy", Boolean.toString(useHttpForSrmCopy),
                "useHttpForSrmCopy");
        put(document,root,"useFtpForSrmCopy", Boolean.toString(useFtpForSrmCopy),
                "useFtpForSrmCopy");
        put(document,root,"recursiveDirectoryCreation", Boolean.toString(recursiveDirectoryCreation),
                "recursiveDirectoryCreation");
        put(document,root,"advisoryDelete", Boolean.toString(advisoryDelete),
                "advisoryDelete");
        put(document,root,"nextRequestIdStorageTable", nextRequestIdStorageTable,
                "nextRequestIdStorageTable");

        put(document,root,"reserve_space_implicitely",Boolean.toString(reserve_space_implicitely)," true or false");
        put(document,root,
                "space_reservation_strict",
                Boolean.toString(space_reservation_strict)," true or false");
        put(document,root,
                "storage_info_update_period",
                Long.toString(storage_info_update_period),
                "storage_info_update_period in milliseconds");
        put(document,root,
                XML_LABEL_TRANSPORT_CLIENT,
                clientTransport,
                "transport to use when connecting to other SRM instances");
    }


    protected void set(String name, String value) {
        switch (name) {
        case "debug":
            debug = Boolean.valueOf(value);
            break;
        case "gsissl":
            gsissl = Boolean.valueOf(value);
            break;
        case "gsiftpclient":
            gsiftpclinet = value;
            break;
        case "urlcopy":
            urlcopy = value;
            break;
        case "buffer_size":
            buffer_size = Integer.parseInt(value);
            break;
        case "tcp_buffer_size":
            tcp_buffer_size = Integer.parseInt(value);
            break;
        case "port":
            port = Integer.parseInt(value);
            break;
        case "srmAuthzCacheLifetime":
            authzCacheLifetime = Long.parseLong(value);
            break;
        case "srm_root":
            srm_root = value;
            break;
        case "proxies_directory":
            proxies_directory = value;
            break;
        case "timeout":
            timeout = Integer.parseInt(value);
            break;
        case "timeout_script":
            timeout_script = value;
            break;
        case "getReqTQueueSize":
            getReqTQueueSize = Integer.parseInt(value);
            break;
        case "getThreadPoolSize":
            getThreadPoolSize = Integer.parseInt(value);
            break;
        case "getMaxWaitingRequests":
            getMaxWaitingRequests = Integer.parseInt(value);
            break;
        case "getReadyQueueSize":
            getReadyQueueSize = Integer.parseInt(value);
            break;
        case "getMaxReadyJobs":
            getMaxReadyJobs = Integer.parseInt(value);
            break;
        case "getMaxNumOfRetries":
            getMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "getRetryTimeout":
            getRetryTimeout = Long.parseLong(value);
            break;
        case "getMaxRunningBySameOwner":
            getMaxRunningBySameOwner = Integer.parseInt(value);
            break;
        case "bringOnlineReqTQueueSize":
            bringOnlineReqTQueueSize = Integer.parseInt(value);
            break;
        case "bringOnlineThreadPoolSize":
            bringOnlineThreadPoolSize = Integer.parseInt(value);
            break;
        case "bringOnlineMaxWaitingRequests":
            bringOnlineMaxWaitingRequests = Integer.parseInt(value);
            break;
        case "bringOnlineReadyQueueSize":
            bringOnlineReadyQueueSize = Integer.parseInt(value);
            break;
        case "bringOnlineMaxReadyJobs":
            bringOnlineMaxReadyJobs = Integer.parseInt(value);
            break;
        case "bringOnlineMaxNumOfRetries":
            bringOnlineMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "bringOnlineRetryTimeout":
            bringOnlineRetryTimeout = Long.parseLong(value);
            break;
        case "bringOnlineMaxRunningBySameOwner":
            bringOnlineMaxRunningBySameOwner = Integer.parseInt(value);
            break;
        case "lsReqTQueueSize":
            lsReqTQueueSize = Integer.parseInt(value);
            break;
        case "lsThreadPoolSize":
            lsThreadPoolSize = Integer.parseInt(value);
            break;
        case "lsMaxWaitingRequests":
            lsMaxWaitingRequests = Integer.parseInt(value);
            break;
        case "lsReadyQueueSize":
            lsReadyQueueSize = Integer.parseInt(value);
            break;
        case "lsMaxReadyJobs":
            lsMaxReadyJobs = Integer.parseInt(value);
            break;
        case "lsMaxNumOfRetries":
            lsMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "lsRetryTimeout":
            lsRetryTimeout = Long.parseLong(value);
            break;
        case "lsMaxRunningBySameOwner":
            lsMaxRunningBySameOwner = Integer.parseInt(value);
            break;
        case "putReqTQueueSize":
            putReqTQueueSize = Integer.parseInt(value);
            break;
        case "putThreadPoolSize":
            putThreadPoolSize = Integer.parseInt(value);
            break;
        case "putMaxWaitingRequests":
            putMaxWaitingRequests = Integer.parseInt(value);
            break;
        case "putReadyQueueSize":
            putReadyQueueSize = Integer.parseInt(value);
            break;
        case "putMaxReadyJobs":
            putMaxReadyJobs = Integer.parseInt(value);
            break;
        case "putMaxNumOfRetries":
            putMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "putRetryTimeout":
            putRetryTimeout = Long.parseLong(value);
            break;
        case "putMaxRunningBySameOwner":
            putMaxRunningBySameOwner = Integer.parseInt(value);
            break;
        case "copyReqTQueueSize":
            copyReqTQueueSize = Integer.parseInt(value);
            break;
        case "copyThreadPoolSize":
            copyThreadPoolSize = Integer.parseInt(value);
            break;
        case "copyMaxWaitingRequests":
            copyMaxWaitingRequests = Integer.parseInt(value);
            break;
        case "copyMaxNumOfRetries":
            copyMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "copyRetryTimeout":
            copyRetryTimeout = Long.parseLong(value);
            break;
        case "copyMaxRunningBySameOwner":
            copyMaxRunningBySameOwner = Integer.parseInt(value);
            break;
        case "getLifetime":
            getLifetime = Long.parseLong(value);
            break;
        case "bringOnlineLifetime":
            bringOnlineLifetime = Long.parseLong(value);
            break;
        case "putLifetime":
            putLifetime = Long.parseLong(value);
            break;
        case "copyLifetime":
            copyLifetime = Long.parseLong(value);
            break;
        case "defaultSpaceLifetime":
            defaultSpaceLifetime = Long.parseLong(value);
            break;
        case "useUrlcopyScript":
            useUrlcopyScript = Boolean.valueOf(value);
            break;
        case "useDcapForSrmCopy":
            useDcapForSrmCopy = Boolean.valueOf(value);
            break;
        case "useGsiftpForSrmCopy":
            useGsiftpForSrmCopy = Boolean.valueOf(value);
            break;
        case "useHttpForSrmCopy":
            useHttpForSrmCopy = Boolean.valueOf(value);
            break;
        case "useFtpForSrmCopy":
            useFtpForSrmCopy = Boolean.valueOf(value);
            break;
        case "recursiveDirectoryCreation":
            recursiveDirectoryCreation = Boolean.valueOf(value);
            break;
        case "advisoryDelete":
            advisoryDelete = Boolean.valueOf(value);
            break;
        case "nextRequestIdStorageTable":
            nextRequestIdStorageTable = value;
            break;
        case "reserve_space_implicitely":
            reserve_space_implicitely = Boolean.valueOf(value);
            break;
        case "space_reservation_strict":
            space_reservation_strict = Boolean.valueOf(value);
            break;
        case "storage_info_update_period":
            storage_info_update_period = Long.parseLong(value);
            break;
        case "qosPluginClass":
            qosPluginClass = value;
            break;
        case "qosConfigFile":
            qosConfigFile = value;
            break;
        case XML_LABEL_TRANSPORT_CLIENT:
            clientTransport = Transport.transportFor(value).name();
            break;
        case "reserveSpaceReqTQueueSize":
            reserveSpaceReqTQueueSize = Integer.parseInt(value);
            break;
        case "reserveSpaceThreadPoolSize":
            reserveSpaceThreadPoolSize = Integer.parseInt(value);
            break;
        case "reserveSpaceMaxWaitingRequests":
            reserveSpaceMaxWaitingRequests = Integer.parseInt(value);
            break;
        case "reserveSpaceReadyQueueSize":
            reserveSpaceReadyQueueSize = Integer.parseInt(value);
            break;
        case "reserveSpaceMaxReadyJobs":
            reserveSpaceMaxReadyJobs = Integer.parseInt(value);
            break;
        case "reserveSpaceMaxNumOfRetries":
            reserveSpaceMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "reserveSpaceRetryTimeout":
            reserveSpaceRetryTimeout = Long.parseLong(value);
            break;
        case "reserveSpaceMaxRunningBySameOwner":
            reserveSpaceMaxRunningBySameOwner = Integer.parseInt(value);
            break;
        }
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

    /** Getter for property storage.
     * @return Value of property storage.
     */
    public AbstractStorageElement getStorage() {
        return storage;
    }

    /** Setter for property storage.
     * @param storage New value of property storage.
     */
    public void setStorage(AbstractStorageElement storage) {
        this.storage = storage;
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
        sb.append("\n\t\t *** GetRequests Scheduler  Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.getLifetime);
        sb.append("\n\t\t max thread queue size =").append(this.getReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.getThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.getMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.getReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.getMaxReadyJobs);
        sb.append("\n\t\t maximum number of retries = ").append(this.getMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.getRetryTimeout);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.getMaxRunningBySameOwner);
        sb.append("\n\t\t switch to async mode delay=").append(timeToString(this.getSwitchToAsynchronousModeDelay));

        sb.append("\n\t\t *** BringOnlineRequests Scheduler  Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.bringOnlineLifetime);
        sb.append("\n\t\t max thread queue size =").append(this.bringOnlineReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.bringOnlineThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.bringOnlineMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.bringOnlineReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.bringOnlineMaxReadyJobs);
        sb.append("\n\t\t maximum number of retries = ").append(this.bringOnlineMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.bringOnlineRetryTimeout);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.bringOnlineMaxRunningBySameOwner);
        sb.append("\n\t\t switch to async mode delay=").append(timeToString(this.bringOnlineSwitchToAsynchronousModeDelay));

        sb.append("\n\t\t *** LsRequests Scheduler  Parameters **");
        sb.append("\n\t\t max thread queue size =").append(this.lsReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.lsThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.lsMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.lsReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.lsMaxReadyJobs);
        sb.append("\n\t\t maximum number of retries = ").append(this.lsMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.lsRetryTimeout);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.lsMaxRunningBySameOwner);
        sb.append("\n\t\t switch to async mode delay=").append(timeToString(this.lsSwitchToAsynchronousModeDelay));

        sb.append("\n\t\t *** PutRequests Scheduler  Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.putLifetime);
        sb.append("\n\t\t max thread queue size =").append(this.putReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.putThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.putMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.putReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.putMaxReadyJobs);
        sb.append("\n\t\t maximum number of retries = ").append(this.putMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.putRetryTimeout);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.putMaxRunningBySameOwner);
        sb.append("\n\t\t switch to async mode delay=").append(timeToString(this.putSwitchToAsynchronousModeDelay));

        sb.append("\n\t\t *** ReserveSpaceRequests Scheduler  Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.reserveSpaceLifetime);
        sb.append("\n\t\t max thread queue size =").append(this.reserveSpaceReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.reserveSpaceThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.reserveSpaceMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.reserveSpaceReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.reserveSpaceMaxReadyJobs);
        sb.append("\n\t\t maximum number of retries = ").append(this.reserveSpaceMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.reserveSpaceRetryTimeout);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.reserveSpaceMaxRunningBySameOwner);

        sb.append("\n\t\t *** CopyRequests Scheduler  Parameters **");
        sb.append("\n\t\t request Lifetime in miliseconds =").append(this.copyLifetime);
        sb.append("\n\t\t max thread queue size =").append(this.copyReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.copyThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.copyMaxWaitingRequests);
        sb.append("\n\t\t maximum number of retries = ").append(this.copyMaxNumOfRetries);
        sb.append("\n\t\t retry timeout in miliseconds =").append(this.copyRetryTimeout);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.copyMaxRunningBySameOwner);

        for (DatabaseParameters parameters: databaseParameters.values()) {
            sb.append(parameters);
        }

        sb.append("\n\treserve_space_implicitely=").append(this.reserve_space_implicitely);
        sb.append("\n\tspace_reservation_strict=").append(this.space_reservation_strict);
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
     * Getter for property getReqTQueueSize.
     * @return Value of property getReqTQueueSize.
     */
    public int getGetReqTQueueSize() {
        return getReqTQueueSize;
    }

    /**
     * Setter for property getReqTQueueSize.
     * @param getReqTQueueSize New value of property getReqTQueueSize.
     */
    public void setGetReqTQueueSize(int getReqTQueueSize) {
        this.getReqTQueueSize = getReqTQueueSize;
    }

    /**
     * Getter for property getThreadPoolSize.
     * @return Value of property getThreadPoolSize.
     */
    public int getGetThreadPoolSize() {
        return getThreadPoolSize;
    }

    /**
     * Setter for property getThreadPoolSize.
     * @param getThreadPoolSize New value of property getThreadPoolSize.
     */
    public void setGetThreadPoolSize(int getThreadPoolSize) {
        this.getThreadPoolSize = getThreadPoolSize;
    }

    /**
     * Getter for property getMaxWaitingRequests.
     * @return Value of property getMaxWaitingRequests.
     */
    public int getGetMaxWaitingRequests() {
        return getMaxWaitingRequests;
    }

    /**
     * Setter for property getMaxWaitingRequests.
     * @param getMaxWaitingRequests New value of property getMaxWaitingRequests.
     */
    public void setGetMaxWaitingRequests(int getMaxWaitingRequests) {
        this.getMaxWaitingRequests = getMaxWaitingRequests;
    }

    /**
     * Getter for property getReadyQueueSize.
     * @return Value of property getReadyQueueSize.
     */
    public int getGetReadyQueueSize() {
        return getReadyQueueSize;
    }

    /**
     * Setter for property getReadyQueueSize.
     * @param getReadyQueueSize New value of property getReadyQueueSize.
     */
    public void setGetReadyQueueSize(int getReadyQueueSize) {
        this.getReadyQueueSize = getReadyQueueSize;
    }

    /**
     * Getter for property getMaxReadyJobs.
     * @return Value of property getMaxReadyJobs.
     */
    public int getGetMaxReadyJobs() {
        return getMaxReadyJobs;
    }

    /**
     * Setter for property getMaxReadyJobs.
     * @param getMaxReadyJobs New value of property getMaxReadyJobs.
     */
    public void setGetMaxReadyJobs(int getMaxReadyJobs) {
        this.getMaxReadyJobs = getMaxReadyJobs;
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
     * Getter for property getMaxRunningBySameOwner.
     * @return Value of property getMaxRunningBySameOwner.
     */
    public int getGetMaxRunningBySameOwner() {
        return getMaxRunningBySameOwner;
    }

    /**
     * Setter for property getMaxRunningBySameOwner.
     * @param getMaxRunningBySameOwner New value of property getMaxRunningBySameOwner.
     */
    public void setGetMaxRunningBySameOwner(int getMaxRunningBySameOwner) {
        this.getMaxRunningBySameOwner = getMaxRunningBySameOwner;
    }

    /**
     * Getter for property putReqTQueueSize.
     * @return Value of property putReqTQueueSize.
     */
    public int getPutReqTQueueSize() {
        return putReqTQueueSize;
    }

    /**
     * Setter for property putReqTQueueSize.
     * @param putReqTQueueSize New value of property putReqTQueueSize.
     */
    public void setPutReqTQueueSize(int putReqTQueueSize) {
        this.putReqTQueueSize = putReqTQueueSize;
    }

    /**
     * Getter for property putThreadPoolSize.
     * @return Value of property putThreadPoolSize.
     */
    public int getPutThreadPoolSize() {
        return putThreadPoolSize;
    }

    /**
     * Setter for property putThreadPoolSize.
     * @param putThreadPoolSize New value of property putThreadPoolSize.
     */
    public void setPutThreadPoolSize(int putThreadPoolSize) {
        this.putThreadPoolSize = putThreadPoolSize;
    }

    /**
     * Getter for property putMaxWaitingRequests.
     * @return Value of property putMaxWaitingRequests.
     */
    public int getPutMaxWaitingRequests() {
        return putMaxWaitingRequests;
    }

    /**
     * Setter for property putMaxWaitingRequests.
     * @param putMaxWaitingRequests New value of property putMaxWaitingRequests.
     */
    public void setPutMaxWaitingRequests(int putMaxWaitingRequests) {
        this.putMaxWaitingRequests = putMaxWaitingRequests;
    }

    /**
     * Getter for property putReadyQueueSize.
     * @return Value of property putReadyQueueSize.
     */
    public int getPutReadyQueueSize() {
        return putReadyQueueSize;
    }

    /**
     * Setter for property putReadyQueueSize.
     * @param putReadyQueueSize New value of property putReadyQueueSize.
     */
    public void setPutReadyQueueSize(int putReadyQueueSize) {
        this.putReadyQueueSize = putReadyQueueSize;
    }

    /**
     * Getter for property putMaxReadyJobs.
     * @return Value of property putMaxReadyJobs.
     */
    public int getPutMaxReadyJobs() {
        return putMaxReadyJobs;
    }

    /**
     * Setter for property putMaxReadyJobs.
     * @param putMaxReadyJobs New value of property putMaxReadyJobs.
     */
    public void setPutMaxReadyJobs(int putMaxReadyJobs) {
        this.putMaxReadyJobs = putMaxReadyJobs;
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
     * Getter for property putMaxRunningBySameOwner.
     * @return Value of property putMaxRunningBySameOwner.
     */
    public int getPutMaxRunningBySameOwner() {
        return putMaxRunningBySameOwner;
    }

    /**
     * Setter for property putMaxRunningBySameOwner.
     * @param putMaxRunningBySameOwner New value of property putMaxRunningBySameOwner.
     */
    public void setPutMaxRunningBySameOwner(int putMaxRunningBySameOwner) {
        this.putMaxRunningBySameOwner = putMaxRunningBySameOwner;
    }

    /**
     * Getter for property copyReqTQueueSize.
     * @return Value of property copyReqTQueueSize.
     */
    public int getCopyReqTQueueSize() {
        return copyReqTQueueSize;
    }

    /**
     * Setter for property copyReqTQueueSize.
     * @param copyReqTQueueSize New value of property copyReqTQueueSize.
     */
    public void setCopyReqTQueueSize(int copyReqTQueueSize) {
        this.copyReqTQueueSize = copyReqTQueueSize;
    }

    /**
     * Getter for property copyThreadPoolSize.
     * @return Value of property copyThreadPoolSize.
     */
    public int getCopyThreadPoolSize() {
        return copyThreadPoolSize;
    }

    /**
     * Setter for property copyThreadPoolSize.
     * @param copyThreadPoolSize New value of property copyThreadPoolSize.
     */
    public void setCopyThreadPoolSize(int copyThreadPoolSize) {
        this.copyThreadPoolSize = copyThreadPoolSize;
    }

    /**
     * Getter for property copyMaxWaitingRequests.
     * @return Value of property copyMaxWaitingRequests.
     */
    public int getCopyMaxWaitingRequests() {
        return copyMaxWaitingRequests;
    }

    /**
     * Setter for property copyMaxWaitingRequests.
     * @param copyMaxWaitingRequests New value of property copyMaxWaitingRequests.
     */
    public void setCopyMaxWaitingRequests(int copyMaxWaitingRequests) {
        this.copyMaxWaitingRequests = copyMaxWaitingRequests;
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
     * Getter for property copyMaxRunningBySameOwner.
     * @return Value of property copyMaxRunningBySameOwner.
     */
    public int getCopyMaxRunningBySameOwner() {
        return copyMaxRunningBySameOwner;
    }

    /**
     * Setter for property copyMaxRunningBySameOwner.
     * @param copyMaxRunningBySameOwner New value of property copyMaxRunningBySameOwner.
     */
    public void setCopyMaxRunningBySameOwner(int copyMaxRunningBySameOwner) {
        this.copyMaxRunningBySameOwner = copyMaxRunningBySameOwner;
    }



    /**
     * Getter for property reserveSpaceReadyQueueSize.
     * @return Value of property reserveSpaceReadyQueueSize.
     */
    public int getReserveSpaceReadyQueueSize() {
        return reserveSpaceReadyQueueSize;
    }

    /**
     * Setter for property reserveSpaceReadyQueueSize.
     * @param reserveSpaceReadyQueueSize New value of property reserveSpaceReadyQueueSize.
     */
    public void setReserveSpaceReadyQueueSize(int reserveSpaceReadyQueueSize) {
        this.reserveSpaceReadyQueueSize = reserveSpaceReadyQueueSize;
    }

    /**
     * Getter for property reserveSpaceMaxReadyJobs.
     * @return Value of property reserveSpaceMaxReadyJobs.
     */
    public int getReserveSpaceMaxReadyJobs() {
        return reserveSpaceMaxReadyJobs;
    }

    /**
     * Setter for property reserveSpaceMaxReadyJobs.
     * @param reserveSpaceMaxReadyJobs New value of property reserveSpaceMaxReadyJobs.
     */
    public void setReserveSpaceMaxReadyJobs(int reserveSpaceMaxReadyJobs) {
        this.reserveSpaceMaxReadyJobs = reserveSpaceMaxReadyJobs;
    }




    /**
     * Getter for property reserveSpaceReqTQueueSize.
     * @return Value of property reserveSpaceReqTQueueSize.
     */
    public int getReserveSpaceReqTQueueSize() {
        return reserveSpaceReqTQueueSize;
    }

    /**
     * Setter for property reserveSpaceReqTQueueSize.
     * @param reserveSpaceReqTQueueSize New value of property reserveSpaceReqTQueueSize.
     */
    public void setReserveSpaceReqTQueueSize(int reserveSpaceReqTQueueSize) {
        this.reserveSpaceReqTQueueSize = reserveSpaceReqTQueueSize;
    }

    /**
     * Getter for property reserveSpaceThreadPoolSize.
     * @return Value of property reserveSpaceThreadPoolSize.
     */
    public int getReserveSpaceThreadPoolSize() {
        return reserveSpaceThreadPoolSize;
    }

    /**
     * Setter for property reserveSpaceThreadPoolSize.
     * @param reserveSpaceThreadPoolSize New value of property reserveSpaceThreadPoolSize.
     */
    public void setReserveSpaceThreadPoolSize(int reserveSpaceThreadPoolSize) {
        this.reserveSpaceThreadPoolSize = reserveSpaceThreadPoolSize;
    }

    /**
     * Getter for property reserveSpaceMaxWaitingRequests.
     * @return Value of property reserveSpaceMaxWaitingRequests.
     */
    public int getReserveSpaceMaxWaitingRequests() {
        return reserveSpaceMaxWaitingRequests;
    }

    /**
     * Setter for property reserveSpaceMaxWaitingRequests.
     * @param reserveSpaceMaxWaitingRequests New value of property reserveSpaceMaxWaitingRequests.
     */
    public void setReserveSpaceMaxWaitingRequests(int reserveSpaceMaxWaitingRequests) {
        this.reserveSpaceMaxWaitingRequests = reserveSpaceMaxWaitingRequests;
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
     * Getter for property reserveSpaceMaxRunningBySameOwner.
     * @return Value of property reserveSpaceMaxRunningBySameOwner.
     */
    public int getReserveSpaceMaxRunningBySameOwner() {
        return reserveSpaceMaxRunningBySameOwner;
    }

    /**
     * Setter for property reserveSpaceMaxRunningBySameOwner.
     * @param reserveSpaceMaxRunningBySameOwner New value of property reserveSpaceMaxRunningBySameOwner.
     */
    public void setReserveSpaceMaxRunningBySameOwner(int reserveSpaceMaxRunningBySameOwner) {
        this.reserveSpaceMaxRunningBySameOwner = reserveSpaceMaxRunningBySameOwner;
    }


    public static void main( String[] args) throws Exception {
        if(args == null || args.length !=2 ||
                args[0].equalsIgnoreCase("-h")  ||
                args[0].equalsIgnoreCase("-help")  ||
                args[0].equalsIgnoreCase("--h")  ||
                args[0].equalsIgnoreCase("--help")
                ) {
            System.err.println("Usage: Configuration load <file>\n or Configuration save <file>");
            return;
        }

        String command = args[0];
        String file = args[1];

        switch (command) {
        case "load": {
            System.out.println("reading configuration from file " + file);
            Configuration config = new Configuration(file);
            System.out.println("read configuration successfully:");
            System.out.print(config.toString());
            break;
        }
        case "save": {
            Configuration config = new Configuration();
            System.out.print(config.toString());
            System.out.println("writing configuration to a file " + file);
            config.write(file);
            System.out.println("done");
            break;
        }
        default:
            System.err
                    .println("Usage: Co<nfiguration load <file>\n or Configuration save <file>");

            break;
        }
    }

    /**
     * Getter for property reserve_space_implicitely.
     *
     * @return Value of property reserve_space_implicitely.
     */
    public boolean isReserve_space_implicitely() {
        return reserve_space_implicitely;
    }

    /**
     * Setter for property reserve_space_implicitely.
     *
     * @param reserve_space_implicitely New value of property reserve_space_implicitely.
     */
    public void setReserve_space_implicitely(boolean reserve_space_implicitely) {
        this.reserve_space_implicitely = reserve_space_implicitely;
    }

    /**
     * Getter for property space_reservation_strict.
     * @return Value of property space_reservation_strict.
     */
    public boolean isSpace_reservation_strict() {
        return space_reservation_strict;
    }

    /**
     * Setter for property space_reservation_strict.
     * @param space_reservation_strict New value of property space_reservation_strict.
     */
    public void setSpace_reservation_strict(boolean space_reservation_strict) {
        this.space_reservation_strict = space_reservation_strict;
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

    public int getBringOnlineReqTQueueSize() {
        return bringOnlineReqTQueueSize;
    }

    public void setBringOnlineReqTQueueSize(int bringOnlineReqTQueueSize) {
        this.bringOnlineReqTQueueSize = bringOnlineReqTQueueSize;
    }

    public int getBringOnlineThreadPoolSize() {
        return bringOnlineThreadPoolSize;
    }

    public void setBringOnlineThreadPoolSize(int bringOnlineThreadPoolSize) {
        this.bringOnlineThreadPoolSize = bringOnlineThreadPoolSize;
    }

    public int getBringOnlineMaxWaitingRequests() {
        return bringOnlineMaxWaitingRequests;
    }

    public void setBringOnlineMaxWaitingRequests(int bringOnlineMaxWaitingRequests) {
        this.bringOnlineMaxWaitingRequests = bringOnlineMaxWaitingRequests;
    }

    public int getBringOnlineReadyQueueSize() {
        return bringOnlineReadyQueueSize;
    }

    public void setBringOnlineReadyQueueSize(int bringOnlineReadyQueueSize) {
        this.bringOnlineReadyQueueSize = bringOnlineReadyQueueSize;
    }

    public int getBringOnlineMaxReadyJobs() {
        return bringOnlineMaxReadyJobs;
    }

    public void setBringOnlineMaxReadyJobs(int bringOnlineMaxReadyJobs) {
        this.bringOnlineMaxReadyJobs = bringOnlineMaxReadyJobs;
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

    public int getBringOnlineMaxRunningBySameOwner() {
        return bringOnlineMaxRunningBySameOwner;
    }

    public void setBringOnlineMaxRunningBySameOwner(int bringOnlineMaxRunningBySameOwner) {
        this.bringOnlineMaxRunningBySameOwner = bringOnlineMaxRunningBySameOwner;
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

    public int getLsReqTQueueSize() {
        return lsReqTQueueSize;
    }

    public void setLsReqTQueueSize(int lsReqTQueueSize) {
        this.lsReqTQueueSize = lsReqTQueueSize;
    }

    public int getLsThreadPoolSize() {
        return lsThreadPoolSize;
    }

    public void setLsThreadPoolSize(int lsThreadPoolSize) {
        this.lsThreadPoolSize = lsThreadPoolSize;
    }

    public int getLsMaxWaitingRequests() {
        return lsMaxWaitingRequests;
    }

    public void setLsMaxWaitingRequests(int lsMaxWaitingRequests) {
        this.lsMaxWaitingRequests = lsMaxWaitingRequests;
    }

    public int getLsReadyQueueSize() {
        return lsReadyQueueSize;
    }

    public void setLsReadyQueueSize(int lsReadyQueueSize) {
        this.lsReadyQueueSize = lsReadyQueueSize;
    }

    public int getLsMaxReadyJobs() {
        return lsMaxReadyJobs;
    }

    public void setLsMaxReadyJobs(int lsMaxReadyJobs) {
        this.lsMaxReadyJobs = lsMaxReadyJobs;
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

    public int getLsMaxRunningBySameOwner() {
        return lsMaxRunningBySameOwner;
    }

    public void setLsMaxRunningBySameOwner(int lsMaxRunningBySameOwner) {
        this.lsMaxRunningBySameOwner = lsMaxRunningBySameOwner;
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
