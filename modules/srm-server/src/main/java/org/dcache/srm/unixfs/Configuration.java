package org.dcache.srm.unixfs;

import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

import org.dcache.srm.client.Transport;


/**
 * Configuration class that is based on a standard set of SRM configuration
 * but that adds support for UnixFS-specific configuration options.
 */
public class Configuration extends org.dcache.srm.util.Configuration
{
    private static final String XML_LABEL_TRANSPORT_CLIENT = "client_transport";

    private String _kpwdfile="../conf/dcache.kpwd";

    private int getReqTQueueSize=1000;
    private int getThreadPoolSize=30;
    private int getMaxWaitingRequests=1000;
    private int getReadyQueueSize=1000;
    private int getMaxReadyJobs=60;
    private int getMaxRunningBySameOwner=10;

    private int lsReqTQueueSize=1000;
    private int lsThreadPoolSize=30;
    private int lsMaxWaitingRequests=1000;
    private int lsReadyQueueSize=1000;
    private int lsMaxReadyJobs=60;
    private int lsMaxRunningBySameOwner=10;

    private int bringOnlineReqTQueueSize=1000;
    private int bringOnlineThreadPoolSize=30;
    private int bringOnlineMaxWaitingRequests=1000;
    private int bringOnlineReadyQueueSize=1000;
    private int bringOnlineMaxReadyJobs=60;
    private int bringOnlineMaxRunningBySameOwner=10;

    private int putReqTQueueSize=1000;
    private int putThreadPoolSize=30;
    private int putMaxWaitingRequests=1000;
    private int putReadyQueueSize=1000;
    private int putMaxReadyJobs=60;
    private int putMaxRunningBySameOwner=10;

    private int copyReqTQueueSize=1000;
    private int copyThreadPoolSize=30;
    private int copyMaxWaitingRequests=1000;
    private int copyMaxRunningBySameOwner=10;

    private int reserveSpaceReqTQueueSize=1000;
    private int reserveSpaceThreadPoolSize=30;
    private int reserveSpaceMaxWaitingRequests=1000;
    private int reserveSpaceReadyQueueSize=1000;
    private int reserveSpaceMaxReadyJobs=60;
    private int reserveSpaceMaxRunningBySameOwner=10;

    public Configuration()
    {
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
        try {
            addSrmHost(InetAddress.getLocalHost().getCanonicalHostName());
        } catch(IOException ioe) {
            addSrmHost("localhost");
        }
    }

    /** Getter for property kpwdFile.
     * @return Value of property kpwdFile.
     */
    public String getKpwdfile() {
        return _kpwdfile;
    }

    /** Setter for property kpwdFile.
     * @param kpwdfile New value of property kpwdFile.
     */
    public void setKpwdfile(String kpwdfile) {
        this._kpwdfile = kpwdfile;
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

    protected void set(String name, String value)
    {
        switch (name) {
        case "kpwdfile":
            _kpwdfile = value;
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
        case "copyMaxRunningBySameOwner":
            copyMaxRunningBySameOwner = Integer.parseInt(value);
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
        case "reserveSpaceMaxRunningBySameOwner":
            reserveSpaceMaxRunningBySameOwner = Integer.parseInt(value);
            break;
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
        case "getMaxNumOfRetries":
            getMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "getRetryTimeout":
            getRetryTimeout = Long.parseLong(value);
            break;
        case "bringOnlineMaxNumOfRetries":
            bringOnlineMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "bringOnlineRetryTimeout":
            bringOnlineRetryTimeout = Long.parseLong(value);
            break;
        case "lsMaxNumOfRetries":
            lsMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "lsRetryTimeout":
            lsRetryTimeout = Long.parseLong(value);
            break;
        case "putMaxNumOfRetries":
            putMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "putRetryTimeout":
            putRetryTimeout = Long.parseLong(value);
            break;
        case "copyMaxNumOfRetries":
            copyMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "copyRetryTimeout":
            copyRetryTimeout = Long.parseLong(value);
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
        case "reserveSpaceMaxNumOfRetries":
            reserveSpaceMaxNumOfRetries = Integer.parseInt(value);
            break;
        case "reserveSpaceRetryTimeout":
            reserveSpaceRetryTimeout = Long.parseLong(value);
            break;
        }
    }

    protected void write(Document document, Element root)
    {
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
        put(document,root,"getMaxNumOfRetries",Integer.toString(getMaxNumOfRetries),
            "Maximum Number Of Retries for get file request");
        put(document,root,"getRetryTimeout",Long.toString(getRetryTimeout),
            "get request Retry Timeout in milliseconds");

        put(document,root,"bringOnlineMaxNumOfRetries",Integer.toString(bringOnlineMaxNumOfRetries),
            "Maximum Number Of Retries for bringOnline file request");
        put(document,root,"bringOnlineRetryTimeout",Long.toString(bringOnlineRetryTimeout),
            "bringOnline request Retry Timeout in milliseconds");

        put(document,root,"lsMaxNumOfRetries",Integer.toString(lsMaxNumOfRetries),
            "Maximum Number Of Retries for ls file request");
        put(document,root,"lsRetryTimeout",Long.toString(lsRetryTimeout),
            "ls request Retry Timeout in milliseconds");

        put(document,root,"putMaxNumOfRetries",Integer.toString(putMaxNumOfRetries),
            "Maximum Number Of Retries for put file request");
        put(document,root,"putRetryTimeout",Long.toString(putRetryTimeout),
            "put request Retry Timeout in milliseconds");

        put(document,root,"reserveSpaceMaxNumOfRetries",Integer.toString(reserveSpaceMaxNumOfRetries),
            "Maximum Number Of Retries for reserveSpace file request");
        put(document,root,"reserveSpaceRetryTimeout",Long.toString(reserveSpaceRetryTimeout),
            "reserveSpace request Retry Timeout in milliseconds");

        put(document,root,"copyMaxNumOfRetries",Integer.toString(copyMaxNumOfRetries),
            "Maximum Number Of Retries for copy file request");
        put(document,root,"copyRetryTimeout",Long.toString(copyRetryTimeout),
            "copy request Retry Timeout in milliseconds");

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

        put(document,root,
            "storage_info_update_period",
            Long.toString(storage_info_update_period),
            "storage_info_update_period in milliseconds");
        put(document,root,
            XML_LABEL_TRANSPORT_CLIENT,
            clientTransport,
            "transport to use when connecting to other SRM instances");
        put(document,root, "kpwdfile", _kpwdfile,
                "kpwdfile, a dcache authorization database ");

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
        put(document, root, "bringOnlineMaxReadyJobs", Integer.toString(bringOnlineMaxReadyJobs),
            "bringOnlineMaxReadyJobs");
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
        put(document,root,"putMaxRunningBySameOwner",Integer.toString(putMaxRunningBySameOwner),
            "putMaxRunningBySameOwner");

        put(document,root,"copyReqTQueueSize",Integer.toString(copyReqTQueueSize),
            "copyReqTQueueSize");
        put(document,root,"copyThreadPoolSize",Integer.toString(copyThreadPoolSize),
            "copyThreadPoolSize");
        put(document,root,"copyMaxWaitingRequests",Integer.toString(copyMaxWaitingRequests),
            "copyMaxWaitingRequests");
        put(document,root,"copyMaxRunningBySameOwner",Integer.toString(copyMaxRunningBySameOwner),
            "copyMaxRunningBySameOwner");

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
        put(document,root,"reserveSpaceMaxRunningBySameOwner",Integer.toString(reserveSpaceMaxRunningBySameOwner),
            "reserveSpaceMaxRunningBySameOwner");
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\tkpwdfile=").append(this._kpwdfile);
        sb.append(super.toString());
        sb.append("\n\t\t *** GetRequests Scheduler  Parameters **");
        sb.append("\n\t\t max thread queue size =").append(this.getReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.getThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.getMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.getReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.getMaxReadyJobs);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.getMaxRunningBySameOwner);

        sb.append("\n\t\t *** BringOnlineRequests Scheduler  Parameters **");
        sb.append("\n\t\t max thread queue size =").append(this.bringOnlineReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.bringOnlineThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.bringOnlineMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.bringOnlineReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.bringOnlineMaxReadyJobs);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.bringOnlineMaxRunningBySameOwner);

        sb.append("\n\t\t *** LsRequests Scheduler  Parameters **");
        sb.append("\n\t\t max thread queue size =").append(this.lsReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.lsThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.lsMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.lsReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.lsMaxReadyJobs);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.lsMaxRunningBySameOwner);

        sb.append("\n\t\t *** PutRequests Scheduler  Parameters **");
        sb.append("\n\t\t max thread queue size =").append(this.putReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.putThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.putMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.putReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.putMaxReadyJobs);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.putMaxRunningBySameOwner);

        sb.append("\n\t\t *** ReserveSpaceRequests Scheduler  Parameters **");
        sb.append("\n\t\t max thread queue size =").append(this.reserveSpaceReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.reserveSpaceThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.reserveSpaceMaxWaitingRequests);
        sb.append("\n\t\t max ready queue size =").append(this.reserveSpaceReadyQueueSize);
        sb.append("\n\t\t max number of ready file requests =").append(this.reserveSpaceMaxReadyJobs);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.reserveSpaceMaxRunningBySameOwner);

        sb.append("\n\t\t *** CopyRequests Scheduler  Parameters **");
        sb.append("\n\t\t max thread queue size =").append(this.copyReqTQueueSize);
        sb.append("\n\t\t max number of threads =").append(this.copyThreadPoolSize);
        sb.append("\n\t\t max number of waiting file requests =").append(this.copyMaxWaitingRequests);
        sb.append("\n\t\t maximum number of jobs running created");
        sb.append("\n\t\t by the same owner if other jobs are queued =").append(this.copyMaxRunningBySameOwner);
        return sb.toString();
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

    public int getBringOnlineMaxRunningBySameOwner() {
        return bringOnlineMaxRunningBySameOwner;
    }

    public void setBringOnlineMaxRunningBySameOwner(int bringOnlineMaxRunningBySameOwner) {
        this.bringOnlineMaxRunningBySameOwner = bringOnlineMaxRunningBySameOwner;
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

    public int getLsMaxRunningBySameOwner() {
        return lsMaxRunningBySameOwner;
    }

    public void setLsMaxRunningBySameOwner(int lsMaxRunningBySameOwner) {
        this.lsMaxRunningBySameOwner = lsMaxRunningBySameOwner;
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
}
