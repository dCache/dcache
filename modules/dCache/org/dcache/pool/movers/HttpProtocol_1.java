package org.dcache.pool.movers;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

import diskCacheV111.vehicles.*;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.HttpConnectionHandler;
import diskCacheV111.util.HttpByteRange;
import org.dcache.pool.repository.Allocator;
import org.dcache.util.PortRange;

import dmg.cells.nucleus.*;
import java.io.*;
import java.net.URL;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.util.List;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.util.NetworkUtils;

public class HttpProtocol_1 implements MoverProtocol
{
    public static final Logger _log = LoggerFactory.getLogger(HttpProtocol_1.class);

    public static final int READ   =  1;
    public static final int WRITE  =  2;
    public static final long SERVER_LIFE_SPAN= 60 * 5 * 1000; /* 5 minutes */

    private final CellEndpoint      _cell;
    private ServerSocket httpserver;
    private long timeout_time;
    private long start_transfer_time    = System.currentTimeMillis();

    /**
     * Port range to use.
     */
    protected final PortRange _portRange;

    public HttpProtocol_1(CellEndpoint cell) {
        _cell = cell;

        String range = System.getProperty("org.globus.tcp.port.range");
        if (range != null) {
            _portRange = PortRange.valueOf(range);
        } else {
            _portRange = new PortRange(0);
        }

        say("HttpProtocol_1 created");
    }

    private void say(String str) {
        _log.info(str);
    }

    private void esay(String str) {
        _log.error(str);
    }

    private HttpConnectionHandler httpconnection = null;
    public void runIO(RandomAccessFile diskFile,
                       ProtocolInfo protocol,
                       StorageInfo  storage,
                       PnfsId       pnfsId ,
                       Allocator    allocator,
                       IoMode       access)
        throws Exception
    {
        say("runIO("+diskFile+",\n"+
            protocol+",\n"+storage+",\n"+pnfsId+",\n"+access+")");
        if(! (protocol instanceof HttpProtocolInfo))
            {
                throw new  CacheException(44, "protocol info not HttpProtocolInfo");
            }
        HttpProtocolInfo httpProtocolInfo = (HttpProtocolInfo) protocol;

        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket();
            _portRange.bind(serverSocket);
        } catch(IOException ioe) {
            esay("exception while trying to create a server socket : "+ioe);
            throw ioe;
        }
        this.httpserver = serverSocket;

        String targetUrl = buildUrl(serverSocket.getLocalPort(), httpProtocolInfo);

        CellPath cellpath = new CellPath(httpProtocolInfo.getHttpDoorCellName (),
                                         httpProtocolInfo.getHttpDoorDomainName ());
        say(" runIO() cellpath="+cellpath);
        HttpDoorUrlInfoMessage httpDoorMessage =
            new HttpDoorUrlInfoMessage(pnfsId.getId (),targetUrl);
        say(" runIO() created message");
        _cell.sendMessage (new CellMessage(cellpath,httpDoorMessage));

        try
            {
                httpserver.setSoTimeout((int) SERVER_LIFE_SPAN);
                Socket connection = httpserver.accept();
                say(" accepted connection!!!");
                httpconnection = new HttpConnectionHandler(connection);

                String method = httpconnection.getHttpMethod ();
                if(!method.equals("GET"))
                    {
                        String error_string = "method : "+method+" is not supported";
                        httpconnection.returnErrorHeader (error_string);
                        return;
                    }

                say("method = "+method+" url="+httpconnection.getUrlString());
                String[] headers = httpconnection.getHeaders();
                for(int i = 0;i<headers.length;++i)
                    {
                        String header =httpconnection.getHeaderValue(headers[i]);
                        say("header["+i+"]="+headers[i]+":"+header);
                    }

                URL url = httpconnection.getUrl();
                String path = url.getPath();
                say("url returned path : "+path);

                PnfsFile transferfile = new PnfsFile(url.getPath());
                PnfsFile requestedfile = new PnfsFile(httpProtocolInfo.getPath());

                if(!transferfile.equals (requestedfile))
                    {
                        say("incorrect file requested : "+url.getPath());
                        String error_string = "incorrect path : "+url.getPath();
                        httpconnection.returnErrorHeader (error_string);
                        return;
                    }
                say("received request for a correct file : "+url.getPath()+" start transmission");

                List<HttpByteRange>ranges = null;
                try{
                    String rangeHeader = httpconnection.getHeaderValue("range");
                    if(rangeHeader != null)
                        ranges =  HttpByteRange.parseRanges(rangeHeader,0,diskFile.length()-1);
                }catch(ParseException e)
                    {
                    say("(HttpProtocol_1) " + e.getMessage());
                 }

                /* We do not know how to handle multiple ranges so
                 * we treat a request of multiple ranges as invalid and
                 * send the entire file -- the rfc's prescription when it
                 * comes to invalid ranges.
                 */
                if(ranges == null || ranges.size() != 1){
                    httpconnection.sendFile(diskFile);
                }else{
                    HttpByteRange range = ranges.get(0);
                    say("received request for range: " + range);
                    httpconnection.sendPartialFile(diskFile, range.getLower(), range.getSize());
                }
                say("transmission complete");
            }
        catch(java.net.SocketTimeoutException ste)
            {
                say("(HttpProtocol_1) http servet timeout ");

            }
        catch(Exception e)
            {
                esay("(HttpProtocol_1) error in the http server thread : "+e);
            }
        finally
            {
                say("(HttpProtocol_1) closing server socket");
                try
                    {
                        httpserver.close();
                    }
                catch(IOException ee)
                    {
                    }
                say("(HttpProtocol_1) done");
            }
        say(" runIO() done");
    }
    public long getLastTransferred()
    {
        if(httpconnection == null)
            {
                return  start_transfer_time;
            }
        return httpconnection.getLast_transfer_time();
    }

    public void setAttribute(String name, Object attribute)
    {
    }
    public Object getAttribute(String name)
    {
        return null;
    }
    public long getBytesTransferred()
    {
        if(httpconnection == null)
            {
                return  0;
            }
        return httpconnection.transfered();

    }

    public long getTransferTime()
    {
        return System.currentTimeMillis() - start_transfer_time;
    }
    public boolean wasChanged(){ return false; }

    private String buildUrl(int localPort, HttpProtocolInfo httpProtocolInfo)
            throws Exception{
        InetAddress localAddress = NetworkUtils.getLocalAddressForClient(httpProtocolInfo.getHosts());

        StringBuffer url_sb = new StringBuffer("http://");
        url_sb.append(localAddress.getCanonicalHostName());
        url_sb.append(':').append(localPort);
        if(!httpProtocolInfo.getPath().startsWith("/"))
            {
                url_sb.append('/');
            }
        url_sb.append(httpProtocolInfo.getPath());
        say(" redirecting to  "+
            url_sb.toString());

        return url_sb.toString();
    }
}



