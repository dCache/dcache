// $Id: HttpDoor.java,v 1.12 2003-07-03 23:39:45 cvs Exp $
/*
 * @(#)HttpDoor.java	1.1 07/02/02
 *
 * Copyright 2002 Desy Lab & FermiLab. All rights reserved.
 */

package diskCacheV111.doors;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;
import diskCacheV111.cells.* ;

import dmg.cells.nucleus.*;
import dmg.cells.network.*;
import dmg.util.*;

import java.util.*;
import java.io.*;
//import java.net.*;
import java.net.URL;
import java.lang.reflect.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides access to the Storage through HTTP protocol
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */
public class HttpDoor extends CellAdapter  implements Runnable
{
    private final static Logger _log =
        LoggerFactory.getLogger(HttpDoor.class);

  /* synchronization object */
  private Object sync = new Object();
  private String         host;
  private PnfsHandler    pnfs_handler;
  private String redirection_url;
  private String poolManagerName = "PoolManager";
  private String pnfsManagerName = "PnfsManager";
  private CellPath poolManagerPath = new CellPath(poolManagerName);
  private CellPath pnfsManagerPath = new CellPath(pnfsManagerName);
  private Reader  engine_reader;
  private OutputStream engine_output;
  private Writer engine_writer;

  private List<FsPath> _allowedPaths = Collections.emptyList();
  private FsPath _rootPath = new FsPath();

  private synchronized String getRedirectionUrl()
  {
    return redirection_url;
  }

  private synchronized void setRedirectionUrl(String redirection_url)
  {
     this.redirection_url = redirection_url;
  }


  private static final int poolManagerTimeout =   1500 ;
  private static final int poolTimeout        = 5 * 60 ;

  private static long   counter = 10000 ;

  protected static boolean _debug = false ;

  protected static  boolean initDone = false ;
  protected synchronized static void __init( Args args )
  {
     if( initDone )
     {
       return ;
     }
     initDone = true ;
  }



  protected synchronized static long next()
  {
    return counter++ ;
  }

    /**
     * Creates an instance of HttpDoor </p>
     *
     * @param  name
     *         String containing cell name
     * @param  engine
     *         StreamEngine incapsulating socket connected
     *         to the http client
     * @param  args
     *         arguments
     */
  public HttpDoor (String name, StreamEngine engine, Args args)
  {
    super( name , args , false );

    _log.info("Starting HttpDoor:");
    _log.info("$Id: HttpDoor.java,v 1.12 2003-07-03 23:39:45 cvs Exp $");

    __init( args ) ;

    host     = engine.getInetAddress().getHostName();
    pnfs_handler = new PnfsHandler( this, pnfsManagerPath ) ;
    // HttpConnectionHandler will read the http request header and
    // generate a redirection or error response for us
    try
    {
      // create HttpConnectionHandler
       engine_reader = engine.getReader();
       engine_output = engine.getOutputStream();
       engine_writer =  engine.getWriter();
       if(engine_reader == null)
       {
           _log.warn("engine.getReader() returned null");
           throw new IllegalArgumentException("bad engine:"+
            "engine.getReader() returned null");
       }
       if(engine_output == null)
       {
           _log.warn("engine.getOutputStream() returned null");
           throw new IllegalArgumentException("bad engine:"+
           "engine.getOutputStream() returned null");
       }
       if(engine_writer == null)
       {
           _log.warn("engine.getWriter() returned null");
           throw new IllegalArgumentException("bad engine:"+
           "engine.getWriter() returned null");
       }

       String allowedPaths = args.getOpt("allowedPaths");
       if (allowedPaths != null) {
           _allowedPaths = new ArrayList();
           for (String path: allowedPaths.split(":")) {
               _allowedPaths.add(new FsPath(path));
           }
       }

       String rootPath = args.getOpt("rootPath");
       if (rootPath != null) {
           _rootPath = new FsPath(rootPath);
       }

      new Thread(this).start();
      useInterpreter(true);
      start() ;
    }
    catch(IllegalArgumentException iae)
    {
         start();
         _log.info("done");
         kill();
        throw iae;
    }
  }


    /**
     * Forms a full PNFS path. The path is created by concatenating
     * the root path and path. The root path is guaranteed to be a
     * prefix of the path returned.
     */
    private FsPath createFullPath(String path)
    {
        return new FsPath(_rootPath, new FsPath(path));
    }

    /**
     * check wether the given path matches against a list of allowed paths
     * @param pathToOpen the path which is going to be checked
     * @param authorizedWritePathList the list of allowed paths
     * @return
     */
    private boolean isAllowedPath(FsPath path)
    {
        if (_allowedPaths.isEmpty()) {
            return true;
        }
        for (FsPath prefix: _allowedPaths) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

  public void run()
  {
     HttpConnectionHandler connectionHandler = null;
      try
      {
          connectionHandler =
            new HttpConnectionHandler(
               new BufferedReader(engine_reader ),
               engine_output,
               new BufferedWriter(engine_writer));
          // creation succeded, header is parsed bu this time
          // get http method
          String method = connectionHandler.getHttpMethod ();
          // in the future we will handle publishing of the files as well
          if(!method.equals("GET"))
          {
            String error_string = " method "+method+ " is not currently supported";
            _log.info("returnErrorHeader");
            connectionHandler.returnErrorHeader(error_string);
            return;
          }
          _log.info("method = "+method+" url="+connectionHandler.getUrlString());
          String[] headers = connectionHandler.getHeaders();
          for(int i = 0;i<headers.length;++i)
          {
           String header =connectionHandler.getHeaderValue(headers[i]);
           _log.info("header["+i+"]="+headers[i]+":"+header);
          }
          _log.info("processing GET method");
          URL url = connectionHandler.getUrl ();
          String path = url.getPath();
          _log.info("url returned path : "+path);

          FsPath fullPath = createFullPath(path);
          if (!isAllowedPath(fullPath)) {
            String error_string = "Access denied";
            connectionHandler.returnErrorHeader(error_string);
            return;
          }

          // the path in url should corresponf to the pnfs path
          // process request will try boolean bring the file to the pool
          // and get the http url of the pool
          String newURL= processGetRequest(fullPath.toString());
          if(newURL != null)
          {
            // redirect client to the pool url,
            // our mission is completed by now
            _log.info("redirecting to : "+newURL);
            connectionHandler.returnRedirectHeader(newURL);
          }
          else
          {
            // we are out of luck
            String error_string = "internal problem: can not get redirection url";
            _log.info("returnErrorHeader, returned url is null");
            connectionHandler.returnErrorHeader(error_string);
            return;
          }
      }
        catch(IOException ioe)
        {
            // we are completely out of luck
            _log.warn("IOException : ", ioe);
            String error_string = ioe.getMessage();
            try
            {
              connectionHandler.returnErrorHeader(error_string==null?ioe.toString():error_string);
            }
            catch(IOException io)
            {
            // silently ignore, nothing we can do now
            }

        }
      catch(Exception e)
      {
           _log.warn("Exception e: ", e);
            String error_string = e.getMessage();
            _log.warn("returning error header with  a string "+error_string);
            try
            {
              connectionHandler.returnErrorHeader(error_string==null?e.toString():error_string);
            }
            catch(IOException io)
            {
            // silently ignore, nothing we can do now
            }
      }
      finally
      {
         _log.info("done");
         kill();
      }
  }

    /**
     * gets the pnsf id of the file from pnfs manager
     * contacts pool manager and gets the pool that will handle
     * output of the file
     * contacts the pool, which will start an http server
     * and waits for the message from the pool with the pool
     * specific url of the pool http server
     * </p>
     *
     * @param  patj
     *         pnfs path to the file
     *
     * @return  the url of the pool http server
     *
     * @throws  IOException
     *          If file does not exist or anything goes wrong
     */
  private String processGetRequest(String path) throws Exception
  {

     PnfsGetStorageInfoMessage storage_info_msg =null;
     try
     {
       storage_info_msg =pnfs_handler.getStorageInfoByPath(path);;
     }
     catch( diskCacheV111.util.CacheException ce)
     {
       _log.warn("can not find pnfsid of path : "+path);
       _log.warn("CacheException = "+ce);
       throw new IOException("can not find pnfsid of path : "+path +" root error " +ce.getMessage());
     }
     PnfsId pnfs_id = storage_info_msg.getPnfsId();
     StorageInfo storage_info =storage_info_msg.getStorageInfo();
     diskCacheV111.util.FileMetaData util_fmd = storage_info_msg.getMetaData();
     diskCacheV111.util.FileMetaData.Permissions perms = util_fmd.getWorldPermissions();
     if(!perms.canRead ())
     {
       throw new IOException("have no permissions to read this file : "+path);
     }

     HttpProtocolInfo protocol_info =
       new HttpProtocolInfo("Http",1,1,host,0,this.getCellName(),
         this.getCellDomainName(),path);
     _log.info("created HttpProtocolInfo = "+protocol_info);
   _log.info("processGetRequest ,asking for read pool");
   String pool = askForReadPool(pnfs_id,
                                storage_info,
                                protocol_info,
                                false); /* false for read */
   _log.info("processGetRequest, read pool is "+pool+"asking for file");
   askForFile( pool ,
               pnfs_id ,
               storage_info ,
               protocol_info ,
               false      );/* false for read */
   _log.info("processGetRequest, asked for file");
     try
     {
       synchronized(sync)
       {
         if(getRedirectionUrl () == null)
         {
           _log.info("processGetRequest, waiting");
           sync.wait();
         }
       }
     }
     catch(InterruptedException ie)
     {
     }
     return this.redirection_url;
  }

    //
    // the cell implemetation
    //
    public String toString(){ return "HttpDoor@"+host; }

    public void getInfo( PrintWriter pw )
    {
     	pw.println( "            HTTPDoor" );
	    pw.println( "         Host  : "+host );
    }

    //handle post-transfer success/failure messages going back to the client
    public void   messageArrived( CellMessage msg )
    {
       Object object = msg.getMessageObject();
       _log.info ("Message messageArrived ["+object.getClass()+"]="+object.toString());
       _log.info ("Message messageArrived source = "+msg.getSourceAddress());
       if(object instanceof HttpDoorUrlInfoMessage)
       {
         HttpDoorUrlInfoMessage reply =
           (HttpDoorUrlInfoMessage) object;
         synchronized(sync)
         {
           this.setRedirectionUrl (reply.getUrl());
           sync.notifyAll();
         }
       }
       else if (object instanceof DoorTransferFinishedMessage)
        {

            DoorTransferFinishedMessage reply =
                 (DoorTransferFinishedMessage)object ;


            HttpProtocolInfo      info   =
                     (HttpProtocolInfo)reply.getProtocolInfo() ;

        }
        else
        {
            _log.info ("Unexpected message class "+object.getClass());
            _log.info ("source = "+msg.getSourceAddress());
        }
    }

 // these were taken almost without changes from other doors

  private void   askForFile( String       pool ,
                             PnfsId       pnfsId ,
                             StorageInfo  storageInfo ,
                             ProtocolInfo protocolInfo ,
                             boolean      isWrite      ) throws Exception
  {
    _log.info("Trying pool "+pool+" for "+(isWrite?"Write":"Read"));
    PoolIoFileMessage poolMessage  =  isWrite ?
         (PoolIoFileMessage)
         new PoolAcceptFileMessage(
                              pool,
                              pnfsId,
                              protocolInfo ,
                  storageInfo     )
         :
         (PoolIoFileMessage)
         new PoolDeliverFileMessage(
                              pool,
                              pnfsId,
                              protocolInfo ,
                  storageInfo     );

      poolMessage.setId( next() ) ;

    CellMessage reply = sendAndWait(
                               new CellMessage(
                                    new CellPath(pool) ,
                                    poolMessage
                                               )  ,
                              poolTimeout*1000
                                      ) ;
    if( reply == null)
    {
       throw new  Exception( "Pool request timed out : "+pool ) ;
    }

    Object replyObject = reply.getMessageObject();

    if( ! ( replyObject instanceof PoolIoFileMessage ) )
    {
      throw new Exception( "Illegal Object received : "+
                     replyObject.getClass().getName());
    }

    PoolIoFileMessage poolReply = (PoolIoFileMessage)replyObject;

    if (poolReply.getReturnCode() != 0)
    {
      throw new Exception( "Pool error: "+poolReply.getErrorObject() ) ;
    }

    _log.info("Pool "+pool+" will deliver file "+pnfsId);

  }


  private String askForReadPool( PnfsId       pnfsId ,
                                 StorageInfo  storageInfo ,
                                 ProtocolInfo protocolInfo ,
                                 boolean      isWrite       ) throws Exception
  {

      //
      // ask for a pool
      //
    PoolMgrSelectPoolMsg request = isWrite ?
           (PoolMgrSelectPoolMsg)
           new PoolMgrSelectWritePoolMsg(
                    pnfsId,
                    storageInfo,
                    protocolInfo ,
                    0L                 )
           :
           (PoolMgrSelectPoolMsg)
           new PoolMgrSelectReadPoolMsg(
                    pnfsId  ,
                    storageInfo,
                    protocolInfo ,
                    0L                 );

    CellMessage reply =
             sendAndWait(
                new CellMessage(  poolManagerPath, request ) ,
                poolManagerTimeout*1000
                         );

    if( reply == null )
    {
      throw new  Exception("PoolMgrSelectReadPoolMsg timed out") ;
    }

    Object replyObject = reply.getMessageObject();

    if( ! ( replyObject instanceof  PoolMgrSelectPoolMsg ) )
    {
       throw new Exception( "Not a PoolMgrSelectPoolMsg : "+
                     replyObject.getClass().getName() ) ;
    }

    request =  (PoolMgrSelectPoolMsg)replyObject;
    _log.info("poolManagerReply = "+request);
    if( request.getReturnCode() != 0 )
    {
      throw new Exception( "Pool manager error: "+
                      request.getErrorObject() ) ;
    }

    String pool = request.getPoolName();
    _log.info("Positive reply from pool "+pool);

    return pool ;
  }
}



