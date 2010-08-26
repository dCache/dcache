/*
 * ConcreteTransferManager.java
 *
 * Created on November 12, 2004, 12:22 PM
 */

package diskCacheV111.services;

import dmg.cells.nucleus.*;
import dmg.cells.network.*;
import dmg.util.*;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsFile;
import org.globus.util.GlobusURL;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolSetStickyMessage;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Iterator;

/**
 *
 * @author  timur
 */
public class GsiftpTransferManager extends TransferManager{

    public boolean _messageArrived(CellMessage cellMessage ) {
       Object object = cellMessage.getMessageObject();

       	if (! (object instanceof Message) ){
	    say("Unexpected message class "+object.getClass());
	    return false;
	}

        Message transferMessage = (Message)object ;

        if(object instanceof RemoteGsiftpDelegateUserCredentialsMessage)
        {
            try
            {
                hanldeDelegateMessage((RemoteGsiftpDelegateUserCredentialsMessage)object);
                return true;
            }
            catch(Exception e)
            {
                esay(e);
                return false;
            }
        }
        return false;
    }

    protected IpProtocolInfo getProtocolInfo(long callerId,
        diskCacheV111.vehicles.transferManager.TransferManagerMessage transferRequest)
        throws java.io.IOException
    {
        RemoteGsiftpTransferManagerMessage remGsiftpTransferRequest =
            (RemoteGsiftpTransferManagerMessage) transferRequest;



        String  gridftpurlString = transferRequest.getRemoteURL();
        GlobusURL gsiftpurl = new GlobusURL(transferRequest.getRemoteURL());
        String path = gsiftpurl.getPath();
        if(!path.startsWith("/")) {
            int indx =gridftpurlString.length() -path.length();
            gridftpurlString = gridftpurlString.substring(0,indx-1);
            gridftpurlString = gridftpurlString+"//"+path;

        }
        String remoteHost =  gsiftpurl.getHost();
        InetAddress[] addresses = InetAddress.getAllByName( remoteHost );
        String[] hosts = new String[addresses.length];
         for(int i = 0; i<addresses.length; ++i)
         {
             hosts[i] = addresses[i].getHostName();
         }

        RemoteGsiftpTransferProtocolInfo protocolInfo =
        new RemoteGsiftpTransferProtocolInfo(
            "RemoteGsiftpTransfer",
            1,1,hosts,0,
            gridftpurlString,
            _nucleus.getCellName(),
            _nucleus.getCellDomainName(),
            transferRequest.getId(),
            callerId,
            remGsiftpTransferRequest.getBufferSize(),
            remGsiftpTransferRequest.getTcpBufferSize(),
            remGsiftpTransferRequest.getCredentialId());
        protocolInfo.setEmode(remGsiftpTransferRequest.isEmode());
        protocolInfo.setStreams_num(remGsiftpTransferRequest.getStreams_num());
        return   protocolInfo;

    }


    /** Creates a new instance of ConcreteTransferManager */
    public GsiftpTransferManager(String cellName, String argString) throws Exception {
        super(cellName,argString);
    }

    private void hanldeDelegateMessage(RemoteGsiftpDelegateUserCredentialsMessage deleg_req) throws Exception {
        long id = deleg_req.getId();
        TransferManagerHandler h = getHandler(id);
        if(h == null) {
            esay("hanldeDelegateMessage: can't fid handler # "+id);
            return;
        }
        CellPath src_cell_path = h.getRequestSourcePath();
        say("forwarding delegate crededtial message message to "+src_cell_path );
	sendMessage(new CellMessage(src_cell_path,deleg_req));

    }
}
