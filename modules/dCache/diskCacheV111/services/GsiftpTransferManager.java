/*
 * ConcreteTransferManager.java
 *
 * Created on November 12, 2004, 12:22 PM
 */

package diskCacheV111.services;

import org.globus.util.GlobusURL;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author  timur
 */
public class GsiftpTransferManager extends TransferManager{
    private static final Logger log = LoggerFactory.getLogger(GsiftpTransferManager.class);

    public boolean _messageArrived(CellMessage cellMessage ) {
       Object object = cellMessage.getMessageObject();

       	if (! (object instanceof Message) ){
            log.debug("Unexpected message class "+object.getClass());
            return false;
        }
       
        if(object instanceof RemoteGsiftpDelegateUserCredentialsMessage)
        {
            try
            {
                hanldeDelegateMessage((RemoteGsiftpDelegateUserCredentialsMessage)object);
                return true;
            }
            catch(Exception e)
            {
                log.error(e.toString());
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
            getCellName(),
            getCellDomainName(),
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
            log.error("hanldeDelegateMessage: can't fid handler # "+id);
            return;
        }
        CellPath src_cell_path = h.getRequestSourcePath();
        log.debug("forwarding delegate crededtial message message to "+src_cell_path );
	sendMessage(new CellMessage(src_cell_path,deleg_req));

    }
}
