/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.io.urlcopy;

import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;
import java.net.URLDecoder;

import org.globus.io.streams.GlobusInputStream;
import org.globus.io.streams.GlobusOutputStream;
import org.globus.io.streams.GlobusFileInputStream;
import org.globus.io.streams.GlobusFileOutputStream;
import org.globus.io.streams.FTPInputStream;
import org.globus.io.streams.FTPOutputStream;
import org.globus.io.streams.HTTPInputStream;
import org.globus.io.streams.HTTPOutputStream;
import org.globus.io.streams.GassInputStream;
import org.globus.io.streams.GassOutputStream;
import org.globus.io.streams.GridFTPInputStream;
import org.globus.io.streams.GridFTPOutputStream;
import org.globus.ftp.FTPClient;
import org.globus.ftp.GridFTPClient;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.Session;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.exception.FTPException;
import org.globus.util.GlobusURL;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.gsi.gssapi.auth.SelfAuthorization;

import org.ietf.jgss.GSSCredential;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * Limitations:
 * o third party transfer use binary type
 * o third party transfer negotiate DCAU - defaults to DCAU on
 * o no ability to set ascii/binary transfer type
 * o no ability to set authorization per connection
 */
public class UrlCopy implements Runnable {

    private static Log logger = 
        LogFactory.getLog(UrlCopy.class.getName());

    /** maximum buffer size to read or write when putting and getting files */
    public static final int BUFF_SIZE  = 2048;

    protected int bufferSize           = BUFF_SIZE;
    protected GSSCredential srcCreds   = null;
    protected Authorization srcAuth    = null;
    protected GSSCredential dstCreds   = null;
    protected Authorization dstAuth    = null;

    protected boolean dcau             = true;
    protected boolean appendMode       = false;
    protected GlobusURL srcUrl         = null;
    protected GlobusURL dstUrl         = null;
    protected boolean canceled         = false;
    protected boolean thirdParty       = true;
    protected List listeners           = null;
    
    protected long sourceOffset      = 0;
    protected long destinationOffset = 0;
    protected long sourceLength      = Long.MAX_VALUE;
    protected int  tcpBufferSize     = 0;
    
    protected boolean disableAllo    = false;


    public void setDCAU(boolean dcau) {
        this.dcau = dcau;
    }

    public boolean getDCAU() {
        return this.dcau;
    }

    /**
     * Sets credentials to use for both sides.
     * 
     * @param credentials user credentials
     */
    public void setCredentials(GSSCredential credentials) {
        setSourceCredentials(credentials);
        setDestinationCredentials(credentials);
    }

    /**
     * Sets source url credentials.
     *
     * @param srcCredentials source url credentials.
     */
    public void setSourceCredentials(GSSCredential srcCredentials) {
        this.srcCreds = srcCredentials;
    }

    /**
     * Sets destination url credentials.
     *
     * @param dstCredentials destination url credentials.
     */
    public void setDestinationCredentials(GSSCredential dstCredentials) {
        this.dstCreds = dstCredentials;
    }

    /**
     * Sets source authorization type
     *
     * @param auth authorization type to perform for source
     */
    public void setSourceAuthorization(Authorization auth) {
        this.srcAuth = auth;
    }

    /**
     * Sets destination authorization type
     *
     * @param auth authorization type to perform for destination
     */
    public void setDestinationAuthorization(Authorization auth) {
        this.dstAuth = auth;
    }

    /**
     * Returns credentials used for authenticating 
     * the source side for the url copy.
     * If no source credentials are set, the default 
     * user credentials will used.
     *
     * @return source credentials.
     */
    public GSSCredential getSourceCredentials() {
        return this.srcCreds;
    }

    /**
     * Returns credentials used for authenticating
     * the destination side for the url copy.
     * If no destination credentials are set, the default
     * user credentials will used.
     *
     * @return destination credentials.
     */
    public GSSCredential getDestinationCredentials() {
        return this.dstCreds;
    }

    /**
     * Returns authorization type for 
     * the source side for the url copy.
     * If no authorization type is set, the default
     * authorization will be performed for a given protocol.
     *
     * @return source authorization type
     */
    public Authorization getSourceAuthorization() {
        return this.srcAuth;
    }

    /**
     * Returns authorization type for 
     * the destination side for the url copy.
     * If no authorization type is set, the default
     * authorization will be performed for a given protocol.
     *
     * @return destination authorization type
     */
    public Authorization getDestinationAuthorization() {
        return this.dstAuth;
    }
    
    /**
     * Adds url copy listener.
     *
     * @param listener url copy listener
     */
    public void addUrlCopyListener(UrlCopyListener listener) {
        if (listeners == null) listeners = new LinkedList();
        listeners.add(listener);
    }
    
    /**
     * Remove url copy listener
     *
     * @param listener url copy listener
     */
    public void removeUrlCopyListener(UrlCopyListener listener) {
        if (listeners == null) return;
        listeners.remove(listener);
    }
    
    /**
     * Sets buffer size for transfering data.
     * It does not set the TCP buffers.
     *
     * @param size size of the data buffer
     */
    public void setBufferSize(int size) {
        bufferSize = size;
    }
    
    /**
     * Returns buffer size used for transfering
     * data.
     *
     * @return data buffer size
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Sets the TCP buffer size for GridFTP transfers.
     *
     * @param size size of TCP buffer
     */
    public void setTCPBufferSize(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("The TCP buffer size must be a positive");
        }
        this.tcpBufferSize = size;
    }
    
    /**
     * Returns TCP buffer size used for transfers
     * data.
     *
     * @return TCP buffer size
     */
    public int getTCPBufferSize() {
        return this.tcpBufferSize;
    }

    /**
     * Enables/disables append mode.
     *
     * @param appendMode if true, destination file 
     *                   will be appended.
     */
    public void setAppendMode(boolean appendMode) {
        this.appendMode = appendMode;
    }
    
    /**
     * Checks if append mode is enabled.
     *
     * @return true if appending will be performed,
     *         false otherwise.
     */
    public boolean isAppendMode() {
        return appendMode;
    }
    
       
    /**
     * Gets the offset in the destination file from which data starts
     * to be written
     * 
     * @return a value indicating the offset in bytes
     */
    public long getDestinationOffset() {
        return this.destinationOffset;
    }
    
    /**
     * Sets the offset in the destination file from which data starts
     * to be written. The default offset is 0 (the beginning of the file)
     * 
     * @param destinationOffset the offset in bytes
     */
    public void setDestinationOffset(long destinationOffset) {
        this.destinationOffset = destinationOffset;
    }
    
    /**
     * Gets the maximum data size that will be transfered.
     * 
     * @return the size in bytes
     */
    public long getSourceLength() {
        return this.sourceLength;
    }
    
    /**
     * Allows a partial transfer by setting the maximum number of bytes 
     * that will be transfered. By default the entire source file is 
     * transfered.
     * 
     * @param sourceLength the size of the transfer in bytes
     */
    public void setSourceFileLength(long sourceLength) {
        this.sourceLength = sourceLength;
    }
    
    /**
     * Gets the offset in the source file from which data starts
     * to be read
     * 
     * @return a value indicating the offset in bytes
     */
    public long getSourceOffset() {
        return this.sourceOffset;
    }
    
    /**
     * Sets the offset in the source file from which data starts
     * to be read. The default offset is 0 (the beginning of the file)
     * 
     * @param sourceOffset the offset in bytes
     */
    public void setSourceFileOffset(long sourceOffset) {
        this.sourceOffset = sourceOffset;
    }    
    
    private void checkUrl(GlobusURL url) 
        throws UrlCopyException {
        String urlPath = url.getPath();
        if (urlPath == null || urlPath.length() == 0) {
            throw new UrlCopyException("The '" + url.getURL() + 
                                       "' url does not specify the file location.");
        }
    }
    
    /**
     * Sets source url.
     *
     * @param source source url.
     */
    public void setSourceUrl(GlobusURL source) 
        throws UrlCopyException {
        if (source == null) {
            throw new IllegalArgumentException("Source url cannot be null");
        }
        checkUrl(source);
        srcUrl = source;
    }
    
    /**
     * Returns source url.
     *
     * @return url
     */
    public GlobusURL getSourceUrl() {
        return srcUrl;
    }
    
    /**
     * Sets destination url.
     *
     * @param dest destination url
     */
    public void setDestinationUrl(GlobusURL dest) 
        throws UrlCopyException {
        if (dest == null) {
            throw new IllegalArgumentException("Desitination url cannot be null");
        }
        checkUrl(dest);
        dstUrl = dest;
    }
    
    /**
     * Returns destination url.
     *
     * @return url
     */
    public GlobusURL getDestinationUrl() {
        return dstUrl;
    }

    /**
     * Enables/disables usage of third party transfers.
     *
     * @param thirdParty if true enable, false disable
     */
    public void setUseThirdPartyCopy(boolean thirdParty) {
        this.thirdParty  = thirdParty;
    }
    
    /**
     * Can be used to query whether the use of the ALLO command 
     * with GridFTP uploads is disabled. 
     */
    public boolean getDisableAllo() {
        return disableAllo;
    }
    
    /**
     * Allows disabling of the use of ALLO with GridFTP
     * uploads
     */

    public void setDisableAllo(boolean disableAllo) {
        this.disableAllo = disableAllo;
    }
    

    /**
     * Cancels the transfer in progress. If no transfer
     * is in progress it is ignored.
     */
    public void cancel() {
        canceled = true;
    }
  
    /**
     * Checks if the transfer was canceled.
     *
     * @return true if transfer was canceled
     */
    public boolean isCanceled() {
        return canceled;
    }
    
    /**
     * This method is an implementation of the {@link Runnable} interface
     * and can be used to perform the copy in a separate thread.
     * <p>
     * This method will perform the transfer and signal completion and 
     * errors through the {@link UrlCopyListener#transferCompleted()} and 
     * {@link UrlCopyListener#transferError(Exception)} of any registered listeners
     * (see {@link addUrlCopyListener}). 
     *  
     */     
    public void run() {
        try {
            copy();
        } catch(Exception e) {
            if (listeners != null) {
                Iterator iter = listeners.iterator();
                while(iter.hasNext()) {
                    ((UrlCopyListener)iter.next()).transferError(e);
                }
            }
        } finally {
            if (listeners != null) {
                Iterator iter = listeners.iterator();
                while(iter.hasNext()) {
                    ((UrlCopyListener)iter.next()).transferCompleted();
                }
            }
        }
    }
    
    /**
     * Performs the copy function.
     * Source and destination urls must be specified otherwise
     * a exception is thrown. Also, if source and destination url
     * are ftp urls and thirdPartyCopy is enabled, third party transfer
     * will be performed. Urls, of course, must be of supported protocol.
     * Currently, gsiftp, ftp, https, http, and file are supported.
     * <p>
     * This method does not cause the {@link UrlCopyListener#transferCompleted()}
     * and {@link UrlCopyListener#transferError(Exception)} to be called. If you want
     * completion/failures to be signaled asynchronously, either call the
     * {@link #run} method or wrap this object in a {@link Thread}. 
     * 
     * @throws UrlCopyException in case of an error.
     */
    public void copy() 
        throws UrlCopyException {
        
        if (srcUrl == null) {
            throw new UrlCopyException("Source url is not specified");
        }
        
        if (dstUrl == null) {
            throw new UrlCopyException("Destination url is not specified");
        }
         
        String fromP  = srcUrl.getProtocol();
        String toP    = dstUrl.getProtocol();
        
        if (thirdParty && fromP.endsWith("ftp") && toP.endsWith("ftp")) {
            thirdPartyTransfer();
            return;
        }
         
        GlobusInputStream in   = null;
        GlobusOutputStream out = null;
        boolean rs             = false;
        
        try {
            in = getInputStream();
           
            long size = in.getSize();
           
            if (size == -1) {
                logger.debug("Source size: unknown");
            } else {
                logger.debug("Source size: " + size);
            }
           
            out = getOutputStream(size);
           
            rs = transfer(size, in, out);
           
            in.close();
            out.close();
           
        } catch(Exception e) {
            if (out != null) out.abort();
            if (in  != null) in.abort();
            throw new UrlCopyException("UrlCopy transfer failed.", e);
        }

        if (!rs && isCanceled()) {
            throw new UrlCopyException("Transfer Aborted");
        }
    }
    
    
    /**
     * Returns input stream based on the source url
     */
    protected GlobusInputStream getInputStream() 
        throws Exception {
        
        GlobusInputStream in = null;
        String fromP         = srcUrl.getProtocol();
        String fromFile      = srcUrl.getPath();
        
        if (fromP.equalsIgnoreCase("file")) {
            fromFile = URLDecoder.decode(fromFile);
            in = new GlobusFileInputStream(fromFile);
        } else if (fromP.equalsIgnoreCase("ftp")) {
            fromFile = URLDecoder.decode(fromFile);
            in = new FTPInputStream(srcUrl.getHost(),
                                    srcUrl.getPort(),
                                    srcUrl.getUser(),
                                    srcUrl.getPwd(),
                                    fromFile);
        } else if (fromP.equalsIgnoreCase("gsiftp") ||
                   fromP.equalsIgnoreCase("gridftp")) {
            Authorization auth = getSourceAuthorization();
            if (auth == null) {
                auth = HostAuthorization.getInstance();
            }
            fromFile = URLDecoder.decode(fromFile);
            in = new GridFTPInputStream(getSourceCredentials(),
                                        auth,
                                        srcUrl.getHost(),
                                        srcUrl.getPort(),
                                        fromFile,
                                        getDCAU());
            
        } else if (fromP.equalsIgnoreCase("https")) {
            Authorization auth = getSourceAuthorization();
            if (auth == null) {
                auth = SelfAuthorization.getInstance();
            }
            in = new GassInputStream(getSourceCredentials(), 
                                     auth,
                                     srcUrl.getHost(),
                                     srcUrl.getPort(),
                                     fromFile);
        } else if (fromP.equalsIgnoreCase("http")) {
            in = new HTTPInputStream(srcUrl.getHost(),
                                     srcUrl.getPort(),
                                     fromFile);
        } else {
            throw new Exception("Source protocol: " + fromP + 
                                " not supported!");
        }
        
        return in;
    }
    
    /**
     * Returns output stream based on the destination url.
     */
    protected GlobusOutputStream getOutputStream(long size) 
        throws Exception {

        GlobusOutputStream out = null;
        String toP             = dstUrl.getProtocol();
        String toFile          = dstUrl.getPath();
        
        if (toP.equalsIgnoreCase("file")) {
            toFile = URLDecoder.decode(toFile);
            out = new GlobusFileOutputStream(toFile, appendMode);
        } else if (toP.equalsIgnoreCase("ftp")) {
            toFile = URLDecoder.decode(toFile);
            out = new FTPOutputStream(dstUrl.getHost(),
                                      dstUrl.getPort(),
                                      dstUrl.getUser(),
                                      dstUrl.getPwd(),
                                      toFile,
                                      appendMode);
        } else if (toP.equalsIgnoreCase("gsiftp") ||
                   toP.equalsIgnoreCase("gridftp")) {
            Authorization auth = getDestinationAuthorization();
            if (auth == null) {
                auth = HostAuthorization.getInstance();
            }
            toFile = URLDecoder.decode(toFile);
            out = new GridFTPOutputStream(getDestinationCredentials(),
                                          auth,
                                          dstUrl.getHost(),
                                          dstUrl.getPort(),
                                          toFile,
                                          appendMode,
                                          getDCAU(),
                                          (disableAllo ? -1 : size));
        } else if (toP.equalsIgnoreCase("https")) {
            Authorization auth = getDestinationAuthorization();
            if (auth == null) {
                auth = SelfAuthorization.getInstance();
            }
            out = new GassOutputStream(getDestinationCredentials(),
                                       auth,
                                       dstUrl.getHost(),
                                       dstUrl.getPort(),
                                       toFile,
                                       size,
                                       appendMode);
        } else if (toP.equalsIgnoreCase("http")) {
            out = new HTTPOutputStream(dstUrl.getHost(),
                                       dstUrl.getPort(),
                                       toFile,
                                       size,
                                       appendMode);
        } else {
            throw new Exception("Destination protocol: " + toP + 
                                " not supported!");
        }
        
        return out;
    }
    
    /**
     * This function performs the actual transfer.
     */
    private boolean transfer(long total, 
                             GlobusInputStream in, 
                             GlobusOutputStream out)
        throws IOException {
        
        byte [] buffer       = new byte[bufferSize];
        int bytes            = 0;
        long totalBytes      = total;
        long transferedBytes = 0;
        
        if (total == -1) {
            while( (bytes = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytes);
                out.flush();
                
                if (listeners != null) {
                    transferedBytes += bytes;
                    fireUrlTransferProgressEvent(totalBytes, 
                                                 transferedBytes);
                }
                if (isCanceled()) return false;
            }
        } else {
            while ( total != 0 ) {                     
                
                bytes = bufferSize;
                if (total < bufferSize) bytes = (int)total;
                
                bytes = in.read(buffer);
                out.write(buffer, 0, bytes);
                out.flush();
                total -= bytes;
                
                if (listeners != null) {
                    transferedBytes += bytes;
                    fireUrlTransferProgressEvent(totalBytes, 
                                                 transferedBytes);
                }
                if (isCanceled()) return false;
            }
        }
        
        return true;
    }
    
    private void fireUrlTransferProgressEvent(long totalBytes, 
                                              long transferedBytes) {
        Iterator iter = listeners.iterator();
        while(iter.hasNext()) {
            ((UrlCopyListener)iter.next()).transfer(transferedBytes,
                                                    totalBytes);
        }
    }
    
    /**
     * This performs thrid party transfer only if source and destination urls
     * are ftp urls.
     */
    private void thirdPartyTransfer() 
        throws UrlCopyException {
        
        logger.debug("Trying third party transfer...");
        
        FTPClient srcFTP = null;
        FTPClient dstFTP = null;
        
        try {
            srcFTP = createFTPConnection(srcUrl, true);
            
            dstFTP = createFTPConnection(dstUrl, false);

            negotiateDCAU(srcFTP, dstFTP);
            
            srcFTP.setType(Session.TYPE_IMAGE);
            dstFTP.setType(Session.TYPE_IMAGE);
        
            if (listeners != null) {
                fireUrlTransferProgressEvent(-1, -1);
            }
            
            if (this.sourceOffset == 0 && 
                this.destinationOffset == 0 && 
                this.sourceLength == Long.MAX_VALUE) {
                
                srcFTP.setMode(Session.MODE_STREAM);
                dstFTP.setMode(Session.MODE_STREAM);
                   
                srcFTP.transfer(srcUrl.getPath(), 
                                dstFTP, 
                                dstUrl.getPath(), 
                                false, 
                                null);
            } else if (srcFTP instanceof GridFTPClient && 
                       dstFTP instanceof GridFTPClient) {
                
                GridFTPClient srcGridFTP = (GridFTPClient) srcFTP;
                GridFTPClient dstGridFTP = (GridFTPClient) dstFTP;
                                        
                srcGridFTP.setMode(GridFTPSession.MODE_EBLOCK);
                dstGridFTP.setMode(GridFTPSession.MODE_EBLOCK);
                                        
                srcGridFTP.extendedTransfer(srcUrl.getPath(),
                                            this.sourceOffset,
                                            this.sourceLength,
                                            dstGridFTP,
                                            dstUrl.getPath(),
                                            this.destinationOffset,
                                            null);
            } else {
                throw new UrlCopyException("Partial 3rd party transfers not supported " +
                                           "by FTP client. Use GridFTP for both source and destination.");                             
            }
        } catch(Exception e) {
            throw new UrlCopyException("UrlCopy third party transfer failed.",
                                       e);
        } finally {
            if (srcFTP != null) {
                try { srcFTP.close(); } catch (Exception ee) {}
            }
            if (dstFTP != null) {
                try { dstFTP.close(); } catch (Exception ee) {}
            }
        }
    }   

    /*
     * This could replaced later with something more inteligent
     * where the user would set if dcau is required or not, etc.
     */
    protected void negotiateDCAU(FTPClient src, FTPClient dst) 
        throws IOException, FTPException {
        if (src instanceof GridFTPClient) {
            // src: dcau can be on or off
            if (dst instanceof GridFTPClient) {
                // dst: dca can be on or off
                GridFTPClient s = (GridFTPClient)src;
                GridFTPClient d = (GridFTPClient)dst;

                if (src.isFeatureSupported("DCAU") &&
                    dst.isFeatureSupported("DCAU")) {
                    
                    setDCAU(s, getDCAU());
                    setDCAU(d, getDCAU());
                    
                } else {
                    setDCAU(s, false);
                    setDCAU(d, false);
                    setDCAU(false);
                }
                
            } else {
                // dst: no dcau supported - disable src
                setDCAU((GridFTPClient)src, false);
                setDCAU(false);
            }
        } else {
            // src: no dcau
            if (dst instanceof GridFTPClient) {
                // dst: just disable dcau
                setDCAU((GridFTPClient)dst, false);
            } else {
                // dst: no dcau
                // we are all set then
            }
            setDCAU(false);
        }
    }

    private static void setDCAU(GridFTPClient c, boolean dcau) 
        throws IOException, FTPException {
        if (c.isFeatureSupported("DCAU")) {
            if (!dcau) {
                c.setDataChannelAuthentication(DataChannelAuthentication.NONE);
            }
        } else if (dcau) {
            throw new IOException("DCAU not supported but DCAU requested");
        }
    }

    /**
     * Creates ftp connection based on the ftp url (secure vs. unsecure)
     */
    private FTPClient createFTPConnection(GlobusURL ftpURL, boolean srcSide)
        throws Exception {

        String protocol = ftpURL.getProtocol();

        if (protocol.equalsIgnoreCase("ftp")) {
            
            FTPClient ftp = new FTPClient(ftpURL.getHost(),
                                          ftpURL.getPort());
            ftp.authorize(ftpURL.getUser(),
                          ftpURL.getPwd());
            
            return ftp;
            
        } else {
            
            GridFTPClient ftp = new GridFTPClient(ftpURL.getHost(),
                                                  ftpURL.getPort());
            
            if (srcSide) {
                Authorization auth = getSourceAuthorization();
                if (auth == null) {
                    auth = HostAuthorization.getInstance();
                }
                ftp.setAuthorization(auth);
                ftp.authenticate(getSourceCredentials());
            } else {
                Authorization auth = getDestinationAuthorization();
                if (auth == null) {
                    auth = HostAuthorization.getInstance();
                }
                ftp.setAuthorization(auth);
                ftp.authenticate(getDestinationCredentials());
            }
            
            if (tcpBufferSize != 0) {
                ftp.setTCPBufferSize(tcpBufferSize);
            }
            
            return ftp;
        }
    }
    
}
