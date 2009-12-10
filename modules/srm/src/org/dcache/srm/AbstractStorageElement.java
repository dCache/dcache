//$Id$
//$Log: not supported by cvs2svn $
//Revision 1.30  2007/03/10 00:13:19  timur
//started work on adding support for optional overwrite
//
//Revision 1.29  2007/02/10 04:46:13  timur
// first version of SrmExtendFileLifetime
//
//Revision 1.27  2007/01/10 23:00:22  timur
//implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
//Revision 1.26  2006/11/14 22:37:13  timur
//getSpaceTokens implementation
//
//Revision 1.25  2006/11/10 22:57:03  litvinse
//introduced setFileMetadata function to prapare for SrmSetPermission
//
//Revision 1.24  2006/11/09 22:34:08  timur
//implementation of SrmGetSpaceMetaData function
//
//Revision 1.23  2006/10/10 20:59:56  timur
//more changes for srmBringOnline
//
//Revision 1.22  2006/08/18 22:05:31  timur
//srm usage of space by srmPrepareToPut implemented
//
//Revision 1.21  2006/08/02 22:03:12  timur
//more work for space management
//
//Revision 1.20  2006/08/01 00:09:50  timur
//more space reservation code
//
//Revision 1.19  2006/07/29 18:10:40  timur
//added schedulable requests for execution reserve space requests
//
//Revision 1.18  2006/06/23 21:12:17  timur
//use correct transfer request ids in srm copy file request, use request credential id  to refernce delegated credential
//
//Revision 1.17  2006/06/20 15:42:15  timur
//initial v2.2 commit, code is based on a week old wsdl, will update the wsdl and code next
//
//Revision 1.16  2005/11/16 22:13:37  litvinse
//implemented Mv
//
//Revision 1.15  2005/11/15 01:10:42  litvinse
//implemented SrmMkdir function
//
//Revision 1.14  2005/11/14 02:17:07  litvinse
//redo removeDirectory function so it is not asynchronous
//
//Revision 1.13  2005/11/12 22:15:35  litvinse
//implemented SrmRmDir
//
//WARNING: if directory is sym-link or recursion level is specified an
//	 a subdirectory contains sym-link - it will follow it. Do not
//	 use if there are symbolic link.
//
//Revision 1.12  2005/11/10 22:58:57  timur
//better faster srm ls in non verbose mode
//
//Revision 1.11  2005/11/09 23:56:48  timur
//srm ls related improvements
//
//Revision 1.10  2005/11/01 17:07:16  litvinse
//implemented SrmRm
//
//Revision 1.9  2005/10/07 22:57:15  timur
//work for srm v2
//
//Revision 1.8  2005/07/22 17:32:54  leoheska
//srm-ls modifications
//
//Revision 1.7  2005/06/22 22:11:28  timur
//added extentions to globus plugins for gridifying tomcat, which removed dependency on grid-mapfile
//
//Revision 1.6  2005/06/06 21:59:03  leoheska
//Added srm-ls functionality
//
//Revision 1.5  2005/03/23 18:10:37  timur
//more space reservation related changes, need to support it in case of "copy"
//
//Revision 1.4  2005/03/11 21:16:24  timur
//making srm compatible with cern tools again
//
//Revision 1.3  2005/03/07 22:55:33  timur
//refined the space reservation call, restored logging of sql commands while debugging the sql performance
//
//Revision 1.2  2005/03/01 23:10:38  timur
//Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
//Revision 1.1  2005/01/14 23:07:13  timur
//moving general srm code in a separate repository
//
//Revision 1.20  2004/12/02 05:30:20  timur
//new GsiftpTransferManager
//
//Revision 1.19  2004/08/06 19:35:21  timur
//merging branch srm-branch-12_May_2004 into the trunk
//
//Revision 1.18.2.16  2004/07/29 22:17:29  timur
//Some functionality for disk srm is working
//
//Revision 1.18.2.15  2004/06/30 22:11:05  cvs
//More docs
//
//Revision 1.18.2.14  2004/06/22 17:04:58  cvs
//More docs
//
//Revision 1.18.2.13  2004/06/22 16:22:20  cvs
//More typos correction
//
//Revision 1.18.2.12  2004/06/22 15:17:55  cvs
//Typos corrected
//
//Revision 1.18.2.11  2004/06/15 22:46:34  cvs
//More docs
//
//Revision 1.18.2.10  2004/06/15 21:55:27  timur
//added cvs logging tags at the top
//

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
 * AbstractStorageElement.java
 *
 * Created on January 10, 2003, 12:37 PM
 */

package org.dcache.srm;


import org.dcache.srm.FileMetaData;
import diskCacheV111.srm.StorageElementInfo;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import java.util.Vector;
import org.dcache.srm.v2_2.TMetaDataSpace;


/**
 * This interface has to be implemented for each Storage Element the user wants
 * to work with. The Storage Element can support a different set of protocols for
 * put and get operations. Before any get/put operations user has to get the list of
 * supported get/put protocols.
 * THe sequence of actions to process get request:
 * 
 * 1) call supportedGetProtocols() to get the list of supported protocols
 * 
 * 2) call getFileInfo(SRMUser user, String filePath, GetFileInfoCallbacks callbacks)
 * The result will be returned thru callbacks
 * GetFileInfoCallbacks is one of the interfaces to be implemented for Storage Element
 * The most important method is:
 *      public void StorageInfoArrived(String fileId, FileMetaData fileMetaData);
 *      FileId is unique ID for the file inside Storage Element
 *      fileId is persistent in SRM
 *      fileMetaData -- is not guaranteed to be persistent
 * 
 *  To be continued...
 * 
 * @author timur
 */
public interface AbstractStorageElement extends Logger{
    
    /**
     * Method must be called before any "get" operations. It gives the list of all
     * protocols implemented in the Storage Element for "get" operations
     * @return Array of strings with protocol names.
     * @throws SRMException in case of error
     */
    public String[] supportedGetProtocols() throws SRMException;
    
    /** Method must be called before any "put" operations. It gives the list of all
     * protocols implemented in the Storage Element for "put" operations
     * @throws SRMException
     * @return Array of strings with protocol names.
     */
    public String[] supportedPutProtocols()throws SRMException;
    
    /** This method has to be called to get the transport URL for file operation.
     * The returned value is passed to the user and user does actual data transfer
     * @param user User ID
     * @param filePath File path
     * @param protocols List of SE supported protocols
     * @throws SRMException
     * @return Transport URL for file operation
     */
    public String getPutTurl(SRMUser user,String filePath,
            String[] protocols)
            throws SRMException;
    
    /** To accomodate the property of dcache that requires that the same client get all
     * dcap transfers though the same dcap door, we put the get(Get/Put)Turl which has
     * this siganture.
     * The previous_turl is the turl that was obtained by the client before.
     * @param user User ID
     * @param filePath File path
     * @param previous_turl The transport URL received from the previous call of getPutTurl
     * @throws SRMException
     * @return Transport URL for file operation
     */
    public String getPutTurl(SRMUser user,String filePath,
            String previous_turl)
            throws SRMException;
    
    /** This method has to be called to get the transport URL for file operation.
     * The returned value is passed to the user and user does actual data transfer
     * @param user User ID
     * @param filePath File path
     * @param protocols
     * @throws SRMException
     * @return Transport URL for file operation
     */
    public String getGetTurl(SRMUser user,String filePath,
            String[] protocols)
            throws SRMException;
    
    /** To accomodate the property of dcache that requires that the same client get all
     * dcap transfers though the same dcap door, we put the get(Get/Put)Turl which has
     * this siganture.
     * The previous_turl is the turl that was obtained by the client before.
     * @param user User ID
     * @param filePath File path
     * @param previous_turl The transport URL received from the previous call of getPutTurl
     * @throws SRMException
     * @return Transport URL for file operation
     */
    public String getGetTurl(SRMUser user, String filePath,
            String previous_turl)
            throws SRMException;
    
    /** This method discovers the info about the requested file and
     * returns the info via GetFileInfoCallbacks interface
     * Method must be nonblocking - when called it creates thread and returns
     * immediately.
     * @param user User ID
     * @param filePath Actual file path
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to get file info from the storage
     */
    public void getFileInfo(SRMUser user, String filePath,
            GetFileInfoCallbacks callbacks);
    
    /** Method must be nonblocking -- when called it creates thread and returns immediately,
     *         result will be sent thru callbacks
     * @param user User ID
     * @param filePath File path
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * @param overwrite allow overwrite if true
     * various actions performed to put file from the storage
     */
    public void prepareToPut(SRMUser user, String filePath,
            PrepareToPutCallbacks callbacks,
            boolean overwrite);
    
    /** This method allows to pin file in the Storage Element,
     * i.e. put the file in "fast access state"
     * @param user User ID
     * @param fileId Storage Element internal file ID
     * @param network address from which file will be read
     *        null, if unknown
     * @param fmd File metadata returned by getFileMetaData
     * @param pinLifetime Requested pin operation lifetime in millis
     * @param requestId - ping will save request id
     *        so that unping by file name and request id can take place
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to "pin" file in the storage
     */
    

    public void pinFile(SRMUser user,
           String fileId,
           String clientHost,
           FileMetaData fmd, 
           long pinLifetime,
           long requestId,
           PinCallbacks callbacks);
    
    /**
     * @param user User ID
     * @param pinId Id of a valid pin
     * @param newPinLifetime new lifetime in millis to assign to pin
     * @return long lifetime left for pin in millis
     */
    
    public long extendPinLifetime(SRMUser user, String fileId, String pinId, long newPinLifetime)
    throws SRMException ;
   
    /**
     * @param user User ID
     * @param path File path
     * @param size
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to put file into reserved space in the storage
     */
    public void prepareToPutInReservedSpace(SRMUser user, String path,  long size,
            long spaceReservationToken,    PrepareToPutInSpaceCallbacks callbacks);
    
    
    /**
     *this method perform a transfer from the remote transfer url to the local file, specified by actualFilePath
     * this method can return the string identifier of the pending transfer, and then notify about the
     * completeon of the transfer asynchronously, though the callbacks interface
     *
     *
     * @param user User ID
     * @param remoteTURL
     * @param actualFilePath
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return transfer id
     *  an id to the pending tranfer that can be used to cancel the transfer via killRemoteTransfer
     */
    public String getFromRemoteTURL(
            SRMUser user,
            String remoteTURL,
            String actualFilePath,
            SRMUser remoteUser,
            Long requestCredentialId,
            CopyCallbacks callbacks)
            throws SRMException;
    
    /**
     *this method perform a transfer from the remote transfer url to the local file, specified by actualFilePath
     * this method can return the string identifier of the pending transfer, and then notify about the
     * completeon of the transfer asynchronously, though the callbacks interface
     * this variant is used when the size of the file to be transfered is known and the
     * space has been reserved
     *
     *
     * @param user User ID
     * @param remoteTURL
     * @param actualFilePath
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return transfer id
     *  an id to the pending tranfer that can be used to cancel the transfer via killRemoteTransfer
     */
    public String getFromRemoteTURL(
            SRMUser user,
            String remoteTURL,
            String actualFilePath,
            SRMUser remoteUser,
            Long requestCredentialId,
            String spaceReservationId,
            long size,
            CopyCallbacks callbacks)
            throws SRMException;
    
    /**
     *this method perform a transfer from the local file to the remote transfer url, specified by actualFilePath
     * this method can return the string identifier of the pending transfer, and then notify about the
     * completeon of the transfer asynchronously, though the callbacks interface
     *
     * @param user User ID
     * @param actualFilePath
     * @param remoteTURL
     * @param remoteUser
     * @param callbacks
     * @param remoteCredetial
     * @throws SRMException
     * @return transfer id
     *    an id to the pending tranfer that can be used to cancel the transfer via killRemoteTransfer
     */
    public String putToRemoteTURL(SRMUser user,
            String actualFilePath,
            String remoteTURL,
            SRMUser remoteUser,
            Long requestCredentialId,
            CopyCallbacks callbacks)
            throws SRMException;
    
    /**
     * while the copy is in progress, this method would call the transfer to be canceled
     * this should lead to the invocation of the copyFailed method CopyCallbacks interfaced
     */
    public void killRemoteTransfer(String transferId);
    
    
    /**
     * @param user User ID
     * @param actualFromFilePath
     * @param actualToFilePath
     * @throws SRMException
     */
    public void localCopy(SRMUser user,String actualFromFilePath, String actualToFilePath)
    throws SRMException;
    
    /**
     * @param url User ID
     * @throws SRMException
     * @return
     */
    public boolean isLocalTransferUrl(String url) throws SRMException;
    
    /**
     * @param user User ID
     * @param path
     * @return
     */
    public FileMetaData getFileMetaData(SRMUser user,String path) throws SRMException;
    
    /**
     * @param user User ID
     * @param path
     * @return
     */

    public void setFileMetaData(SRMUser user,FileMetaData fmd) throws SRMException;
    
    /**
     * @param user User ID
     * @param path
     * @return
     */
    public FileMetaData getFileMetaData(SRMUser user,String path,FileMetaData parentFMD) throws SRMException;
    
    /** This method allows to unpin file in the Storage Element,
     * i.e. cancel the request to have the file in "fast access state"
     * @param user User ID
     * @param fileId Storage Element internal file ID
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to "unpin" file in the storage
     * @param pinId Unique id received during pinFile operation (?)
     */
    public void unPinFile(SRMUser user,String fileId,UnpinCallbacks callbacks,String pinId);
    
    /** This method allows to unpin file in the Storage Element,
     * i.e. cancel the request to have the file in "fast access state"
     * @param user User ID
     * @param fileId Storage Element internal file ID
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to "unpin" file in the storage
     * @param srmRequestId id given to the storage  during pinFile operation 
     */
    public void unPinFileBySrmRequestId(SRMUser user,String fileId,
            UnpinCallbacks callbacks,
            long srmRequestId);
    /** Unpin all pins on this file that user has permission to unpin
     * @param user Authorization Record of the user
     * @param fileId Storage Element internal file ID
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to "unpin" file in the storage
     */
    public void unPinFile(SRMUser user,String fileId,
            UnpinCallbacks callbacks);
    
    /** This method tells SE that the specified file can be removed from the storage.
     * This is up to SE to decide when the file will be deleted
     * @param user User ID
     * @param path File path
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to remove file from the storage
     */
    public void advisoryDelete(SRMUser user, String path,AdvisoryDeleteCallbacks callbacks);
    
    /**
     *
     * @param user User ID
     * @param path File path
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to remove file from the storage
     */
    public void removeFile(SRMUser user, String path,RemoveFileCallbacks callbacks);
    
    /**
     *
     * @param user User ID
     * @param path File path
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to remove file from the storage
     */
    
    public void removeDirectory(SRMUser user,
            Vector tree) throws SRMException;
    
    /**
     * 
     * @param user 
     * @param directory 
     * @throws org.dcache.srm.SRMException 
     */
    public void createDirectory(SRMUser user,
            String directory) throws SRMException;
    
    /**
     * 
     * @param user 
     * @param from 
     * @param to 
     * @throws org.dcache.srm.SRMException 
     */
    public void moveEntry(SRMUser user, String from,
            String to) throws SRMException;
    
    
    /** This method tells if the specified file can be read
     * @param user User ID
     * @param fileId SE internal file ID
     * @param fmd File metadata received from getFileMetaData
     * @return Boolean T/F
     */
    public boolean canRead(SRMUser user,String fileId,FileMetaData fmd);
    
    /** This method tells if the specified file can be written
     * @param user User ID
     * @param fileId SE internal file ID
     * @param fmd File metadata received from getFileMetaData
     * @param parentFileId SE internal parent directory ID
     * @param parentFmd Directory metadata received from getFileMetaData
     * 2param overwrite is true, allow overwrite of existing files
     * @return Boolean T/F
     */
    public boolean canWrite(SRMUser user,
            String fileId,
            FileMetaData fmd,
            String parentFileId, 
            FileMetaData parentFmd,
            boolean overwrite);
    
    /** This method returns via callbacks the size of the
     * space reserved in the "pool" (pool is a space storage part that can be
     * utilized in a continuous manner. If pool says it has 100GB of free space,
     * than it can accomodate a single 100GB file. In case of dcache pool is a
     * dcache pool) and the name (unique string  id) of the pool.
     * path is the root of the file path that will be written into the reserved
     * space
     * @param user User ID
     * @param spaceSize Requested space
     * @param filename name of the file to be put in this space
     * @param host name of the host from which the transfer will be performed
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     *
     * various actions performed to reserve space in the storage
     */
    
    
    
    /**
     * This was used for explicit space reservation, not a new type of reservation
     * Reserves spaceSize bytes of the space for storage of file with the path filename
     * for future transfer from the host. <br>
     * The storage will invoke methods of the callback interface
     * to syncronously notify of the srm about the results of the reservation.
     * @param user User ID
     * @param spaceSize size of the space to be released
     * @param reservationLifetime lifetime of reservation in milliseconds
     * @param host - name of the host from which the client connects
     * @param callbacks - inteface used by the storage  for feedback
     *    to the srm about progress of reservation
     * @see org.dcache.srm.ReserveSpaceCallbacks
     */
    public void reserveSpace(
            SRMUser user,
            long spaceSize,
            long reservationLifetime,
            String filename,
            String host,
            ReserveSpaceCallbacks callbacks);
    
    /**
     * This was used for explicit space reservation, not a new type of reservation
     * Release spaceSize bytes of the reserved space identified with the token
     * This method returns via callbacks the size of the
     * space released in the "pool" (pool is a space storage part that can be
     * utilized in a continuous manner. In case of dcache pool is a
     * dcache pool) and the name (unique string  id) of the pool.
     * @param user User ID
     * @param spaceSize size of the space to be released
     * @param reservationToken identifier of the space
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to release space in the storage
     */
    public void releaseSpace( SRMUser user, long spaceSize, String spaceToken, ReleaseSpaceCallbacks callbacks);
    
    /**
     * This was used for explicit space reservation, not a new type of reservation
     * Release all of the space identified with the token
     * This method returns via callbacks the size of the
     * space released in the "pool" (pool is a space storage part that can be
     * utilized in a continuous manner. In case of dcache pool is a
     * dcache pool) and the name (unique string  id) of the pool.
     * @param user User ID
     * @param reservationToken identifier of the space
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to release space in the storage
     */
    public void releaseSpace( SRMUser user,  String spaceToken, ReleaseSpaceCallbacks callbacks);
    
     
    /** This method returns the information about the Storage Element
     * @param user User ID
     * @throws SRMException
     * @return StorageElementInfo object contained information about the Storage Element
     */
    public StorageElementInfo getStorageElementInfo(SRMUser user) throws SRMException;
    
    /**
     * 
     * @param user 
     * @param directoryName 
     * @throws org.dcache.srm.SRMException 
     * @return 
     */
    public String[] listNonLinkedDirectory(SRMUser user,String directoryName) throws SRMException;
    /**
     * 
     * @param user 
     * @param directoryName 
     * @param fileMetaData 
     * @throws org.dcache.srm.SRMException 
     * @return 
     */
    public String[] listDirectory(SRMUser user,String directoryName,FileMetaData fileMetaData) throws SRMException;
    /**
     * 
     * @param user 
     * @param directoryName 
     * @param fileMetaData 
     * @throws org.dcache.srm.SRMException 
     * @return 
     */
    public java.io.File[] listDirectoryFiles(SRMUser user,String directoryName,FileMetaData fileMetaData) throws SRMException;
    
    /**
     * 
     * @param user 
     * @param sizeInBytes 
     * @param spaceReservationLifetime 
     * @param retentionPolicy 
     * @param accessLatency 
     * @param description 
     * @param callbacks 
     */
    public void srmReserveSpace(SRMUser user,
            long sizeInBytes,
            long spaceReservationLifetime,
            String retentionPolicy,
            String accessLatency,
            String description,
            SrmReserveSpaceCallbacks callbacks);
    
    /**
     * 
     * @param user 
     * @param spaceToken 
     * @param sizeInBytes 
     * @param callbacks 
     */
    public void srmReleaseSpace(SRMUser user,
            String spaceToken,
            Long sizeInBytes,
            SrmReleaseSpaceCallbacks callbacks);
            
    /**
     * 
     * @param user 
     * @param spaceToken 
     * @param fileName 
     * @param sizeInBytes 
     * @param useLifetime 
     * @param callbacks 
     */
    public void srmMarkSpaceAsBeingUsed(SRMUser user,
            String spaceToken,
            String fileName,
            long sizeInBytes,
            long useLifetime,
            boolean overwrite,
            SrmUseSpaceCallbacks callbacks);
    
    /**
     * 
     * @param user 
     * @param spaceToken 
     * @param fileName 
     * @param callbacks 
     */
    public void srmUnmarkSpaceAsBeingUsed(SRMUser user,
            String spaceToken,
            String fileName,
            SrmCancelUseOfSpaceCallbacks callbacks);
    
    /**
     * 
     * @param spaceTokens 
     * @throws org.dcache.srm.SRMException 
     * @return 
     */
    public TMetaDataSpace[] srmGetSpaceMetaData(SRMUser user,String[] spaceTokens)
        throws SRMException;

    /**
     * 
     * @param description 
     * @throws org.dcache.srm.SRMException 
     * @return 
     */
    public String[] srmGetSpaceTokens(SRMUser user,String description)
        throws SRMException;
    
      /**
     * @param user User ID
     * @param spaceToken of a valid space reservation
     * @param newReservationLifetime new lifetime in millis to assign to space reservation
     * @return long lifetime of spacereservation left in milliseconds
     *  
     */
    
    public long srmExtendReservationLifetime(SRMUser user, String spaceToken, long newReservationLifetime)
    throws SRMException ;
   /**
     * 
     * @param description 
     * @throws org.dcache.srm.SRMException 
     * @return 
     */
    public String[] srmGetRequestTokens(SRMUser user,String description)
        throws SRMException;
    
    /**
     * @param newLifetime SURL lifetime in milliseconds
     *   -1 stands for infinite lifetime
     * @return long lifetime left in milliseconds
     *   -1 stands for infinite lifetime
     */
    public int srmExtendSurlLifetime(SRMUser user, String fileName, int newLifetime)
    throws SRMException;

    public String getStorageBackendVersion();

    public boolean exists(SRMUser user, String path) throws SRMException;
}
