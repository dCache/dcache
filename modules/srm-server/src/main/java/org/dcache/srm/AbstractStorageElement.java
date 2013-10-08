//$Id$
//$Log: not supported by cvs2svn $

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


import javax.annotation.Nonnull;

import java.net.URI;
import java.util.List;

import diskCacheV111.srm.StorageElementInfo;

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
public interface AbstractStorageElement {

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
     * @param surl SURL
     * @param protocols List of SE supported protocols
     * @throws SRMException
     * @return Transport URL for file operation
     */
    public URI getPutTurl(SRMUser user, URI surl, String[] protocols)
        throws SRMException;

    /** To accomodate the property of dcache that requires that the same client get all
     * dcap transfers though the same dcap door, we put the get(Get/Put)Turl which has
     * this siganture.
     * The previous_turl is the turl that was obtained by the client before.
     * @param user User ID
     * @param surl SURL
     * @param previous_turl The transport URL received from the previous call of getPutTurl
     * @throws SRMException
     * @return Transport URL for file operation
     */
    public URI getPutTurl(SRMUser user, URI surl, URI previous_turl)
        throws SRMException;

    /** This method has to be called to get the transport URL for file operation.
     * The returned value is passed to the user and user does actual data transfer
     * @param user User ID
     * @param filePath File path
     * @param protocols
     * @throws SRMException
     * @return Transport URL for file operation
     */
    public URI getGetTurl(SRMUser user, URI surl, String[] protocols)
        throws SRMException;

    /** To accomodate the property of dcache that requires that the same client get all
     * dcap transfers though the same dcap door, we put the get(Get/Put)Turl which has
     * this siganture.
     * The previous_turl is the turl that was obtained by the client before.
     * @param user User ID
     * @param surl SURL
     * @param previous_turl The transport URL received from the previous call of getPutTurl
     * @throws SRMException
     * @return Transport URL for file operation
     */
    public URI getGetTurl(SRMUser user, URI surl, URI previous_turl)
        throws SRMException;

    /** Method must be nonblocking -- when called it creates thread and returns immediately,
     *         result will be sent thru callbacks
     * @param user User ID
     * @param surl SURL
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * @param overwrite allow overwrite if true
     * various actions performed to put file from the storage
     */
    public void prepareToPut(SRMUser user, URI surl,
            PrepareToPutCallbacks callbacks,
            boolean overwrite);

    /** This method allows to pin file in the Storage Element,
     * i.e. put the file in "fast access state"
     * @param user User ID
     * @param surl
     * @param network address from which file will be read
     *        null, if unknown
     * @param pinLifetime Requested pin operation lifetime in millis
     * @param requestToken - pin will save request token
     *        so that unpinning by file name and request token can take place
     * @param callbacks This interface is used for asynchronous notification of SRM of the
     * various actions performed to "pin" file in the storage
     */


    public void pinFile(SRMUser user,
           URI surl,
           String clientHost,
           long pinLifetime,
           String requestToken,
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
     * @param surl
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return transfer id
     *  an id to the pending tranfer that can be used to cancel the transfer via killRemoteTransfer
     */
    public String getFromRemoteTURL(
            SRMUser user,
            URI remoteTURL,
            URI surl,
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
     * @param surl
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return transfer id
     *  an id to the pending tranfer that can be used to cancel the transfer via killRemoteTransfer
     */
    public String getFromRemoteTURL(
            SRMUser user,
            URI remoteTURL,
            URI surl,
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
     * @param surl
     * @param remoteTURL
     * @param remoteUser
     * @param callbacks
     * @param remoteCredetial
     * @throws SRMException
     * @return transfer id
     *    an id to the pending tranfer that can be used to cancel the transfer via killRemoteTransfer
     */
    public String putToRemoteTURL(SRMUser user,
            URI surl,
            URI remoteTURL,
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
     * @param fromSurl
     * @param toSurl
     * @throws SRMException
     */
    public void localCopy(SRMUser user, URI fromSurl, URI toSurl)
    throws SRMException;

    /**
     * @param url
     * @throws SRMException
     * @return
     */
    public boolean isLocalTransferUrl(URI url) throws SRMException;

    /**
     * Retrieves the FileMetaData of a file.
     *
     * An implementation may check whether the user sufficient
     * privileges. If the read parameter is true, an implementation is
     * requested to check whether the user is allowed to read the file
     * in addition to retrieving the FileMetaData. If the read
     * parameter is false, then only permission to retrieve the
     * FileMetaData is checked.
     *
     * @param user User ID
     * @param filePath File path
     * @param read True if read permission are required, false otherwise
     * @return FileMetaData of the file
     * @throws SRMAuthorizationException if the user lacks sufficient
     *         privileges
     * @throws SRMInvalidPathException if the file does not exist
     * @throws SRMInternalErrorException in case of transient errors
     * @throws SRMException for any other error
     */
    @Nonnull
    public FileMetaData getFileMetaData(SRMUser user,URI surl,boolean read)
        throws SRMException;

    /**
     * @param user User ID
     * @param path
     * @return
     */

    public void setFileMetaData(SRMUser user,FileMetaData fmd) throws SRMException;

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
     * @param requestToken id given to the storage  during pinFile operation
     */
    public void unPinFileBySrmRequestId(SRMUser user,String fileId,
            UnpinCallbacks callbacks,
            String requestToken);
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
     * @param surl
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to remove file from the storage
     */
    public void advisoryDelete(SRMUser user, URI surl,AdvisoryDeleteCallbacks callbacks);

    /**
     *
     * @param user User ID
     * @param surl
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to remove file from the storage
     */
    public void removeFile(SRMUser user, URI surl,RemoveFileCallbacks callbacks);

    /**
     * @param user User ID
     * @param surls List of SURLs
     */
    public void removeDirectory(SRMUser user, List<URI> surls)
        throws SRMException;

    /**
     *
     * @param user
     * @param surl
     * @throws SRMException
     */
    public void createDirectory(SRMUser user,
                                URI surl) throws SRMException;

    /**
     *
     * @param user
     * @param from
     * @param to
     * @throws SRMException
     */
    public void moveEntry(SRMUser user,
                          URI from,
                          URI to) throws SRMException;



    /** This method tells if the specified file can be written
     * @param user User ID
     * @param fileId SE internal file ID
     * @param fmd File metadata received from getFileMetaData
     * @param parentFileId SE internal parent directory ID
     * @param parentFmd Directory metadata received from getFileMetaData
     * 2param overwrite is true, allow overwrite of existing files
     * @return Boolean T/F
     */

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


    /** This method returns the information about the Storage Element
     * @param user User ID
     * @throws SRMException
     * @return StorageElementInfo object contained information about the Storage Element
     */
    public StorageElementInfo getStorageElementInfo(SRMUser user) throws SRMException;

    /**
     * Provides a directory listing of surl if and only if surl is not
     * a symbolic link. As a side effect, the method checks that surl
     * can be deleted by the user.
     *
     * @param user The SRMUser performing the operation; this myst be
     * of type AuthorizationRecord
     * @param surl The directory to delete
     * @return The array of directory entries or null if directoryName
     * is a symbolic link
     */
    public List<URI> listNonLinkedDirectory(SRMUser user, URI surl)
        throws SRMException;

    /**
     *
     * @param user
     * @param surl
     * @param fileMetaData
     * @throws SRMException
     * @return
     */
    public List<URI> listDirectory(SRMUser user, URI surl, FileMetaData fileMetaData) throws SRMException;

    /**
     * Lists directory contents. The contents is provided as a list of
     * FileMetaData objects, with one FileMetaDataObject per directory
     * entry.
     *
     * The path of each file is provided in the SURL field of the
     * FileMetaDataObject. The path of the <code>surl</code> parameter
     * is a prefix of all paths and hence the SURL field is not a
     * complete SURL.
     *
     * If verbose listing is requested, additional fields such as the
     * spaceTokens and isCached fields of the FileMetaData object will
     * be filled. Those fields may be more expensive to retrieve.
     *
     * @param user The user requesting the list operation
     * @param surl The path of the directory to list
     * @param verbose Whether to include fields that are expensive to
     *                retrieve
     * @param offset The first entry in the directory to retrieve
     * @param count The maximum number of entries to retrieve
     * @return The directory contents as a list of FileMetaData objects.
     * @throws SRMInternalErrorException if the operation timed out or
     *         was aborted for other internal reasons.
     * @throws SRMInvalidPathException if <code>directory</code> does
     *         not exist or is not a directory.
     * @throws SRMAuthorizationException if <code>user</code> does not have
     *         permission to list <code>directory</code>.
     * @throws SRMException for other failures.
     */
    List<FileMetaData>
        listDirectory(SRMUser user, URI surl, boolean verbose,
                      int offset, int count)
        throws SRMException;

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
     * @param surl
     * @param sizeInBytes
     * @param useLifetime
     * @param callbacks
     */
    public void srmMarkSpaceAsBeingUsed(SRMUser user,
                                        String spaceToken,
                                        URI surl,
                                        long sizeInBytes,
                                        long useLifetime,
                                        boolean overwrite,
                                        SrmUseSpaceCallbacks callbacks);

    /**
     *
     * @param user
     * @param spaceToken
     * @param surl
     * @param callbacks
     */
    public void srmUnmarkSpaceAsBeingUsed(SRMUser user,
                                          String spaceToken,
                                          URI surl,
                                          SrmCancelUseOfSpaceCallbacks callbacks);

    /**
     *
     * @param spaceTokens
     * @throws SRMException
     * @return
     */
    public TMetaDataSpace[] srmGetSpaceMetaData(SRMUser user,String[] spaceTokens)
        throws SRMException;

    /**
     *
     * @param description
     * @throws SRMException
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
     * @throws SRMException
     * @return
     */
    public String[] srmGetRequestTokens(SRMUser user,String description)
        throws SRMException;

    /**
     *
     * @param newLifetime SURL lifetime in milliseconds
     *   -1 stands for infinite lifetime
     * @return long lifetime left in milliseconds
     *   -1 stands for infinite lifetime
     */
    public long srmExtendSurlLifetime(SRMUser user, URI surl, long newLifetime)
    throws SRMException;

    public String getStorageBackendVersion();

    public boolean exists(SRMUser user, URI surl) throws SRMException;
}
