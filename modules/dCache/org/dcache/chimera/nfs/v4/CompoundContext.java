/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.chimera.nfs.v4;

import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.chimera.posix.UnixUser;
import org.dcache.xdr.RpcCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import org.dcache.chimera.nfs.NfsUser;


public class CompoundContext {

    private static final Logger _log = LoggerFactory.getLogger(CompoundContext.class);

    private FsInode _rootInode = null;
    private FsInode _currentInode = null;
    private FsInode _savedInode = null;

    private final int _minorversion;

    private NFSv41Session _session = null;
    private final List<nfs_resop4> _processedOps;

    private final FileSystemProvider _fs;
    private final RpcCall _callInfo;
    private final UnixUser _user;
    private final ExportFile _exportFile;
    private final NFSv41DeviceManager _deviceManager;
    private final AclHandler _aclHandler;
    private final NFSv4StateHandler _stateHandler;
    private final NfsIdMapping _idMapping;

    /**
     * Create context of COUMPOUND request.
     *
     * @param processedOps @{link List} where results of processed operations are stored.
     * @param minorversion NFSv4 minor version number.
     * @param fs backend file-system interface
     * @param call RPC call
     * @param exportFile list of servers exports.
     */
    public CompoundContext(List<nfs_resop4> processedOps, int minorversion, FileSystemProvider fs,
            NFSv4StateHandler stateHandler,
            NFSv41DeviceManager deviceManager, AclHandler aclHandler, RpcCall call,
            NfsIdMapping idMapping,
            ExportFile exportFile) {
        _processedOps = processedOps;
        _minorversion = minorversion;
        _fs = fs;
        _deviceManager = deviceManager;
        _aclHandler = aclHandler;
        _callInfo = call;
        _exportFile = exportFile;
        _user = NfsUser.remoteUser(_callInfo, _exportFile);
        _stateHandler = stateHandler;
        _idMapping = idMapping;
    }

    public RpcCall getRpcCall() {
        return _callInfo;
    }
    public UnixUser getUser() {
        return _user;
    }

    public FileSystemProvider getFs() {
        return _fs;
    }

    public NFSv41DeviceManager getDeviceManager() {
        return _deviceManager;
    }

    public AclHandler getAclHandler() {
        return _aclHandler;
    }
    /**
     * Get NFSv4 minor version number. The version number os provided by client
     * for each compound.
     * @return version number.
     */
    public int getMinorversion() {
        return _minorversion;
    }

    /**
     * Current file handle is a server side variable passed from one operation
     * to other inside a compound.
     *
     * @return file handle
     * @throws ChimeraNFSException
     */
    public FsInode currentInode() throws ChimeraNFSException {
        if( _currentInode == null ) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_NOFILEHANDLE, "no file handle");
        }
        return _currentInode;
    }

    /**
     * Set current file handle.
     *
     * @param inode
     * @throws ChimeraNFSException
     */
    public void currentInode(FsInode inode) throws ChimeraNFSException {
        _currentInode = inode;
        _log.debug("current Inode: {}",  _currentInode );
    }

    /**
     * Consume current file handle.
     *
     * @throws ChimeraNFSException
     */
    public void clearCurrentInode() throws ChimeraNFSException {
        _currentInode = null;
    }

    public FsInode rootInode() {
        return _rootInode;
    }

    public void rootInode(FsInode inode) {
        _rootInode = inode;
        _log.debug("root Inode: {}", _rootInode );
    }

    /**
     * Set the current file handle to the value in the saved file handle.
     * If there is no saved filehandle then the server will return the
     * error NFS4ERR_RESTOREFH.
     * @throws ChimeraNFSException
     */
    public void restoreSavedInode() throws ChimeraNFSException {
        if( _savedInode == null ) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_RESTOREFH, "no saved file handle");
        }
        _currentInode = _savedInode;
        _log.debug("restored Inode: {}",  _currentInode );
    }

    public FsInode savedInode() throws ChimeraNFSException {
        if( _savedInode == null ) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_NOFILEHANDLE, "no file handle");
        }
        return _savedInode;
    }

    /**
     * Save the current filehandle. If a previous filehandle was saved then it
     * is no longer accessible. The saved filehandle can be restored as
     * the current filehandle with the RESTOREFH operator.
     * @throws ChimeraNFSException
     */
    public void saveCurrentInode() throws ChimeraNFSException {
        if( _currentInode == null ) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_NOFILEHANDLE, "no file handle");
        }
        _savedInode = _currentInode;
        _log.debug("saved Inode: {}", _savedInode );
    }

    /**
     * Set NFSv4.1 session of current request.
     * @param session
     */
    public void setSession(NFSv41Session session) {
        _session = session;
    }

    /**
     * Get {@link NFSv41Session} used by current request.
     * @return current session
     */
    public NFSv41Session getSession() {
        return _session;
    }

    /**
     * Get list of currently processed operations.
     * @return list of operations.
     */
    public List<nfs_resop4> processedOperations() {
        return _processedOps;
    }

    public NFSv4StateHandler getStateHandler() {
        return _stateHandler;
    }

    public NfsIdMapping getIdMapping() {
        return _idMapping;
    }
}
