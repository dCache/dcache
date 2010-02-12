package org.dcache.doors;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.protocol.XrootdProtocol;
import static org.dcache.xrootd.protocol.XrootdProtocol.*;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.RedirectResponse;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.StatxResponse;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.security.AuthorizationHandler;
import org.dcache.xrootd.util.DoorRequestMsgWrapper;
import org.dcache.xrootd.util.FileStatus;
import org.dcache.xrootd.util.ParseException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.FileMetaData.Permissions;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XrootdDoorListener implements StreamListener {

    private class PnfsFileStatus extends FileStatus {

        private PnfsId pnfsId;

        public PnfsFileStatus(PnfsId id) {
            super();
            pnfsId = id;
        }

        public PnfsId getPnfsId() {
            return pnfsId;
        }

    }

    private final static Logger _log =
        LoggerFactory.getLogger(XrootdDoorListener.class);

    private XrootdDoor door;
    private PhysicalXrootdConnection physicalXrootdConnection;
    private PnfsFileStatus fileStatus = null;

    private InetSocketAddress redirectAddress = null;
    private int streamId;

    public XrootdDoorListener(XrootdDoorController controller, int streamID) {
        this.door = controller.getDoor();
        this.physicalXrootdConnection = controller.getXrootdConnection();

        this.streamId = streamID;
    }

    private void logDebugOnOpen(OpenRequest req)
    {
        _log.debug("data size of open request: "
                   + req.getDataLength() + " bytes");

        int options = req.getOptions();
        String openFlags =
            "options to apply for open path (raw=" + options +" ):";

        if ((options & XrootdProtocol.kXR_async) == XrootdProtocol.kXR_async)
            openFlags += " kXR_async";
        if ((options & XrootdProtocol.kXR_compress) == XrootdProtocol.kXR_compress)
            openFlags += " kXR_compress";
        if ((options & XrootdProtocol.kXR_delete) == XrootdProtocol.kXR_delete)
            openFlags += " kXR_delete";
        if ((options & XrootdProtocol.kXR_force) == XrootdProtocol.kXR_force)
            openFlags += " kXR_force";
        if ((options & XrootdProtocol.kXR_new) == XrootdProtocol.kXR_new)
            openFlags += " kXR_new";
        if ((options & XrootdProtocol.kXR_open_read) == XrootdProtocol.kXR_open_read)
            openFlags += " kXR_open_read";
        if ((options & XrootdProtocol.kXR_open_updt) == XrootdProtocol.kXR_open_updt)
            openFlags += " kXR_open_updt";
        if ((options & XrootdProtocol.kXR_refresh) == XrootdProtocol.kXR_refresh)
            openFlags += " kXR_refresh";
        if ((options & XrootdProtocol.kXR_mkpath) == XrootdProtocol.kXR_mkpath)
            openFlags += " kXR_mkpath";
        if ((options & XrootdProtocol.kXR_open_apnd) == XrootdProtocol.kXR_open_apnd)
            openFlags += " kXR_open_apnd";
        if ((options & XrootdProtocol.kXR_retstat) == XrootdProtocol.kXR_retstat)
            openFlags += " kXR_retstat";

        _log.debug("open flags: "+openFlags);

        int mode = req.getUMask();
        String s = "";

        if ((mode & XrootdProtocol.kXR_ur) == XrootdProtocol.kXR_ur)
            s += "r";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_uw) == XrootdProtocol.kXR_uw)
            s += "w";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_ux) == XrootdProtocol.kXR_ux)
            s += "x";
        else
            s += "-";

        s += " ";

        if ((mode & XrootdProtocol.kXR_gr) == XrootdProtocol.kXR_gr)
            s += "r";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_gw) == XrootdProtocol.kXR_gw)
            s += "w";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_gx) == XrootdProtocol.kXR_gx)
            s += "x";
        else
            s += "-";

        s += " ";

        if ((mode & XrootdProtocol.kXR_or) == XrootdProtocol.kXR_or)
            s += "r";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_ow) == XrootdProtocol.kXR_ow)
            s += "w";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_ox) == XrootdProtocol.kXR_ox)
            s += "x";
        else
            s += "-";

        _log.debug("mode to apply to open path: " + s);
    }


    /**
     * The Open method is the only request we need to deal with (login
     * and authentication is done on a per-connection-basis, not for
     * every single file) besides the stat request.  The open, if
     * successful, will always result in a redirect response to the
     * proper pool, hence no subsequent requests like sync, read,
     * write or close are expected at the door.
     */
    public void doOnOpen(OpenRequest req)
    {
        DoorRequestMsgWrapper info = new DoorRequestMsgWrapper();
        String path = req.getPath();
        int options = req.getOptions();

        _log.info("request to open: " + path);

        if (_log.isDebugEnabled()) {
            logDebugOnOpen(req);
        }

        info.setpath(path);

        boolean isWrite = req.isNew() || req.isReadWrite();

        try {
            if (isWrite && door.isReadOnly()) {
                throw new CacheException(CacheException.PERMISSION_DENIED,
                                         "Permission denied. Access is read only.");
            }

            // do authorization check if required by configuration
            if (door.authzRequired()) {
                if (door.getAuthzFactory() == null) {
                    throw new CacheException("Authorization required but appropriate handler is not initialised. Server probably misconfigured.");
                }

                _log.debug("checking authorization for " + path);

                // all information neccessary for checking authorization
                // is found in opaque
                Map opaque;
                try {
                    opaque = req.getOpaque();
                } catch (ParseException e) {
                    StringBuffer msg =
                        new StringBuffer("invalid opaque data: ");
                    msg.append(e);
                    int opaqueStart = req.hasOpaque();
                    if (opaqueStart >= 0) {
                        msg.append(" opaque=");
                        msg.append(new String(req.getData(), opaqueStart,
                                              req.getDataLength() - opaqueStart));
                    }
                    throw new CacheException(CacheException.PERMISSION_DENIED,
                                             msg.toString());
                }

                AuthorizationHandler authzHandler =
                    door.getAuthzFactory().getAuthzHandler();

                boolean isAuthorized = false;
                try {
                    isAuthorized =
                        authzHandler.checkAuthz(path, opaque, isWrite, door);
                } catch (GeneralSecurityException e) {
                    throw new CacheException(CacheException.PERMISSION_DENIED,
                                              "authorization check failed: " +
                                              e.getMessage());
                } finally {
                    if (authzHandler.getUser() != null) {
                        info.setUser(authzHandler.getUser());
                    }
                }

                if (!isAuthorized) {
                    throw new CacheException(CacheException.PERMISSION_DENIED,
                                              "not authorized");
                }

                // In case of enabled authorization, the path in the open
                // request can refer to the lfn.  In this case the real
                // path is delivered by the authz plugin
                if (authzHandler.providesPFN()) {
                    _log.info("access granted for LFN=" + path +
                              " PFN=" + authzHandler.getPFN());

                    // get the real path (pfn) which we will open
                    path = authzHandler.getPFN();
                    info.setpath(path);
                }
            }

            path = new FsPath(path).toString();

            // check if write access is restricted in general and whether
            // the path to open matches the whitelist
            if (isWrite &&
                door.getAuthorizedWritePaths() != null &&
                !matchWritePath(path, door.getAuthorizedWritePaths())) {
                throw new CacheException(CacheException.PERMISSION_DENIED,
                                         "Write permission denied");
            }

            ////////////////////////////////////////////////////////////////
            // interact with core dCache to open the requested file
            PnfsGetStorageInfoMessage msg;
            if (isWrite) {
                boolean createDir = (options & XrootdProtocol.kXR_mkpath) ==
                    XrootdProtocol.kXR_mkpath;
                msg = door.createNewPnfsEntry(path, createDir);
            } else {
                msg = door.getStorageInfo(path);
            }

            StorageInfo storageInfo = msg.getStorageInfo();
            FileMetaData metaData = msg.getMetaData();
            PnfsId pnfsid = msg.getPnfsId();

            info.setMappedIds(metaData.getGid(), metaData.getUid());
            info.setPnfsId(pnfsid);

            // get unique fileHandle (PnfsId is not unique in case the
            // same file is opened more than once in this
            // door-instance)
            int fileHandle = door.getNewFileHandle();

            info.setFileHandle(fileHandle);

            long checksum = req.calcChecksum();
            _log.info("checksum of openrequest: " + checksum);

            fileStatus = (PnfsFileStatus) convertToFileStatus(metaData, pnfsid);
            fileStatus.setWrite(isWrite);
            fileStatus.setID(fileHandle);

            redirectAddress =
                door.transfer(pnfsid, storageInfo,
                              fileHandle, checksum, isWrite);

            // ok, open was successful
            physicalXrootdConnection.getResponseEngine().sendResponseMessage(new RedirectResponse(req.getStreamID(), redirectAddress.getHostName(), redirectAddress.getPort()));

            door.newFileOpen(fileHandle, req.getStreamID());
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                respondWithError(req.getStreamID(),
                                 XrootdProtocol.kXR_FileNotOpen,
                                 "No such file");
                break;

            case CacheException.DIR_NOT_EXISTS:
                respondWithError(req.getStreamID(),
                                 XrootdProtocol.kXR_FileNotOpen,
                                 "No such directory");
                break;

            case CacheException.FILE_EXISTS:
                respondWithError(req.getStreamID(),
                                 XrootdProtocol.kXR_Unsupported,
                                 "Cannot open existing file for writing");
                break;

            case CacheException.TIMEOUT:
                respondWithError(req.getStreamID(),
                                 XrootdProtocol.kXR_ServerError,
                                 "Internal timeout");
                break;

            case CacheException.PERMISSION_DENIED:
                respondWithError(req.getStreamID(),
                                 XrootdProtocol.kXR_NotAuthorized,
                                 e.getMessage());
                break;

            default:
                respondWithError(req.getStreamID(),
                                 XrootdProtocol.kXR_ServerError,
                                 String.format("Failed to open file (%s [%d])",
                                               e.getMessage(), e.getRc()));
                break;
            }
            info.fileOpenFailed(e.getRc(), e.getMessage());
        } catch (InterruptedException e) {
            info.fileOpenFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                e.getMessage());
            respondWithError(req.getStreamID(),
                             XrootdProtocol.kXR_ServerError,
                             "Server shutdown");
        } catch (RuntimeException e) {
            info.fileOpenFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                e.getMessage());
            respondWithError(req.getStreamID(),
                             XrootdProtocol.kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           e.getMessage()));
        } finally {
            door.sendBillingInfo(info);
        }
    }

    private void respondWithError(int streamID, int errorCode, String errMsg) {
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(
                                                                         new ErrorResponse(
                                                                                           streamID,
                                                                                           errorCode,
                                                                                           errMsg ));

        // for billing purposes
    }


    public void doOnStatus(StatRequest req) {
        AbstractResponseMessage response = null;

        if (fileStatus == null) {

            String path = new FsPath(req.getPath()).toString();

            // no OPEN occured before, so we need to ask the for the metadata
            FileMetaData meta = null;
            try {

                meta = door.getFileMetaData(path);

            } catch (CacheException e) {
                _log.info("No PnfsId found for path: " + path);
                response = new StatResponse(req.getStreamID(), null);
            }

            if (meta != null) {

                FileStatus fs = convertToFileStatus(meta, null);

                // we finally got the stat result
                response = new StatResponse(req.getStreamID(), fs);

            } else {
                response = new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FSError, "Internal server error: no metadata");
            }

        } else {

            // there was an OPEN happening before, so we already have the status info
            response = new StatResponse(req.getStreamID(), fileStatus);
        }

        physicalXrootdConnection.getResponseEngine().sendResponseMessage(response);
    }

    public void doOnStatusX(StatxRequest req)
    {
        String[] paths = req.getPaths();
        if (paths.length == 0) {
            physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_ArgMissing, "no paths specified"));
        }

        for (int i = 0; i < paths.length; i++) {
            paths[i] = new FsPath(paths[i]).toString();
        }
        FileMetaData[] allMetas = door.getMultipleFileMetaData(paths);

        int[] flags = new int[allMetas.length];
        Arrays.fill(flags, -1);

        for (int i =0; i < allMetas.length; i++) {
            if (allMetas[i] == null) {
                continue;
            }

            flags[i] = convertToFileStatus(allMetas[i], null).getFlags();
        }

        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new StatxResponse(req.getStreamID(), flags));

    }
    public void doOnRead(ReadRequest req) {
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
    }

    public void doOnReadV(ReadVRequest req) {
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
    }

    public void doOnWrite(WriteRequest req) {
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
    }

    public void doOnSync(SyncRequest req) {
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
    }

    public void doOnClose(CloseRequest request) {
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
    }

    public void handleStreamClose() {

        //		clean up something?

        _log.info("closing logical stream (streamID="+streamId+")");
    }

    /**
     * check wether the given path matches against a list of allowed paths
     * @param pathToOpen the path which is going to be checked
     * @param authorizedWritePathList the list of allowed paths
     * @return
     */
    private boolean matchWritePath(String path, List<String> authorizedWritePathList)
    {
        for (String prefix: authorizedWritePathList) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private FileStatus convertToFileStatus(FileMetaData meta, PnfsId pnfsid) {

        if (meta == null) {
            return null;
        }

        FileStatus fs =
            pnfsid == null ? new FileStatus() : new PnfsFileStatus(pnfsid);

        fs.setSize(meta.getFileSize());
        fs.setModtime(meta.getLastModifiedTime());

        // set flags
        if (meta.isDirectory()) fs.addToFlags(kXR_isDir);
        if (!meta.isRegularFile() && !meta.isDirectory()) fs.addToFlags(kXR_other);
        Permissions pm = meta.getWorldPermissions();
        if (pm.canExecute()) fs.addToFlags(kXR_xset);
        if (pm.canRead()) fs.addToFlags(kXR_readable);
        if (pm.canWrite()) fs.addToFlags(kXR_writable);

        return fs;
    }

}
