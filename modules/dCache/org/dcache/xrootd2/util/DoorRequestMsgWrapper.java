package org.dcache.xrootd2.util;

import diskCacheV111.util.PnfsId;

public class DoorRequestMsgWrapper {

    private int _errorCode;
    private String _errMsg;
    private String _path;
    private PnfsId _pnfsID;
    private int _gid;
    private int _uid;
    private String _user;
    private int _fileHandle;

    public void fileOpenFailed(int errorCode, String errMsg) {
        _errorCode = errorCode;
        _errMsg = errMsg;
    }

    public void setpath(String pathToOpen) {
        _path =  pathToOpen;
    }

    public void setMappedIds(int gid, int uid) {
        _gid = gid;
        _uid = uid;
    }

    public void setPnfsId(PnfsId pnfsId) {
        _pnfsID = pnfsId;
    }

    public void setUser(String user) {
        _user = user;
    }

    public void setFileHandle(int fileHandle) {
        this._fileHandle = fileHandle;
    }

    public int getErrorCode() {
        return _errorCode;
    }

    public String getErrorMsg() {
        return _errMsg;
    }

    public String getPath() {
        return _path;
    }

    public PnfsId getPnfsId() {
        return _pnfsID;
    }

    public int getGid() {
        return _gid;
    }

    public int getUid() {
        return _uid;
    }

    public String getUser() {
        return _user;
    }

    public int getFileHandle() {
        return _fileHandle;
    }
}