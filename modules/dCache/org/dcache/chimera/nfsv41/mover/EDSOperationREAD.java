package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.acplt.oncrpc.server.OncRpcCallInformation;
import org.apache.log4j.Logger;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.IOHimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.nfs.v4.AbstractNFSv4Operation;
import org.dcache.chimera.nfs.v4.CompoundArgs;
import org.dcache.chimera.nfs.v4.HimeraNFS4Exception;
import org.dcache.chimera.nfs.v4.NFSv4OperationResult;
import org.dcache.chimera.nfs.v4.READ4res;
import org.dcache.chimera.nfs.v4.READ4resok;
import org.dcache.chimera.nfs.v4.nfs_argop4;
import org.dcache.chimera.nfs.v4.nfs_opnum4;
import org.dcache.chimera.nfs.v4.nfsstat4;
import org.dcache.chimera.posix.Stat;

public class EDSOperationREAD extends AbstractNFSv4Operation {

	private static final Logger _log = Logger.getLogger(EDSOperationREAD.class.getName());

	 private final Map<FsInode, FileChannel> _activeIO;

	public EDSOperationREAD(JdbcFs fs, OncRpcCallInformation call$, CompoundArgs fh, nfs_argop4 args,  Map<FsInode, FileChannel> activeIO) {
		super(fs, call$, fh, args, nfs_opnum4.OP_READ);
		_activeIO = activeIO;
		if(_log.isDebugEnabled() ) {
			_log.debug("NFS Request  READ from: " + _callInfo.peerAddress.getHostAddress() );
		}
	}

	@Override
	public NFSv4OperationResult process() {
        READ4res res = new READ4res();

        try {

            Stat inodeStat = _fh.currentInode().statCache();

            long offset = _args.opread.offset.value.value;
            int count = _args.opread.count.value.value;

            byte[] buf = new byte[count];

	    	IOReadFile in = new IOReadFile(_activeIO.get(_fh.currentInode()));

	    	int bytesReaded = in.read(buf, offset, count);

            if( bytesReaded < 0 ) {
                throw new IOHimeraFsException("IO not allowed");
            }

            res.status = nfsstat4.NFS4_OK;
            res.resok4 = new READ4resok();
            res.resok4.data = new byte[bytesReaded];
            System.arraycopy(buf, 0, res.resok4.data, 0, bytesReaded);

            if( offset + bytesReaded == inodeStat.getSize() ) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: " + bytesReaded + "@"  +offset +" readed, " + _args.opread.count.value.value + " requested.");

        }catch(IOHimeraFsException hioe) {
            res.status = nfsstat4.NFS4ERR_IO;
        }catch(HimeraNFS4Exception he) {
            res.status = he.getStatus();
        }catch(ChimeraFsException hfe) {
            res.status = nfsstat4.NFS4ERR_NOFILEHANDLE;
        }catch(IOException ioe) {
        	_log.error("READ: ", ioe);
    		res.status = nfsstat4.NFS4ERR_IO;
    	}catch(Exception e) {
    		_log.error("WRITE: ", e);
    		res.status = nfsstat4.NFS4ERR_IO;
    	}

       _result.opread = res;

        return new NFSv4OperationResult(_result, res.status);
	}


    private static class IOReadFile {

    	private final FileChannel _fc;

    	public IOReadFile(FileChannel fc) {
	    	_fc = fc;
    	}


	    public int read(byte[] b, long off, long len) throws IOException {

	    	ByteBuffer bb = ByteBuffer.wrap(b, 0, (int)len);
	    	bb.rewind();
	    	_fc.position(off);

	    	return _fc.read(bb);

	    }

	    public long size() throws IOException {
	    	return _fc.size();
	    }
    }


}
