// $Id: DirectoryLookUpPool.java,v 1.5 2007-07-26 15:44:42 tigran Exp $


package org.dcache.chimera.namespace;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HimeraDirectoryEntry;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.XMLconfig;
import org.dcache.vehicles.DirectoryListMessage;

import diskCacheV111.movers.DCapConstants;
import diskCacheV111.movers.DCapDataOutputStream;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolIoFileMessage;
import org.dcache.chimera.DirectoryStreamB;

public class DirectoryLookUpPool extends CellAdapter {

    private final String      _poolName ;
    private final Args        _args     ;
    private final CellNucleus _nucleus  ;
    private final JdbcFs       _fs;



    private final static int MAXCACHESIZE = 100;

    private final Map<FsInode, LookupCacheEntry> _LOOKUP_CACHE = Collections.synchronizedMap( new LinkedHashMap<FsInode, LookupCacheEntry>(){
        // This method is called just after a new entry has been added
        @Override
        public boolean removeEldestEntry(Map.Entry<FsInode, LookupCacheEntry> eldest) {
            return size() > MAXCACHESIZE;
        }
    } );

    private static final Logger _logNameSpace =  Logger.getLogger("logger.org.dcache.namespace");


    public DirectoryLookUpPool(String poolName, String args) throws Exception {
        super( poolName, args , false );


        _poolName = poolName;
        _args     = getArgs();
        _nucleus  = getNucleus() ;

        say("Lookup Pool "+poolName +" starting");

        try {

    	    XMLconfig config = new XMLconfig( new File( _args.getOpt("chimeraConfig") ) );
    	    _fs = new JdbcFs(  config );

        } catch (Exception e){
            say("Exception occurred on startup: "+e);
            start();
            kill();
            throw e;
        }

        useInterpreter( true );
        _nucleus.export();
        start() ;

    }

    @Override
    public void getInfo( PrintWriter pw ){
        pw.println("JdbcFs            : "+_fs.getInfo());
        pw.println("Revision          : [$Id: DirectoryLookUpPool.java,v 1.5 2007-07-26 15:44:42 tigran Exp $]" ) ;
    }

    @Override
    public void messageArrived( CellMessage cellMessage ){


    	Message messageToProcess = (Message) cellMessage.getMessageObject();

		if (messageToProcess instanceof DirectoryListMessage) {
			processListRequest((DirectoryListMessage) messageToProcess);
		} else if (messageToProcess instanceof PoolIoFileMessage) {
			ioFile((PoolIoFileMessage) messageToProcess, cellMessage);
			return;
		}


    	if ( messageToProcess.getReplyRequired() ) {
    		cellMessage.revertDirection();
    		try {
				sendMessage(cellMessage);
			} catch (NoRouteToCellException e) {
				// caller cell died.
				_logNameSpace.info("Cant send reply to " + cellMessage.getDestinationPath() + " : " + e.getMessage());
			}
    	}

    }


    private void processListRequest(DirectoryListMessage messageToProcess) {

    	// TODO: and what about cookies? maxcount? maxbyte?

    	String[] list = list( new FsInode ( _fs, messageToProcess.getPnfsId().getId() ) );
		if( list == null ) {
			// TODO: more checks : not a dir, does not exist, by path....
			messageToProcess.setFailed(CacheException.DIR_NOT_EXISTS , "bad dir");
		}else{
			messageToProcess.list(list);
			messageToProcess.eof(true);
			messageToProcess.cookie(list.length);
		}
	}

	/**
     *
     * @param dirInode
     * @return An array of strings naming the files and directories in the directory denoted by this pnfsid.
     *         The array will be empty if the directory is empty or if no names were accepted by the filter.
     *         Returns null if this pnfsid does not denote a directory, or if an I/O error occurs.
     */
    private String[] list(FsInode dirInode) {

    	String[] list = null;

    	if( !dirInode.isDirectory() ) return null;
    	try {

	    	LookupCacheEntry cacheEntry = _LOOKUP_CACHE.get(dirInode);

	    	if( cacheEntry != null && cacheEntry.mTime() > dirInode.statCache().getMTime() ) {
	    		list =  cacheEntry.list();
	    		if(_logNameSpace.isDebugEnabled() ) {
	    			_logNameSpace.debug("using cached reply for pnfsid " + dirInode.toString() );
	    		}
	    	}else{
	    		if(_logNameSpace.isDebugEnabled() ) {
	    			_logNameSpace.debug("fetching new list for pnfsid " + dirInode.toString() );
	    		}
	    		list = _fs.listDir(dirInode);
	    		cacheEntry = new LookupCacheEntry(System.currentTimeMillis(), list);
	    		_LOOKUP_CACHE.put(dirInode, cacheEntry);
	    	}
    	}catch(ChimeraFsException hfe ) {
    		// do noting as docu describes
    	}

    	return list;

    }

    // commands
    public String hh_ls = " <path>";
    public String ac_ls_$_1( Args args ) throws Exception {

    	String path = args.argv(0);
        StringBuilder sb =  new StringBuilder();

        FsInode inode = _fs.path2inode(args.argv(0));

            if( inode.isDirectory() ) {
                DirectoryStreamB<HimeraDirectoryEntry> dirStream = inode.newDirectoryStream();
                try{
                    for( HimeraDirectoryEntry entry: dirStream) {
                        sb.append(entry.getStat()).append("  ").append( entry.getInode().toString() ).
                        append("  ").append(entry.getName()).append('\n');
                    }
                }finally{
                    dirStream.close();
                }
            }else{
               sb.append(inode.statCache()).append("  ").append( inode.toString() ).
               append("  ").append( new File(path).getName() ).append('\n');
            }

        return sb.toString();
    }

    // commands
    public String hh_dir = " <path>";
    public String ac_dir_$_1( Args args ) throws Exception {

        StringBuilder sb =  new StringBuilder();

        FsInode inode = _fs.path2inode(args.argv(0));

        String[] list = list(inode);

        if( list == null ) {
        	sb.append("not a directory");
        }else{
        	for( int i = 0; i < list.length; i++) {

                sb.append(list[i]).append('\n');
            }
        }


        return sb.toString();
    }


    public String hh_mkdir = " <path>";
    public String ac_mkdir_$_1( Args args ) throws Exception {

    	_fs.mkdir(args.argv(0));

    	return "";
    }


    public String hh_touch = " <path>";
    public String ac_touch_$_1( Args args ) throws Exception {

    	FsInode inode = null;

        try {
             inode = _fs.path2inode(args.argv(0));
             _fs.setFileMTime(inode, System.currentTimeMillis());

        }catch(FileNotFoundHimeraFsException e){
            try {
                 inode = _fs.createFile(args.argv(0));
            }catch(ChimeraFsException ee) {
                return ("unable to create file " + args.argv(0) + " : " + ee.getMessage());
            }
        }

    	return "";
    }


    public String hh_chmod = " <path> <octal mode>";
    public String ac_chmod_$_2( Args args ) throws Exception {

    	_fs.setFileMode( _fs.path2inode(args.argv(0)), Integer.parseInt(args.argv(1), 8)  );

    	return "";
    }


    public String hh_chown = " <path> <uid>";
    public String ac_chown_$_2( Args args ) throws Exception {

    	_fs.setFileOwner( _fs.path2inode(args.argv(0)), Integer.parseInt(args.argv(1))  );

    	return "";
    }

    public String hh_chgrp = " <path> <gid>";
    public String ac_chgrp_$_2( Args args ) throws Exception {

    	_fs.setFileGroup( _fs.path2inode(args.argv(0)), Integer.parseInt(args.argv(1))  );

    	return "";
    }

    public String hh_delete = " <path>";
    public String ac_delete_$_1( Args args ) throws Exception {

    	_fs.remove(args.argv(0));

    	return "";
    }



    private static class LookupCacheEntry {

    	private final String[] _list;
    	private final long _mtime;

    	LookupCacheEntry(long mtime, String[] list) {
    		_mtime = mtime;
    		_list = list;
    	}

    	public String[] list() {
    		return _list;
    	}

    	public long mTime() {
    		return _mtime;
    	}

    }


	// //////////////////////////////////////////////////////////////
	//
	// The io File Part
	//
	//
	private void ioFile(PoolIoFileMessage poolMessage, CellMessage cellMessage) {

		cellMessage.revertDirection();

		try {
			_nucleus.newThread(new DirectoryService(poolMessage, cellMessage),"dir").start();
		} catch (Exception e) {
			esay(e);
		}

	}

	// this is a actual mover
	private class DirectoryService implements Runnable {

		private DCapProtocolInfo dcap;
		private int sessionId;

		private DCapDataOutputStream ostream;
		private DataInputStream istream;
		private Socket _dataSocket = null;
		private DCapDataOutputStream cntOut = null;
		private DataInputStream cntIn = null;
		private final FsInode _dirInode;

		DirectoryService(PoolIoFileMessage poolMessage,
				CellMessage originalCellMessage) throws IOException {

			dcap = (DCapProtocolInfo) poolMessage.getProtocolInfo();

			PnfsId pnfsId = poolMessage.getPnfsId();

			_dirInode = new FsInode(_fs,pnfsId.toString() );
			sessionId = dcap.getSessionId();

		}

		public void run() {

			boolean done = false;
			int commandSize;
			int commandCode;
			int minSize;

			int index = 0;

			try {

				connectToClinet();
				String dirList = createDirEnt(_dirInode);

				while (!done && !Thread.currentThread().isInterrupted()) {



					commandSize = cntIn.readInt();

					if (commandSize < 4)
						throw new CacheException(44,
								"Protocol Violation (cl<4)");

					commandCode = cntIn.readInt();
					switch (commandCode) {
					// -------------------------------------------------------------
					//
					// The IOCMD_CLOSE
					//
					case DCapConstants.IOCMD_CLOSE:
						cntOut.writeACK(DCapConstants.IOCMD_CLOSE);
						done = true;
						break;

					// -------------------------------------------------------------
					//
					// The ReadDir
					//
					case DCapConstants.IOCMD_READ:
						//
						//
						minSize = 12;
						if (commandSize < minSize)
							throw new CacheException(45,
									"Protocol Violation (clREAD<8)");

						long numberOfEntries = cntIn.readLong();
						esay("requested " + numberOfEntries + " bytes");

						cntOut.writeACK(DCapConstants.IOCMD_READ);
						index += doReadDir(cntOut, ostream, dirList, index,
								numberOfEntries);
						cntOut.writeFIN(DCapConstants.IOCMD_READ);

						break;
					default:
						cntOut.writeACK(1717, 9, "Invalid mover command : "
								+ commandCode);
						break;
					}

				}

			} catch (Exception e) {
				esay(e);
			} finally {
				if (ostream != null) {
					try {
						ostream.close();
					} catch (IOException e) {
						// ignored
					}
				}

				if (istream != null) {
					try {
						istream.close();
					} catch (IOException e) {
						// ignored
					}
				}

				if (_dataSocket != null) {
					try {
						_dataSocket.close();
					} catch (IOException e) {
						// ignored
					}
				}
			}

		}

		void connectToClinet() throws Exception {

			int port = dcap.getPort();
			String[] hosts = dcap.getHosts();
			String host = null;
			Exception se = null;

			//
			// try to connect to the client, scan the list.
			//
			for (int i = 0; i < hosts.length; i++) {
				try {
					host = hosts[i];
					_dataSocket = new Socket(host, port);
				} catch (Exception e) {
					se = e;
					continue;
				}
				break;
			}

			if (_dataSocket == null) {
				esay(se);
				throw se;
			}

			ostream = new DCapDataOutputStream(_dataSocket.getOutputStream());
			istream = new DataInputStream(_dataSocket.getInputStream());
			esay("Connected to " + host + "(" + port + ")");
			//
			// send the sessionId and our (for now) 0 byte security
			// challenge.
			//

			cntOut = ostream;
			cntIn = istream;

			cntOut.writeInt(sessionId);
			cntOut.writeInt(0);
			cntOut.flush();

		}

		private int doReadDir(DCapDataOutputStream cntOut,
				DCapDataOutputStream ostream, String dirList, int index,
				long len) throws Exception {

			long rc = 0;
			byte data[] = null;

			if (index > dirList.length()) {
				throw new ArrayIndexOutOfBoundsException(
						" requested index greater then directory size");
			}

			data = dirList.getBytes();
			rc = len > dirList.length() - index ? dirList.length() - index : len;

			cntOut.writeDATA_HEADER();

			ostream.writeDATA_BLOCK(data, index, (int) rc);

			ostream.writeDATA_TRAILER();

			return (int) rc;
		}


		/*
		 * create a string containing entries in following format:
		 *
		 * <pnfsid>:<type>:<namelen>:<name>\n
		 *
		 * where type is 'f' for a file and 'd' for a directory
		 */

		private String createDirEnt(FsInode dirInode) throws ChimeraFsException {

			StringBuilder sb = new StringBuilder();

			if (!dirInode.exists()) {
				sb.append("Path " + dirInode.toString() + " do not exist.");
			} else {

                            if (dirInode.isDirectory()) {
                                DirectoryStreamB<HimeraDirectoryEntry> dirStream = dirInode.newDirectoryStream();
                                try {
                                    for (HimeraDirectoryEntry entry : dirStream) {
                                        FsInode inode = dirInode.inodeOf(entry.getName());
                                        sb.append(inode.toString());
                                        if (inode.isDirectory()) {
                                            sb.append(":d:");
                                        } else {
                                            sb.append(":f:");
                                        }
                                        sb.append(entry.getName().length()).append(':').
                                                append(entry.getName()).append('\n');
                                    }
                                } finally {
                                    try {
                                        dirStream.close();
                                    } catch (IOException e) {
                                        throw new ChimeraFsException(e.getMessage());
                                    }
                                }
                            }
			}

			esay(sb.toString());
			return sb.toString();

		}

	} // end of private class

}
