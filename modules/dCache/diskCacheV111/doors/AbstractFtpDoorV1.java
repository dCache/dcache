//$Id: AbstractFtpDoorV1.java,v 1.85.2.27 2007-10-08 07:57:31 behrmann Exp $
//$Log: not supported by cvs2svn $
//Revision 1.85.2.26  2007/09/10 19:58:19  patrick
//BUG FIX : bracket missing (compile error)
//
//Revision 1.85.2.25  2007/09/07 00:27:57  radicke
//get rid of FileNotFounds related to an unspecified transaction log root dir
//
//Revision 1.85.2.24  2007/08/14 18:58:55  behrmann
//Fixed problem with leaking FTP doors. Rogue InterruptedExceptions could
//potentially break the shutdown sequence, leaving old threads hanging. We
//now make sure that the command poller thread has shut down before
//cleaning up after the transfer.
//
//Revision 1.85.2.23  2007/08/03 12:14:39  radicke
//added try-finally for clean removal of ftp cell in case of unexcepted exception (client goes away). This could have caused the ftp doors dying.
//
//Revision 1.85.2.22  2007/05/29 21:31:08  podstvkv
//Fix the default value for metaDataProvider
//
//Revision 1.85.2.21  2007/05/29 20:32:28  podstvkv
//Missing thread join for ActiveAdapter added
//
//Revision 1.85.2.20  2007/05/22 23:24:39  podstvkv
//The ActiveAdapter added. It is controlled by "allow-relay" flag.
//
//Revision 1.85.2.19  2007/04/02 18:18:59  tigran
//from Gerd Behrmann (NDGF) <behrmann@ndgf.org>:

//Here is a patch for the FTP door and GFtp/1 mover for 1.7.0. It fixes the problem, that the active transfer page in the dCache monitor always shows "No mover found" for GFtp/1 transfers. The problem is caused by the FTP door using __counter for assigning IDs to the mover, but sessionId for reporting IDs to the monitor. So the monitor cannot link transfers at the doors with movers.

//The patch also fixes a small bug in the mover, that would otherwise cause the mover to never report a transfer time while the transfer is in progress. I suspect this to be the reason why the monitor always shows a transfer speed of 0.

//The PnfsHandler to not log exceptions generated in case of non-zero return codes. Name space operations are quite normal to fail (e.g. attempting to create a directory which already exists).

//Revision 1.85.2.18  2007/03/29 16:31:18  tigran
//stop commandPuller thread when connection closed

//Revision 1.85.2.17  2007/03/29 13:01:54  tigran
//better error message

//Revision 1.85.2.16  2007/03/27 13:11:04  tigran
//fixed partial fix of incorrect behavior of the door on a client dicsonnect

//Revision 1.85.2.15  2007/03/18 07:59:25  timur
//fail the transfer if there are errors in the SocketAdapter, new flag to debug SocketAdapter

//Revision 1.85.2.14  2007/03/10 01:02:07  podstvkv
//Close socket on cleanup

//Revision 1.85.2.13  2007/02/21 15:06:44  tigran
//Uptime in seconds

//Revision 1.85.2.12  2007/02/21 15:04:15  tigran
//backport from 1.7.1:
//fixed incorrect door behavior in client disconnect:

//when door requesting a transfer pool from poolmanager, then loop around sendAndWait was putting door into single thread and cilents disconnect was not
//handled. as a result: if file for some reasons unaccessable, then continus request for the same file was blocking all available slots ( DoS )

//Revision 1.85.2.11  2007/02/20 16:02:08  tigran
//no stacktrace on removeing of unexisting files

//Revision 1.85.2.10  2007/01/31 08:47:40  tigran
//from Gerd Behrmann (NDGF):
//make use of 'askForFile' method in ac_store and ac_rert
//as a result: 'transfer initiator' defined for read as well as for writes

//Revision 1.85.2.9  2007/01/05 11:43:41  radicke
//backport of FileMetaDataSource from cvs-head:

//This allows to configure the provider of FileMetadata (mounted Pnfs or PnfsManager). GridFTP-door had to be changed to make use of the new provider (using to mounted pnfs).

//Revision 1.85.2.8  2006/11/30 14:40:22  tigran
//stop 	perfMarkerEngine  on exit
//user DN in info

//Revision 1.85.2.7  2006/10/13 18:42:04  patrick
//BUG FIX : xfer mode sent to pool

//Revision 1.85.2.6  2006/10/13 17:48:29  tigran
//fixed bug with data corruption with parallel streams

//Revision 1.85.2.5  2006/10/12 12:35:08  tigran
//use client real IP during selection

//Revision 1.85.2.4  2006/10/04 14:34:59  tigran
//corrected CLOSE_WAIT handling
//fixed SECURITY BUG:
//list of nonpnfs paths is not allowed

//Revision 1.85.2.3  2006/09/29 12:48:17  tigran
//ose control channel if client gone
//( still CLOSE_WAIT problems )

//Revision 1.85.2.2  2006/09/13 09:32:21  patrick
//Be slighty more picky with the kpwd file

//Revision 1.85.2.1  2006/09/07 07:37:23  tigran
//added VOMS support for dcap ( including extended proxy )

//Revision 1.85  2006/07/11 14:57:24  tigran
//do not check for kpwd file if gPlazma is enabled

//Revision 1.84  2006/06/06 20:45:00  tdh
//Added parsing of use-gplazma-authorization-cell from batch file to turn on use of gplazma cell for authentification.

//Revision 1.83  2006/05/17 12:23:30  tigran
//added exceptions for parent do not exist and parent is not a directory

//Revision 1.82  2006/05/16 15:14:44  tigran
//added file exists exception
//TODO: exceptions  not a dir, parent not exist and so on

//Revision 1.81  2006/04/20 10:30:03  tigran
//added path into door request info
//added initiator intp FileIO message

//Revision 1.80  2006/03/14 19:44:08  kennedy
//Fix response if DELE a directory error occurs to RFC959 specs, and use reply()

//Revision 1.79  2006/03/14 19:26:52  kennedy
//rmdir and chmod working. chmod does not work with symlinks and only supports octal perms as an argument

//Revision 1.78  2006/03/08 15:13:48  kennedy
//Draft imple of FTP <site chmod>. Does not treat symlinks yet.

//Revision 1.77  2006/03/06 15:48:33  kennedy
//Stub out SITE CHMOD implementation

//Revision 1.76  2006/03/06 13:47:18  kennedy
//Implement RMD (rmdir) for FTP doors

//Revision 1.75  2006/01/25 16:43:45  patrick
//Cost mechnism fixed for multi io queues.

//Revision 1.74  2006/01/12 17:10:52  tigran
//set _pnfsEntryIncomplete = false prior sending 226 to the client

//Revision 1.73  2005/12/13 16:53:55  kennedy
//Fix typo introduced after compilation, before editor save

//Revision 1.72  2005/12/13 16:45:02  kennedy
//Clean up Pnfs entry for cases where the command channel is dropped in mid-transfer. Fix ABOR to return 426/226 per RFC 959

//Revision 1.71  2005/11/22 10:59:30  patrick
//Versioning enabled.

//Revision 1.70  2005/11/09 23:59:53  timur
//one more time when we do not need to change the mode

//Revision 1.69  2005/11/03 20:38:46  timur
//do not reset the reply type (clear, enc, misc or conf) when sending performance markers

//Revision 1.68  2005/11/02 21:47:23  aik
//Change PerfMarkerConf class to protected. Java 1.5 had have complains.

//Revision 1.67  2005/10/26 23:49:13  aik
//Fix transfer counters in EBlock-receive mode

//Revision 1.66  2005/10/26 17:56:40  aik
//GFtp performance markers implementation.

//Revision 1.65  2005/10/12 18:13:41  kennedy
//Remove vestigial _moverThread code not used any longer

//Revision 1.64  2005/10/04 18:01:10  kennedy
//Do not print EOF Exception for Socket closed as an error. It almost never is a real error. Also, do not print out a user password if the command involved is a PASS command... not even at test level.

//Revision 1.63  2005/09/27 21:46:51  timur
//do not leave pnfs entry behind after space reservation is created

//Revision 1.62  2005/09/21 14:07:24  tigran
//added client ip into billing

//Revision 1.61  2005/09/14 17:20:31  kennedy
//Support ALLO command as a no-op for now. Explicitly check for and prevent DELE on directories. Most clients convert <rm mydir> to a remove directory command, so we did not see this situation before. One client at least does not, so we need to protect against that case.

//Revision 1.60  2005/09/14 14:12:15  tigran
//fixed copy/paste error
//added _dnUser for GSS/GSI

//Revision 1.59  2005/09/14 09:31:28  tigran
//DoorRequestMessage sent on read

//Revision 1.58  2005/09/14 09:09:22  tigran
//added owner, uid, gid into DoorRequestMessage
//Ftp door send door request
//TODO: send door request on read for FTP door

//Revision 1.57  2005/08/24 17:31:17  kennedy
//As reported by PF, Default useEncp has to be false now

//Revision 1.56  2005/08/22 20:33:33  kennedy
//Remove outdated (and now wrong) advice about using --encp-put not specified.

//Revision 1.55  2005/08/19 20:20:26  litvinse
//implemented multi io-queue functionality

//Revision 1.54  2005/06/22 02:10:31  kennedy
//Corrected fix to socket leak; cannot re-use adapter for multiple gets without some work

//Revision 1.53  2005/06/21 18:53:35  kennedy
//Correct spelling of a few replies to clients

//Revision 1.52  2005/06/21 15:01:09  kennedy
//forward patch meticulous nulling of closed file/socket handles

//Revision 1.51  2005/06/17 22:06:27  timur
//space reservation related changes (more monitoring) , better error handling in gridftp door  STOR method

//Revision 1.50  2005/06/17 20:21:24  kennedy
//back out this change. gate.close is unrelated to resources

//Revision 1.49  2005/06/17 18:27:23  kennedy
//Close readyGate, and be careful to null out possibly resource related references

//Revision 1.48  2005/06/17 14:35:15  kennedy
//Fix MAJOR socket/file-descr leak in ac_pasv(). Only allocate a new passive adapter if one does not already exist. Also, fix a minor superflous getOutputStream() call in active mode section of list(). With my FTP client, every list was preceded by a PASV, hence door went down due to lack of descriptors after about 2500 ls commands.

//Revision 1.47  2005/06/02 08:38:10  patrick
//io-queue options added

//Revision 1.46  2005/05/20 16:51:31  timur
//adding optional usage of vo authorization module

//Revision 1.45.2.1  2005/06/17 21:29:59  timur
//space management related improvent in ftp door

//Revision 1.45  2005/05/19 05:55:43  timur
//added support for monitoring door state via dcache pages

//Revision 1.44  2005/05/10 12:44:46  tigran
//empty or undefined  ftp-adapter-internal-interface
//uses default value ( hostname)

//Revision 1.43  2005/05/04 20:21:12  timur
//back to long format by default

//Revision 1.42  2005/05/03 16:33:22  timur
//use long listing format only when -l option is specified

//Revision 1.41  2005/04/30 05:08:04  kennedy
//Fix globbing for relative paths

//Revision 1.40  2005/04/28 13:49:54  tigran
//changed: passive send interface name whete to clinet connected
//added: '-ftp-adapter-internal-interface' option to force ftpAdapter
//to use other interface in communication with pools

//Revision 1.39  2005/04/27 19:49:38  timur
//make gridftp list command more tollerant of the additional options

//Revision 1.38  2005/04/27 18:52:36  tigran
//removed strange url ( copy-paste error? )

//Revision 1.37  2005/03/23 18:17:19  timur
//SpaceReservation working for the copy case as well

//Revision 1.36  2005/03/10 23:12:08  timur
//Fisrt working version of space reservation module

//Revision 1.35  2005/03/09 23:24:04  timur
//new space reservation code, added mover kill on abort, and switching to active mode on port command

//Revision 1.34  2005/01/04 20:16:53  timur
//use pnfs timeout in seconds

//Revision 1.33  2005/01/04 20:13:04  timur
//use pnfs timeout

//Revision 1.32  2004/09/09 20:27:37  timur
//made ftp transaction logging optional

//Revision 1.31  2004/09/08 21:25:43  timur
//remote gsiftp transfer manager will now use ftp logger too, fixed ftp door logging problem

//Revision 1.30  2004/08/19 18:22:28  timur
//gridftp door gives pool a host name instead of address, reformated code

//Revision 1.29  2004/06/25 12:11:27  patrick
//ftp client port range limitation added.

//Revision 1.28  2004/06/16 13:26:14  tigran
//move to FsPermissionHandler
//fixed wrong permission report on nlist

//Revision 1.27  2004/06/03 12:33:08  tigran
//added mkdir rmdir
//PnfsHandler.deleteEntry throws exception in case of non empty directory

//Revision 1.26  2004/06/03 00:09:01  timur
//fixed LIST and NLST commands, modified pattern matching to match unix command line file name matching

//Revision 1.25  2004/04/16 12:04:45  cvs
//canRead with storageInfo

//Revision 1.24  2004/04/07 15:29:40  timur
//use dcache logging mechanism in SocketAdapter

//Revision 1.23  2004/02/24 21:43:59  timur
//corrected the syncronization between changing the state to running and receiving the transfer completed message

//Revision 1.22  2003/12/08 17:34:05  cvs
//added canDelete() to permissionHandler, use it in ftp door

//Revision 1.21  2003/12/01 18:05:09  cvs
//timur, do not check the existance of the path in the local pnfs if we are not using encp-put script in case of retr and delete, it should now work if pnfs is not mounted at all

//Revision 1.20  2003/11/09 19:52:53  cvs
//first alfa version of srm v2  space reservation functions is complete

//Revision 1.19  2003/10/24 21:11:55  cvs
//added creation of pnfs entry in stor in case of usage of encp script for varification of permission

//Revision 1.18  2003/10/23 20:28:03  cvs
//moved the set of checksum to the place where the pnfs entry is created

//Revision 1.17  2003/10/23 16:18:33  cvs
//new checksum varification code by Igor Mandrichenko

//Revision 1.16  2003/10/03 14:27:18  cvs
//always create permissionHandler

//Revision 1.15  2003/09/30 21:25:21  cvs
//timur: added option to run ftp doors without using encp scripts, added ability to create dirs in pnfs by pnfs manager, added permission handler

//Revision 1.14  2003/09/26 20:18:51  cvs
//timur : fixed a bug related to not reseting values of prm_... varibles after each retrieve

//Revision 1.13  2003/09/25 16:52:04  cvs
//use globus java cog kit gsi gss library instead of gsint

//Revision 1.12  2003/09/02 21:06:19  cvs
//removed logging of security info by kftp and gsiftp doors, changest in srm

//Revision 1.11  2003/07/11 22:16:13  cvs
//timur:  adding code for copy manager

//Revision 1.10  2003/07/08 16:35:45  cvs
//timur: adding the support for gridftp commands ERET and ESTO,
//FEAT will report the features correctly,
//srm copy will use native java gridftp client code to perform
//transfers directly to or from the pools.

//Revision 1.8  2003/07/03 23:38:33  cvs
//error message

//Revision 1.7  2003/07/02 20:16:56  cvs
//AbstractFtpDoorV1.java

//Revision 1.6  2003/06/25 19:59:16  cvs
//fixed the synchronization problem causing "503 Transfer in Progress" to be returned after the transfer is completed

//Revision 1.5  2003/06/12 22:39:30  cvs
//added code to allow an individual user to be read-only

//Revision 1.4  2003/06/11 18:02:18  cvs
//ivm: Implemented the limit for number of data connections per transfer
//ivm: Corrected handling of EODC

//Revision 1.3  2003/05/12 19:26:19  cvs
//create worker threads from sublclasses

//Revision 1.2  2003/05/07 17:44:23  cvs
//new ftp doors are ready

//Revision 1.1  2003/05/06 22:10:48  cvs
//new ftp door classes structure

//Revision 1.25  2003/04/10 22:01:50  cvs
//dcache authorization changes

//Revision 1.24  2003/04/10 20:50:05  cvs
//fixed pathing code needed to allow / to be the virtual root

//Revision 1.23  2003/03/27 22:35:14  cvs
//added code to allow for subdirectories to be listed

//Revision 1.22  2003/03/27 00:05:04  cvs
//k

//Revision 1.21  2003/03/20 21:57:25  cvs
//moved _blockMessage test to a more precise location to try and
//eliminate a cdf/sam error

//Revision 1.20  2003/03/07 22:20:15  cvs
//added ability to ls files with a mask

//Revision 1.19  2003/02/06 21:49:59  cvs
//fixed bug (not edding \r\n into the base64 encodded token from gsint)

//Revision 1.18  2003/02/05 22:09:41  cvs
//removed CR-LF patch because it was wrong.  println already adds this

//Revision 1.17  2003/02/05 13:58:37  cvs
//ftp commands fixed to return CR-LF instead of just CR

//Revision 1.16  2002/11/25 17:01:25  cvs
//changed pbsz and prot to public to allow access by reflected tools.

//Revision 1.15  2002/11/20 17:39:51  cvs
//added fermilab license

//Revision 1.14  2002/11/15 22:08:39  cvs
//Added code to set file permissions

//Revision 1.13  2002/11/15 15:41:15  cvs
//added code to do propose allocation accounting

//Revision 1.12  2002/10/28 23:03:21  cvs
//use cell say instead of System.out

//Revision 1.11  2002/10/16 22:45:14  cvs
//sbuf additions and a security patch for gsi authentication

//Revision 1.10  2002/09/24 18:29:39  cvs
//removed superfluous file size entry in log file

//Revision 1.9  2002/09/19 19:48:24  cvs
//fixed logging problem that prevented filename on reads and file size on some writes from being properly logged

//Revision 1.8  2002/08/29 17:36:59  cvs
//added ftplogging to ac_stor

//Revision 1.7  2002/08/28 16:22:29  cvs
//Fixed pathing problem in ac_stor, fixed threading problem in SenderThread

//Revision 1.6  2002/06/28 00:30:23  cvs
//preliminary gsi/mode E support

//Revision 1.5  2002/06/20 17:58:51  cvs
//Initial release of gftp with write caching and either kerberos or weak authentication

//Revision 1.4  2002/06/06 16:11:16  cvs
//Support for parallel grid ftp transfers

//Revision 1.3  2002/04/09 21:37:54  cvs
//Added code to fix ownership problem

//Revision 1.2  2002/04/01 23:05:38  cvs
//Working copy.  Still stuff to do with uploads

//Revision 1.1  2002/03/27 00:36:09  cvs
//Added door that speaks gridFTP

//Revision 1.4  2002/03/15 16:14:15  cvs
//Fixed parallel file transfers

//Revision 1.3  2002/03/14 19:30:43  cvs
//Added cached writes

//Revision 1.2  2002/02/25 20:28:07  cvs
//Updated to delete based on permissions of the parent of the file rather than the file itself.  Corrects problem where a file is owned by root, but resides in a directory that the user should be able to delete from

//Revision 1.1  2002/02/19 20:30:04  cvs
//Added new files for fermilab k5 authentication

//Revision 1.8.2.2  2001/12/07 20:51:06  wellner
//file deletion changes in 1.4 branch

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

package diskCacheV111.doors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.movers.GFtpPerfMarkersBlock;
import diskCacheV111.services.FileMetaDataSource;
import diskCacheV111.services.FsPermissionHandler;
import diskCacheV111.util.ActiveAdapter;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.DirNotExistsCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.ProxyAdapter;
import diskCacheV111.util.SocketAdapter;
import diskCacheV111.util.UptimeParser;
import diskCacheV111.util.UserAuthBase;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.GFtpProtocolInfo;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsSetLengthMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.spaceManager.SpaceManagerGetInfoAndLockReservationByPathMessage;
import diskCacheV111.vehicles.spaceManager.SpaceManagerUnlockSpaceMessage;
import diskCacheV111.vehicles.spaceManager.SpaceManagerUtilizedSpaceMessage;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.SyncFifo2;
import dmg.util.Args;
import dmg.util.CommandExitException;
import dmg.util.Gate;
import dmg.util.StreamEngine;

/**
 * @author Charles G Waldman, Patrick, rich wellner, igor mandrichenko
 * @version 0.0, 15 Sep 1999
 */
public abstract class AbstractFtpDoorV1 extends CellAdapter implements Runnable    {

    private boolean useEncpScripts=false;

    private int  _lowDataListenPort  = 0 ;
    private int  _highDataListenPort = 0 ;

    protected StreamEngine   _engine;
    protected BufferedReader _in;
    protected PrintWriter    _out;
    protected String         _local_host;
    protected String         _client_data_host;
    protected String         _dnUser = null;
    protected Thread         _workerThread;
    protected Gate           _readyGate      = new Gate(false);
    protected int            _commandCounter = 0;
    protected String         _lastCommand    = "<init>";
    protected Reader         _reader         = null;
    private String           _poolProxy      = null ;

    protected Hashtable      _statusDict = new Hashtable();
    protected Method         _methods[]  = getClass().getMethods();
    protected Hashtable      _methodDict = new Hashtable();
    protected PnfsHandler    _pnfs       = null ;
    protected FsPermissionHandler _permissionHandler = null;
    protected boolean        _needUser=true;
    protected boolean        _needPass=true;

    //XXX this should get set when we authenicate the user
    protected String         _user       = "nobody";
    protected int            _client_data_port       = 20;
    protected Socket         _dataSocket = null;
    // added for the support or ERET with partial retrieve mode

    protected long prm_offset=-1;
    protected long prm_size = -1;


    protected long           _skipBytes  = 0;
    protected String _poolManagerName = "PoolManager";
    protected String _pnfsManagerName = "PnfsManager";
    protected CellPath _poolManagerPath = new CellPath(_poolManagerName);

    protected String _storageGroup = "user" ; //default value


    //XXX arbitrary constants below
    protected int __maxRetries         =      3 ;
    protected int __poolManagerTimeout =   1500 ;
    protected int __pnfsTimeout        = 1 * 60 ; // Increase from 15 sec in <= v1.6.6-1
    protected int __poolTimeout        = 5 * 60 ;
    protected int __retryWait          =     30 ;

    // transferTimeout (in secs)
    // is used for waiting for the end of transfer after the pool
    // already notified us that the file transfer is finished
    // this is needed because we are using adapters etc...
    // if 0 wait without a timeout
    protected int transferTimeout  = 0;
    protected static long   __counter = 10000 ;
    protected static Runtime _runtime = Runtime.getRuntime();

    protected String  _path  = null ;
    protected boolean _pnfsEntryIncomplete = false; // True: pnfsEntry has been created for _path, but the transfer has not yet successfully completed
    protected boolean _isPut = false ;
    protected boolean ConfirmEOFs = false;

    protected String _BufRoot;
    protected String _EncpPutCmd = null;
    protected String _FtpErrorMsg = null;
    protected String _TLogRoot = null;
    protected UserAuthBase _PwdRecord = null;
    protected String _PathRoot = null;
    protected String _CurDirV = null;
    protected String _PnfsFsRoot = null;
    protected File   _TempFile = null;
    protected String _XferMode = "S";
    protected boolean _ReadOnly = false;

    //generalized kpwd file path used by all flavors
    protected String _KPwdFilePath = null;

    //if use_gplazmaAuthzModule is true, then the door will consult
    //authorization module and use its policy configuration, else
    //door keeps using kpwd as in past
    //if use_gplazmaAuthzCell is true, the door will first contact
    //the GPLAZMA cell for authentification
    protected boolean use_gplazmaAuthzCell=false;
    protected boolean use_gplazmaAuthzModule=false;
    protected String gplazmaPolicyFilePath = null;

    // can be "mic", "conf", "enc", "clear"
    protected String GReplyType = "clear";

    /*
     * Enumeration for representing the connection mode.
     *
     * In PASSIVE transfers the client connects directly to the
     * mover at the pool.
     *
     * In ACTIVE transfers the mover at the pool connects directly
     * to the client.
     *
     * In PROXY transfers the mover at the pool and the client
     * connect to a SocketAdapter running at the GridFTP door.
     * 
     * In RELAY transfers the mover at the pool connects to the ActiveAdapter
     * running at the GridFTP door, and the adapter connects to the client
     * 
     */
    private final static class Mode
    {
        public final static int PASSIVE = 0;
        public final static int ACTIVE  = 1;
        public final static int PROXY   = 2;
        public final static int RELAY   = 3;
    }
//  protected boolean _passiveMode = false;
    protected int _mode = Mode.ACTIVE;
    protected ProxyAdapter _adapter = null;
    protected FTPTransactionLog _TLog;

    /** 
     * True if active adapter is allowed, i.e. the client connects
     * to the new proxy adapter at the door, when the pools are on the private
     * network, for example. 
     * Has to be set via arguments
     */
    protected boolean _allowRelay = false;

    //These are the number of parallel streams to have
    //when doing mode e transfers
    protected int _parallelStart = 5;
    protected int _parallelMin = 5;
    protected int _parallelMax = 5;
    protected int _bufSize = 0;
    protected int _maxStreamsPerClient = -1;	// -1 = unlimited
    protected String ftpDoorName="FTP";
    protected String	_checkSum = null;
    private String _base;

    /** @todo breadcrumb - Perf Markers  */
    // GridFtp Performance Markers
    protected PerfMarkerConf _perfMarkerConf = new PerfMarkerConf();
    protected class PerfMarkerConf {
        protected boolean use;
        protected long    period;
        PerfMarkerConf(){
            use    = false;
            period = 3*60*1000L; // default - 3 minutes
        }
    }
    private PerfMarkerEngine     _perfMarkerEngine  = null;

    // if space_reservation_enabled is true, then the door will consult srmv2
    // module to check if the transfer is performed into the
    // space that has been prealocated by the user
    protected boolean space_reservation_enabled=false;

    // this variable consulted only if space_reservation_enabled is true
    // if  space_reservation_strict is true
    // then the transfer that was not precceded by the space allocation
    // will fail
    protected boolean space_reservation_strict=false;

    //this is the info about the corresponding space reservation
    // for current store operation
    SpaceManagerGetInfoAndLockReservationByPathMessage spaceReservationInfo=null;
    //srm timeout in seconds
    protected volatile boolean _transferStarted = false;
    protected volatile boolean _transferInProgress = false;
    protected String transferState;
    protected int spaceManagerTimeout        = 5 * 60 ;
    private Integer moverId =null;
    private String pool = null;
    private PnfsId pnfsId;
    private long sessionId;
    private long transferStartedAt;
    private String  _ioQueueName      = null ;

    // keep billing happy
    protected DoorRequestInfoMessage _info   = null ;
    // remove tiles on incomplete transfers
    private boolean _removeFileOnIncompleteTransfer = false;
    private boolean debugSocketAdapter = false;
    //
    //Use initializer to load up hashes.
    //
    {
        for (int i=0; i<_methods.length; ++i){
            String name = _methods[i].getName();
            if (name.regionMatches(false, 0, "ac_", 0, 3)){
                _methodDict.put(name.substring(3), _methods[i]);
            }
        }
    }

    private synchronized static long next(){
        return __counter++ ;
    }

    public static CellVersion getStaticCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.85.2.27 $" ); }

    public void SetTLog(FTPTransactionLog tlog) {
        say("Setting _TLog");
        //XXX See IVM for how this is supposed to work
        //passive retrieves are reseting tlog before it can be used in some cases
        if( tlog == null) {
            say("Not setting _TLog to null because it seems to screw things up");
        }
        else
            _TLog = tlog;
        //         try {
        //             throw new Exception();
        //         }
        //         catch(Exception ex) {
        //             ex.printStackTrace();
        //         }

    }

    //
    // ftp flavor specific initialization is done in initFtpDoor
    // initFtpDoor is called from the constractor
    //
    public AbstractFtpDoorV1(String name, StreamEngine engine, Args args) throws Exception{
        super( name , args ,  false );

        _engine   = engine;
        _reader   = engine.getReader();
        if(_reader == null) {
            esay(" !!!! engine.getReader() returned null !!!!");
            start();
            kill();
            throw  new
            IllegalStateException(" !!!! engine.getReader() returned null !!!!");
        }
        _in       = new BufferedReader( _reader );
        _out      = new PrintWriter( engine.getWriter() );
        _client_data_host     = engine.getInetAddress().getHostName();

        say( "client hostname in the constructor: " + _client_data_host );

        // REMOVE_dataSocket = ((dmg.protocols.telnet.TelnetStreamEngine)engine).getSocket();
        // REMOVE_port = _dataSocket.getPort();

        String curDirString = null ;
        String problem      = null ;
        try{
            //
            // first of all the options we need ( so not really options )
            //
            debugSocketAdapter = args.getOpt("debugSocketAdapter") != null;
            if( args.getOpt("use-gplazma-authorization-cell") != null) {
                use_gplazmaAuthzCell=
                    args.getOpt("use-gplazma-authorization-cell").equalsIgnoreCase("true");
            }

            if( args.getOpt("use-gplazma-authorization-module") != null) {
                use_gplazmaAuthzModule=
                    args.getOpt("use-gplazma-authorization-module").equalsIgnoreCase("true");
            }

            if (use_gplazmaAuthzModule) {
                if( ( gplazmaPolicyFilePath = args.getOpt("gplazma-authorization-module-policy") ) == null ){
                    problem = "FTPDoor : -gplazma-authorization-module-policy file not specified" ;
                    esay( problem ) ;
                    throw new
                    IllegalArgumentException( problem ) ;
                }
            }

            if( !(use_gplazmaAuthzModule || use_gplazmaAuthzCell)  ) {
                // use kpwd file if gPlazma is not enabled
                if( ( ( _KPwdFilePath = args.getOpt("kpwd-file") ) == null ) ||
                      ( _KPwdFilePath.length() == 0                        ) ||
                      ( !  new File(_KPwdFilePath).exists()                )    ){
                    problem = "FTPDoor : -kpwd-file not specified or file not found" ;
                    esay( problem ) ;
                    throw new
                    IllegalArgumentException( problem ) ;
                }                
            }

            if( ( _EncpPutCmd = args.getOpt("encp-put") ) != null ){

                problem = "FTPDoor : -encp-put is specified. This is DEPRICATED, due to intermittent failures." ;
                esay( problem ) ;
                useEncpScripts=true;
                //throw new
                //IllegalArgumentException( problem ) ;
            }

            if( ( args.getOpt("read-only") ) != null ){
                _ReadOnly = args.getOpt("read-only").equals("true");
            }

            // transferTimeout (in secs)
            // is used for waiting for the end of transfer after the pool
            // already notified us that the file transfer is finished
            // this is needed because we are using adapters etc...
            // if 0 wait without a timeout
            if( args.getOpt("transfer-timeout")  != null) {
                transferTimeout = Integer.parseInt(args.getOpt("transfer-timeout"));
            }

            // if space_reservation_enabled is true, then the door will consult srmv2
            // module to check if the transfer is performed into the
            // space that has been prealocated by the user

            if( args.getOpt("space-reservation") != null) {
                space_reservation_enabled=
                    args.getOpt("space-reservation").equalsIgnoreCase("true");
            }

            // this variable consulted only if space_reservation_enabled is true
            // if  fail_transfers_without_space_reservation is true
            // then the transfer that was not precceded by the space allocation
            // will fail

            if(space_reservation_enabled && args.getOpt("space-reservation-strict") != null) {
                space_reservation_strict=
                    args.getOpt("space-reservation-strict").equalsIgnoreCase("true");
            }

            if( ( args.getOpt("tlog") != null)  &&  ! "".equals(args.getOpt("tlog") ) ){
                _TLogRoot = args.getOpt("tlog");
            }
            
            _base = args.getOpt("root");

            // get the intenal interface name for ftpAdapter-pool internal communication
            _local_host = args.getOpt("ftp-adapter-internal-interface");
            if( (_local_host == null ) || (_local_host.equals("") ) ){
                _local_host = engine.getLocalAddress().getHostName();
            }
            say(_local_host + " used as internal interface for FTPAdapter.");

            if (args.getOpt("allow-relay") != null) {
                _allowRelay = Boolean.valueOf(args.getOpt("allow-relay")).booleanValue() ;
                _mode = (_allowRelay) ? Mode.RELAY : Mode.ACTIVE;
            }

            // remove files on connection close
            _removeFileOnIncompleteTransfer = Boolean.valueOf( args.getOpt("deleteOnConnectionClosed") ).booleanValue();            
            say("removeFileOnIncompleteTransfer=" + _removeFileOnIncompleteTransfer );

            __maxRetries         = setOption( "maxRetries"  , __maxRetries ) ;
            __poolManagerTimeout = setOption( "poolManagerTimeout" , __poolManagerTimeout ) ;
            __pnfsTimeout        = setOption( "pnfsTimeout" , __pnfsTimeout ) ;
            __retryWait          = setOption( "retryWait"   , __retryWait   ) ;
            __poolTimeout        = setOption( "poolTimeout" , __poolTimeout ) ;
            _maxStreamsPerClient = setOption( "maxStreamsPerClient"
                    , _maxStreamsPerClient   ) ;

            _poolProxy       = args.getOpt("poolProxy");
            say("Pool Proxy "+( _poolProxy == null ? "not set" : ( "set to "+_poolProxy ) ) );

            _ioQueueName = args.getOpt("io-queue") ;
            _ioQueueName = ( _ioQueueName == null ) || ( _ioQueueName.length() == 0 ) ? null : _ioQueueName ;
            say( "IoQueueName = "+(_ioQueueName==null?"<undefined>":_ioQueueName));

            String portRange = getArgs().getOpt("clientDataPortRange") ;
            if( portRange != null ){
                try{
                    int ind = portRange.indexOf(":") ;
                    if( ( ind <= 0 ) || ( ind == ( portRange.length() - 1 ) ) )
                        throw new
                        IllegalArgumentException("Not a port range");

                    int low  = Integer.parseInt( portRange.substring(0,ind) ) ;
                    int high = Integer.parseInt( portRange.substring(ind+1) ) ;

                    _lowDataListenPort  = low ;
                    _highDataListenPort = high ;
                }catch(Exception ee ){
                    esay("Invalid port range string (command ignored) : "+portRange ) ;
                }
            }
            say("Selected client data port range [" + _lowDataListenPort + ":" + _highDataListenPort + "]");



            /*
             * permission handler:
             * use user defined or PnfsManager based
             */
            String metaDataProvider = args.getOpt("meta-data-provider");
            if( metaDataProvider == null ) {
                metaDataProvider = "diskCacheV111.services.PnfsManagerFileMetaDataSource";
            }
            say("Loading " + metaDataProvider);
            Class [] argClass = {
                    dmg.cells.nucleus.CellAdapter.class
            };
            Class fileMetaDataSourceClass = Class.forName(metaDataProvider);
            Constructor fileMetaDataSourceCon   = fileMetaDataSourceClass.getConstructor( argClass ) ;
            Object[] initargs = { this };
            FileMetaDataSource fileMetaDataSource = (FileMetaDataSource)fileMetaDataSourceCon.newInstance(initargs);
            _permissionHandler = new FsPermissionHandler(this, fileMetaDataSource );

        }catch(Exception ee ){

            start() ;
            kill() ;
            throw ee ;
        }

        _pnfs = new PnfsHandler( this, new CellPath( _pnfsManagerName ) ) ;
        _pnfs.setPnfsTimeout(__pnfsTimeout*1000L);

        adminCommandListener = new AdminCommandListener();
        addCommandListener(adminCommandListener);
    }
    
    AdminCommandListener adminCommandListener;
    
    public class AdminCommandListener {

        public String hh_get_door_info = "[-binary]" ;
        public Object ac_get_door_info( Args args ){
            IoDoorInfo info = new IoDoorInfo( getCellName() ,
                    getCellDomainName() ) ;
            info.setProtocol("GFtp","1");
            info.setOwner( _PwdRecord == null ? "0" : Integer.toString(_PwdRecord.UID) ) ;
            //info.setProcess( _pid == null ? "0" : _pid ) ;
            info.setProcess( "0") ;
            IoDoorEntry[] entries;
            if(_transferInProgress) {
                IoDoorEntry entry = new IoDoorEntry(sessionId,pnfsId,pool,transferState,transferStartedAt,_client_data_host);
                entries =  new IoDoorEntry[] {entry};
            } else {
                entries = new IoDoorEntry[0];
            }
            info.setIoDoorEntries( entries );
            if( args.getOpt("binary") != null )
                return info ;
            else
                return info.toString() ;
        }
    }


    private int setOption( String optName , int def ){
        String tmp = getArgs().getOpt(optName) ;
        if( tmp == null )return def ;
        try{
            return Integer.parseInt(tmp) ;
        }catch(Exception e){
            esay( "Unable to set "+optName+" to "+tmp ) ;
            return def ;
        }
    }

    private int spawn(String cmd, int errexit) {
        try {
            Process p = _runtime.exec(cmd);
            p.waitFor();
            int returnCode = p.exitValue();
            p.destroy();
            return returnCode;
        }
        catch( Exception e )
        {	return errexit;	}
    }


    public void ftpcommand(String cmdline) throws dmg.util.CommandExitException {
        int l=4;
        // Every FTP command is 3 or 4 characters
        if (cmdline.length()<3){
            reply(err(cmdline,""));
            return;
        }
        if (cmdline.length()==3 || cmdline.charAt(3)==' '){
            l=3;
        }

        String cmd=cmdline.substring(0,l);
        String arg=cmdline.length()>l+1?cmdline.substring(l+1):"";
        Object args[] = {arg};

        cmd = cmd.toLowerCase();

        //most of the ic is handled in the ac_ functions but a few commands
        //need special handling
        if( cmd.equals("mic" ) || cmd.equals("conf") || cmd.equals("enc") ||
                cmd.equals("adat") || cmd.equals("pass")) {
            tsay("FTP CMD: <" + cmd + " ... >");
        }
        else {
            _lastCommand = cmdline;

            tsay("FTP CMD: <" + cmdline + ">");
        }
        // If a transfer is in progress, only permit ABORT and a few other commands to be processed
        synchronized(this) {
            if (_transferInProgress &&
                    ! ( cmd.equals("abor") ||
                            cmd.equals("mic")  || cmd.equals("conf") || cmd.equals("enc") ) ) {
                reply("503  Transfer in progress",false);
                return;
            }
        }
        if (_methodDict.containsKey(cmd)){
            Method m = (Method)(_methodDict.get(cmd));
            try {
                // most of this info is printed above
                // comment this logging, uncommnet for debugging
                // say("Going to invoke:" + m.getName() +"("+arg+")");
                m.invoke(this, args);
                if (!cmd.equals("rest"))
                    _skipBytes=0;
                return;
            } catch( InvocationTargetException ite ){
                //
                // is thrown if the underlying method
                // actively throws an exception.
                //
                Throwable    te = ite.getTargetException();
                if( te instanceof dmg.util.CommandExitException ){
                    throw (dmg.util.CommandExitException)te;
                }
                te.printStackTrace();
                reply("500 "+ite.toString()+": <"+cmd+">");
                say("Cause: " + te.getCause() );
                _skipBytes=0;
                return;
            } catch (Exception e){
                reply("500 "+e.toString());
                _skipBytes=0;
                return;
            }
        }
        _skipBytes=0;
        reply(err(cmd,arg));
    }

    public void run(){
        if( Thread.currentThread() == _workerThread ){
            
            try {
        	
                reply( "220 "+ftpDoorName+" Door ready");
                AsciiCommandPoller commandPoller = new AsciiCommandPoller(_in, Thread.currentThread() );

                Thread commandPollerThread = new Thread(commandPoller, "commandPollerThread");
                commandPollerThread.start();

                try{
                    while( true ){
                        _lastCommand = commandPoller.nextCommand();
                        if( _lastCommand  == null )break;
                        _commandCounter++;
                                
                        // this prints everything that is also printed in
                        // ftpcommand, therefore it is commented
                        //say( "FROMCLIENT : "+_lastCommand ) ;
                        if( execute( _lastCommand ) > 0 ){
                            //
                            // we need to close the socket AND
                            // have to go back to readLine to
                            // finish the ssh protocol gracefully.
                            //	
                            try{ _out.close(); }catch(Exception ee){}
                            // Do not null _out here. Used in cleanUp()
                        }
                    }

                    /* The command poller will interrupt()
                     * this thread when exiting. This will
                     * cause an InterruptedException to be
                     * thrown, but we cannot know for sure
                     * from where it is thrown: It could be
                     * thrown and caught during command
                     * processing, be propagated and caught
                     * below, or it could be thrown after this
                     * point. 
                     *
                     * To make sure that we do not receive
                     * rogue InterruptedExceptions during the
                     * shutdown processing below, we wait for
                     * the command poller to terminate
                     * completely.
                     *
                     * Although join() may throw an
                     * InterruptedException itself, it will
                     * not do so in case the command poller
                     * thread is already dead. Therefore we
                     * manually clear the interrupted flag of
                     * the current thread.
                     */
                    commandPollerThread.join();
                    Thread.currentThread().interrupted();
                }catch( IOException e ){
                    //This is extremely unlikely to be anything but the client
                    //having already closed the pipe.
                    StringBuffer buf = new StringBuffer();
                    buf.append("EOF Exception in read line : " + e);
                    if( buf.toString().indexOf("null fd object") < 0) {
                        say("EOF Exception in read line : "+e );
                    }
                }catch(InterruptedException ie) {
                    // This is expected behaviour of the FTP
                    // door due to messy shutdown sequence.
                }catch( Exception e ){
                    esay("I/O Error in read line : "+e );
                }

                // 
                if( _perfMarkerEngine != null ) {
                    _perfMarkerEngine.stopEngine() ;
                }
                try{ _out.close(); }catch(Exception ee){}
                // --------------------------------------------------------------------
                // Clean-up in case thread is terminating due to premature socket close
                // --------------------------------------------------------------------
                say("AbstractFtpDoorV1::run(): _pnfsEntryIncomplete=" + _pnfsEntryIncomplete + ", _path="+_path) ;

                if (_path != null && _pnfsEntryIncomplete) {
                    if (_removeFileOnIncompleteTransfer) {
                        say("Removeing incompelete file: " + _path);
                        deleteEntry(_path);
                        _path = null;
                        _pnfsEntryIncomplete = false;
                    } else {
                        say("Incompelete file: " + _path);
                    }
                }
            
        	// open gate whatever happened for clean removal
            } finally {
                say( "EOS encountered" );
                _readyGate.open();
                kill();
            }
        }
    }

    public void   cleanUp(){

        say( "Clean up called" );
        reply("");

        if (_out!=null) {
            try{ _out.close(); }catch(Exception ee){}
        }
        _out=null;
        try {
            if (!_engine.getSocket().isClosed()) {
                say("Close socket");
                _engine.getSocket().close();
            }
        } catch (Exception ee) { ; }

        _readyGate.check();
        if ( _adapter != null ) {
            say( "Closing current adapter" );
            _adapter.close();
            _adapter = null;
        }

        //XXX temp file stuff should all be removed
        try {
            if( _TempFile != null ) {
                say("Deleting temp file " + _TempFile);
                _TempFile.delete();
            }
        }
        catch( Exception e )
        {	}

        _TempFile = null;
    }

    public void println( String str ){
        _out.println( str + "\r");
        _out.flush();
        // this prints everything including encoded replies and
        // is not needed for normal logging
        // can be uncommented for debugging
        //say( "TO CLIENT : "+str ) ;
    }

    public void print( String str ){
        _out.print( str );
        _out.flush();
    }

    public int execute( String command ) throws Exception {
        if( command.equals("") ) {
            reply(err("",""));
            return 0;
        }
        try{
            ftpcommand(command);
            return 0;
        }catch( CommandExitException cee ){
            return 1;
        }
    }

    //
    // the cell implemetation
    //
    public String toString(){ return _user+"@"+_client_data_host; }
    public void getInfo( PrintWriter pw ){
        pw.println( "            FTPDoor" );
        pw.println( "         User  : "+_dnUser == null? _user : _dnUser  );
        pw.println( "    User Host  : "+_client_data_host );
        pw.println( "   Local Host  : "+_local_host );
        pw.println( " Last Command  : "+_lastCommand );
        pw.println( " Command Count : "+_commandCounter );
        pw.println( "     I/O Queue : "+_ioQueueName );

        if(useEncpScripts) {
            pw.println( "    Encp Script: "+_EncpPutCmd);
        }
        pw.println(adminCommandListener.ac_get_door_info(new Args("")));
    }

    //
    // this object is used for synchronization between 1: and 2:
    // 1: code that communicates
    // between pool in case of stor/retr
    // 2:code that hanles e messages received back from pool about transfer
    // status
    //
    //handle post-transfer success/failure messages going back to the client
    public void  messageArrived( CellMessage msg ){


        boolean timed_out = false;
        Object object = msg.getMessageObject();
        say("Message messageArrived ["+object.getClass()+"]="+object.toString());
        say("Message messageArrived source = "+msg.getSourceAddress());
        if (object instanceof DoorTransferFinishedMessage) {

            DoorTransferFinishedMessage reply =
                (DoorTransferFinishedMessage)object ;

            boolean adapterFailed = false;

            synchronized(this) {

                //XXX this is to prevent messages from the pools screwing things up
                //in the case of passive transfers.  The finished message can show up
                //before the door even knows about the transfer on small files.
                // comment by Timur:
                // I observed the finished message arriving before the door had chance
                // to change the status to _transferInProgress=false even in case of
                // the active mode for a small file (100 bytes)
                // the synchronization is essential here
                if(!_transferInProgress) {
                    esay("DoorTransferFinishedMessage arrived: wrong state: _transferInProgress is not true");
                    return;
                }
                if (_mode == Mode.PASSIVE || _mode == Mode.RELAY) {
                    say("DoorTransferFinishedMessage arrived: Waiting for adaptor to finish ...");

                    if (_adapter instanceof ActiveAdapter || reply.getReturnCode() != 0) {
                        _adapter.close();
                    }

                    try {
                        _adapter.join(300000); // 5 minutes
                        if (_adapter.isAlive()) {
                            esay("Killing adapter");
                            _adapter.close();
                            _adapter.join(10000); // 10 seconds
                            if (_adapter.isAlive()) {
                                esay("Failed to kill adapter");
                            }
                            adapterFailed = true;
                        } else {
                            adapterFailed = _adapter.isFailed();
                        }
                    } catch (InterruptedException e) {
                        say("Join error: " + e);
                        adapterFailed = true;
                    }
                }
                else {
                    say("DoorTransferFinishedMessage arrived: Active mode transfer...");
                }
                _transferInProgress = false ;
                _transferStarted = false;
            }

            /** @todo - breadcrumb - messages */
            // Stop performance Markers Engine thread, if it was started.
            if (_perfMarkerEngine != null && _perfMarkerEngine.stopEngine()) {
                say("DoorTransferFinishedMessage arrived: Waiting PerfMarkerEngine for to finish ...");
                while (_perfMarkerEngine.isAlive()) {
                    try {
                        _perfMarkerEngine.getThread().join();
                    } catch (Exception e) {
                        say("PerfMarkerEngine join error: " + e);
                    }
                }
            }

            if( reply.getReturnCode() == 0 && !adapterFailed ) {

                if(space_reservation_enabled && spaceReservationInfo != null) {
                    long utilized = reply.getStorageInfo().getFileSize();
                    say("reply.getStorageInfo().getFileSize()="+utilized);
                    if(utilized > spaceReservationInfo.getAvailableLockedSize()) {
                        utilized = spaceReservationInfo.getAvailableLockedSize();
                    }
                    say("set utilized to "+utilized);
                    SpaceManagerUtilizedSpaceMessage utilizedSpace =
                        new SpaceManagerUtilizedSpaceMessage(spaceReservationInfo.getSpaceToken(),utilized);

                    try {
                        sendMessage(new CellMessage(
                                new CellPath("SpaceManager") ,
                                utilizedSpace
                        ));
                    }
                    catch (Exception e){
                        String errmsg = "Can't send message to SRMV2 "+e;
                        esay(errmsg);
                        esay(e) ;
                    }
                    spaceReservationInfo = null;
                }
                StorageInfo storageInfo = reply.getStorageInfo() ;
                if( _TLog != null ) {
                    _TLog.middle(storageInfo.getFileSize());
                    _TLog.success();
                    SetTLog(null);
                }


                // RDK: Note that data/command channels both dropped (esp. ACTIVE mode) at same time
                //      can lead to a race. The transfer will be declared successful, this flag cleared,
                //      and THEN the command channel drop is reacted to. This is difficult to reproduce.
                //      Treat elsewhere to prevent a successful return code from being returned.
                //	Clear the _pnfsEntryIncomplete flag since transfer successful
                _pnfsEntryIncomplete = false ;                

                String reply_string = "226 Transfer complete.";

                reply(reply_string);
                _info.setResult(0, "");
                this.sendDoorRequestInfo();

            } else {
                // clean up the PNFS entry
                String errorMsg = "426 Transfer aborted, closing connection :";
                if(reply.getReturnCode() != 0) {
                    if(reply.getErrorObject() != null) {
                        errorMsg += reply.getErrorObject();
                    } else {
                        errorMsg += " mover failure";
                    }
                }
                if(adapterFailed) {
                    errorMsg += " Passive adapter failed";
                }
                stor_error(errorMsg);
                _info.setResult(426, errorMsg);
                this.sendDoorRequestInfo();
            }

        } // .end DoorTransferFinishedMessage
        else {
            say("Unexpected message class "+object.getClass());
            say("source = "+msg.getSourceAddress());
        }
    }

    private void tsay(String str) {
        Date d = new Date();
        say("" + d + ": " + str);
    }

    //
    // GSS authentication
    //

    protected void reply(String answer, boolean resetReply) {
        if(answer.startsWith("335 ADAT=")) {
            say("REPLY(reset=" + resetReply + " GReplyType=" + GReplyType + "): <335 ADAT=...>");
        }
        else {
            say("REPLY(reset=" + resetReply + " GReplyType=" + GReplyType + "): <" + answer + ">");
        }
        if ( GReplyType.equals("clear") )
            println(answer);
        else if ( GReplyType.equals("mic") )
            secure_reply(answer, "631");
        else if ( GReplyType.equals("enc") )
            secure_reply(answer, "633");
        else if ( GReplyType.equals("conf") )
            secure_reply(answer, "632");
        if( resetReply )
            GReplyType = "clear";
    }

    protected void reply(String answer) {
        reply(answer, true);
    }

    protected abstract void  secure_reply(String answer, String code);



    public void ac_feat(String arg) {
        reply("211-OK\r\n EOF\r\n PARALLEL\r\n SIZE\r\n SBUF\r\n ERET\r\n ESTO\r\n211 End");
    }

    public void opts_retr(String opt) {
        StringTokenizer st = new StringTokenizer(opt, "=" );
        String real_opt = st.nextToken();
        String real_value= st.nextToken();
        if( !real_opt.equalsIgnoreCase("Parallelism")) {
            reply("501 Unrecognized option: " + real_opt + " (" + real_value + ")");
            return;
        }
        say("real_value: " + real_value );
        st = new StringTokenizer( real_value, ",;");

        _parallelStart = (new Integer( st.nextToken())).intValue();
        _parallelMin = (new Integer( st.nextToken())).intValue();
        _parallelMax = (new Integer( st.nextToken())).intValue();

        reply("200 Parallel streams set (" + opt + ")");

    }

    public void opts_stor(String arg) {
        StringTokenizer st = new StringTokenizer(arg);
        String cmd = st.nextToken();	// STOR
        String opt = st.nextToken();	// EOF
        String val = st.nextToken();	// 1/0
        if ( !opt.equalsIgnoreCase("EOF") ) {
            reply("501 Unrecognized option: " + opt + " (" + val + ")");
            return;
        }
        if ( !val.equals("1") ) {
            ConfirmEOFs = true;
            reply("200 EOF confirmation is ON");
            return;
        }
        if ( !val.equals("0") ) {
            ConfirmEOFs = false;
            reply("200 EOF confirmation is OFF");
            return;
        }
        reply("501 Unrecognized option value: " + val );
    }

    public void ac_opts(String arg) {
        StringTokenizer st = new StringTokenizer(arg);
        String cmd = st.nextToken();	// STOR
        if( cmd.equalsIgnoreCase("RETR")) {
            opts_retr(st.nextToken());
        }
        else if ( cmd.equalsIgnoreCase("STOR") ) {
            opts_stor(arg);
        }
        else {
            reply("501 Unrecognized option: " + cmd + " (" + arg + ")");
            return;
        }
    }

    public void ac_dele(String arg) {
        if( _ReadOnly || _PwdRecord.isWeak() || _PwdRecord.isReadOnly() ) {
            println("500 Command disabled");
            return;
        }

        say("dele called");
        say(arg);
        String pathInPnfs = absolutePath( arg);

        // We do not allow DELE of a directory.
        // Some FTP clients let this slip through, like uberftp client.
        // Some FTP clients detect this and send as an "RMD" request instead.
        File theFileToDelete = new File(pathInPnfs);
        if (theFileToDelete.isDirectory()){
            reply("553 Cannot delete a directory");
            return;
        }

        if(useEncpScripts) {
            String parentOfFile = theFileToDelete.getParent();
            //Check if the file is writable (aka deletable)
            String cmd = _EncpPutCmd + " chkw " +
            _PwdRecord.UID + " " +
            _PwdRecord.GID + " " +
            parentOfFile;
            if( spawn(cmd, 1000) != 0 ) {
                reply("553 Permission denied");
                return;
            }

            cmd = _EncpPutCmd + " rm " +
            _PwdRecord.UID + " " +
            _PwdRecord.GID + " " +
            pathInPnfs;
            if( spawn(cmd, 1000) != 0 ) {
                reply("553 Permission denied (actually permissions looked ok, but the delete failed anyway)");
                return;
            }
        }
        else {
            try {
                if(_permissionHandler.canDelete(_PwdRecord.UID,_PwdRecord.GID,pathInPnfs)) {
                    _pnfs.deletePnfsEntry(pathInPnfs);
                }
                else {
                    reply("553 Permission denied");
                    return;
                }
            }catch( FileNotFoundCacheException fnf ) {
                reply("500 File "+pathInPnfs+" not found");
                return;
            }
            catch(Exception e) {
                esay(e);
                reply("553 Permission denied, reason: "+e);
                return;
            }
        }
        reply("200 file deleted");
    }

    public abstract void ac_auth(String arg);


    public abstract void ac_adat(String arg);

    public void ac_mic(String arg)
    throws dmg.util.CommandExitException {
        secure_command(arg, "mic");
    }

    public void ac_enc(String arg)
    throws dmg.util.CommandExitException {
        secure_command(arg, "enc");
    }

    public void ac_conf(String arg)
    throws dmg.util.CommandExitException {
        secure_command(arg, "conf");
    }

    public abstract void secure_command(String arg, String sectype)
    throws dmg.util.CommandExitException;



    public void ac_ccc( String arg ) {
        // We should never received this, only through MIC, ENC or CONF,
        // in which case it will be intercepted by secure_command()
        reply("533 CCC must be protected");
    }

    public abstract void ac_user( String arg);


    public abstract void ac_pass(String arg);




    public void ac_pbsz(String arg) {
        reply("200 OK");
    }

    public void ac_prot(String arg) {
        if( !arg.equals("C") )
            reply("534 Will accept only Clear protection level");
        else
            reply("200 OK");
    }

    ///////////////////////////////////////////////////////////////////////////
    //                                                                       //
    // the interpreter stuff                                                 //
    //                                                                       //


    private String absolutePath( String relCwdPath) {

        if( _PathRoot == null )
            return null;

        _CurDirV = (_CurDirV == null?"/":_CurDirV);
        FsPath relativeToRootPath = new FsPath(_CurDirV);
        relativeToRootPath.add(relCwdPath);


        FsPath absolutePath = new FsPath(_PathRoot);
        String rootPath = absolutePath.toString();
        absolutePath.add("./"+relativeToRootPath.toString());
        String absolutePathStr = absolutePath.toString();
        say("absolute path is \""+absolutePathStr+"\" root is "+_PathRoot);
        if( !absolutePathStr.startsWith(rootPath) ) {
            say("Didn't start with root");
            return null;
        }
        return absolutePathStr;
    }


    public void ac_rmd( String arg ) {
        if( _ReadOnly || _PwdRecord.isWeak() || _PwdRecord.isReadOnly() ) {
            println("500 Command disabled");
            return;
        }

        if (arg.equals("")){
            reply(err("RMD",arg));
            return;
        }

        if ( _PwdRecord == null ) {
            reply("530 Not logged in.");
            return;
        }

        if ( _PwdRecord.isAnonymous() ) {
            println("554 Anonymous write access not permitted");
            return;
        }

        String pathInPnfs = absolutePath( arg);
        if ( pathInPnfs == null ) {
            reply("553 Cannot determine full directory pathname in PNFS: " + arg);
            return;
        }

        // canDeleteDir() will test that isDirectory() and canWrite()
        try { if(_permissionHandler.canDeleteDir(_PwdRecord.UID,_PwdRecord.GID,pathInPnfs))
        {
            File theDirToDelete = new File(pathInPnfs);
            if ( ( theDirToDelete.list() ).length == 0 )  // Only delete empty directories
            {
                _pnfs.deletePnfsEntry(pathInPnfs);
            }
            else
            {
                reply("553 Directory not empty. Cannot delete.");
                return;
            }
        }
        else
        {
            reply("553 Permission denied");
            return;
        }
        }
        catch(CacheException ce)
        {
            reply("553 Permission denied, reason: "+ce);
            return;
        }

        reply("200 OK");
    }


    public void ac_mkd( String arg ) {
        if( _ReadOnly || _PwdRecord.isWeak() || _PwdRecord.isReadOnly() ) {
            println("500 Command disabled");
            return;
        }
        String ftpErrorMsg = "200 Command OK";

        if (arg.equals("")){
            reply(err("MKD",arg));
            return;
        }

        if ( _PwdRecord == null ) {
            reply("530 Not logged in.");
            return;
        }

        if ( _PwdRecord.isAnonymous() ) {
            println("554 Anonymous write access not permitted");
            return;
        }


        String pathInPnfs = absolutePath( arg);
        if ( pathInPnfs == null ) {
            reply("553 Cannot create directory in PNFS: " + arg);
            return;
        }
        if(useEncpScripts) {
            File x = new File(pathInPnfs);
            if ( x.exists() ) {
                reply("550 " + arg + ": already exists");
                return;
            }


            String cmd = _EncpPutCmd + " chkc " +
            _PwdRecord.UID + " " +
            _PwdRecord.GID + " " +
            pathInPnfs;
            if( spawn(cmd, 1000) != 0 ) {
                reply("553 Permission denied");
                return;
            }

            cmd = _EncpPutCmd + " mkd " +
            _PwdRecord.UID + " " +
            _PwdRecord.GID + " " +
            pathInPnfs;
            if ( spawn(cmd, 1000) != 0 ) {
                reply("552 Error creating directory " + arg);
                return;
            }
        }
        else {
            try {
                if(_permissionHandler.canWrite(_PwdRecord.UID,_PwdRecord.GID,pathInPnfs)) {
                    _pnfs.createPnfsDirectory(pathInPnfs,_PwdRecord.UID,_PwdRecord.GID, 0755);
                }
                else {
                    reply("553 Permission denied");
                    return;
                }
            }
            catch(CacheException ce) {
                reply("553 Permission denied, reason: "+ce);
                return;
            }

        }
        reply("200 OK");
    }

    public void ac_help( String arg) {
        reply("214 No help available");
    }

    public void ac_syst( String arg) {
        reply("215 UNIX Type: L8 Version: FTPDoor");
    }

    public void ac_type( String arg) {
        reply("200 Type set to I");
    }

    public void ac_noop( String arg) {
        reply(ok("NOOP"));
    }

    public void ac_allo( String arg) {
        reply(ok("ALLO"));  // No-op for now. Sent by uberftp client.
    }

    public void ac_pwd( String arg) {
        if (!arg.equals("")){
            reply(err("PWD",arg));
            return;
        }
        reply("257 \""+_CurDirV+"\" is current directory");
    }

    public void ac_cwd( String arg){
        String newcwd = absolutePath( arg);
        if( newcwd == null )
            newcwd = _PathRoot;

        File test = new File(newcwd);

        if (!test.isDirectory()){
            reply("550 "+test.toString()+": Not a directory");
            return;
        }
        _CurDirV = newcwd.substring(_PathRoot.length());
        if( _CurDirV.length() == 0 )
            _CurDirV = "/";
        reply("250 CWD command succcessful. New CWD is <"+_CurDirV+">");
    }

    public void ac_cdup( String arg) {
        ac_cwd("..");
    }

    public void ac_port( String arg) {
        int tok[] = {0,0,0,0,0,0};
        StringTokenizer st = new StringTokenizer(arg, ",");
        if (st.countTokens() != 6){
            reply(err("PORT",arg));
            return;
        }
        for (int i=0; i<6; ++i){
            tok[i] = (new Integer(st.nextToken())).intValue();
        }
        String ip = tok[0]+"."+tok[1]+"."+tok[2]+"."+tok[3];
        _client_data_host = ip;

        // XXX if transfer in progress, abort
        _client_data_port = tok[4]*256 + tok[5];

        // if mode was passive, it is not anymore
        if ( _mode==Mode.PASSIVE && _adapter != null ) {
            say( "Closing PassiveAdapter" );
            _adapter.close();
            _adapter = null;
//          _mode = (_allowRelay) ? Mode.RELAY : Mode.ACTIVE;
        }
        _mode = (_allowRelay) ? Mode.RELAY : Mode.ACTIVE;
        if (_mode==Mode.RELAY) {
            try {
                say("Creating ActiveAdapter");
                _adapter = new ActiveAdapter(this, _lowDataListenPort, _highDataListenPort);
                if(debugSocketAdapter) {
                    _adapter.setDebug(true);
                }
            } catch (IOException e) {
                // XXX What do we do here?
                esay("Cannot create ActiveAdapter!");
                esay(e);
            }
        }

        reply(ok("PORT"));
    }

    public void ac_pasv(String arg) {
        try {
            if (_adapter!=null) { // close sockets if still open
                try { _adapter.close() ; }
                catch (Exception ee) {}
                _adapter=null;
            }
            say("Creating PassiveAdapter");
            _adapter = new SocketAdapter(this, _lowDataListenPort , _highDataListenPort);
            if (debugSocketAdapter) {
                _adapter.setDebug(true);
            }
            int port = _adapter.getClientListenerPort();
            byte[] hostb = _engine.getLocalAddress().getAddress();
            int[] host = new int[4];
            for( int i = 0; i < 4; i++ )
                host[i] = hostb[i] & 0377;
            _mode = Mode.PASSIVE;
            reply("227 OK (" +
                    host[0] + "," +
                    host[1] + "," +
                    host[2] + "," +
                    host[3] + "," +
                    port/256 + "," +
                    port % 256 + ")");
            //_host = host[0]+"."+host[1]+"."+host[2]+"."+host[3];
            //_port = 0;		// will be set by retr/stor
        }
        catch ( IOException e ) {
            _mode = (_allowRelay) ? Mode.RELAY : Mode.ACTIVE;
            reply("500 Cannot enter passive mode: " + e);
            return;
        }
    }

    public void ac_mode(String arg) {
        if ( arg.equalsIgnoreCase("S") ) {
            _XferMode = "S";
            reply("200 Will use Stream mode");
        }
        else if( arg.equalsIgnoreCase("E") ) {
            _XferMode = "E";
            reply("200 Will use Extended Block mode");
        }
        else {
            reply("200 Unsupported transfer mode");
        }
    }

    public void ac_site(String arg) {
        if( arg.equals("")) {
            reply("500 must supply the site specific command");
            return;
        }

        String args[] = arg.split(" ");

        if( args[0].equalsIgnoreCase("BUFSIZE") ) {
            if(args.length != 2) {
                reply("500 command must be in the form 'SITE BUFSIZE <number>'");
                return;
            }
            ac_sbuf(args[1]);
        }
        else if ( args[0].equalsIgnoreCase("CHKSUM") ) {
            if(args.length != 2) {
                reply("500 command must be in the form 'SITE CHKSUM <value>'");
                return;
            }
            doCheckSum(args[1]);
        }
        else if ( args[0].equalsIgnoreCase("CHMOD") ) {
            if(args.length != 3) {
                reply("500 command must be in the form 'SITE CHMOD <octal perms> <file/dir>'");
                return;
            }
            doChmod(args[1], args[2]);
        }
        else {
            reply("500 Unknown SITE command");
            return;
        }
    }


    public void doCheckSum(String value) {
        _checkSum = value;
        reply("200 OK");
    }

    public void doChmod(String permstring, String path) {
        if( _ReadOnly || _PwdRecord.isWeak() || _PwdRecord.isReadOnly() ) {
            println("500 Command disabled");
            return;
        }

        if (path.equals("")){
            reply(err("SITE CHMOD",path));
            return;
        }

        if ( _PwdRecord == null ) {
            reply("530 Not logged in.");
            return;
        }

        String pathInPnfs = absolutePath(path);
        if ( pathInPnfs == null ) {
            reply("553 Cannot determine full directory pathname in PNFS: " + path);
            return;
        }

        int newperms;
        try { newperms = Integer.parseInt(permstring, 8); // Assume octal regardless of string
        }
        catch(NumberFormatException ex)
        { reply("501 permissions argument must be an octal integer");
        return;
        }

        // Get meta-data for this file/directory
        PnfsGetFileMetaDataMessage fileMetaDataMsg = null;
        try { fileMetaDataMsg = _pnfs.getFileMetaDataByPath( pathInPnfs ) ;
        }
        catch( CacheException ce )
        { reply("553 Permission denied, reason: "+ce);
        return;
        }

        // Extract fields of interest
        PnfsId       myPnfsId   = fileMetaDataMsg.getPnfsId();
        FileMetaData metaData   = fileMetaDataMsg.getMetaData();
        boolean      isADir     = metaData.isDirectory();
        boolean      isASymLink = metaData.isSymbolicLink();
        int          myUid      = metaData.getUid();
        int          myGid      = metaData.getGid();

        // Only file/directory owner can change permissions on that file/directory
        if (myUid != _PwdRecord.UID)
        { reply("553 Permission denied. Only owner can change permissions.");
        return;
        }

        // Chmod on symbolic links not yet supported (should change perms on file/dir pointed to)
        if (isASymLink)
        { reply("502 chmod of symbolic links is not yet supported.");
        return;
        }

        FileMetaData newMetaData = new FileMetaData(isADir,myUid,myGid,newperms);

        _pnfs.pnfsSetFileMetaData(myPnfsId,newMetaData) ;

        reply("200 OK");
    }

    public void ac_sbuf(String arg) {
        if(arg.equals("")) {
            reply("500 must supply a buffer size");
            return;
        }

        int bufsize;
        try {
            bufsize = Integer.parseInt(arg);
        }
        catch(NumberFormatException ex) {
            reply("500 bufsize argument must be integer");
            return;
        }

        if( bufsize < 1 ) {
            reply("500 bufsize must be positive.  Probably large, but at least positive");
            return;
        }

        _bufSize = bufsize;
        reply("200 bufsize set to " + arg );
    }

    public void ac_eret(String arg) {
        java.util.StringTokenizer st = new java.util.StringTokenizer(arg);
        if(st.countTokens() < 2) {
            reply(err("ERET",arg));
            return;
        }
        String extended_retrieve_mode=st.nextToken();
        String cmd="eret_"+extended_retrieve_mode.toLowerCase();
        Object args[] = {arg};
        if (_methodDict.containsKey(cmd)) {
            Method m = (Method)(_methodDict.get(cmd));
            try {
                say("Going to invoke:" + m.getName() +"("+arg+")");
                m.invoke(this, args);
                return;
            }
            catch (Exception e) {
                reply("500 "+e.toString());
                _skipBytes=0;
                return;
            }
        }
        else {
            reply("504 ERET is not implemented for retrieve mode: "+extended_retrieve_mode);
            return;
        }
    }

    public void ac_esto(String arg) {
        java.util.StringTokenizer st = new java.util.StringTokenizer(arg);
        if(st.countTokens() < 2) {
            reply(err("ESTO",arg));
            return;
        }
        String extended_store_mode=st.nextToken();
        String cmd="esto_"+extended_store_mode.toLowerCase();
        Object args[] = {arg};
        if (_methodDict.containsKey(cmd)) {
            Method m = (Method)(_methodDict.get(cmd));
            try {
                say("Going to invoke:" + m.getName() +"("+arg+")");
                m.invoke(this, args);
                return;
            }
            catch (Exception e) {
                reply("500 "+e.toString());
                _skipBytes=0;
                return;
            }
        }
        else {
            reply("504 ESTO is not implemented for store mode: "+extended_store_mode);
            return;
        }
    }

    //
    // this is the implementation for the ESTO with mode "a"
    // "a" is ajusted store mode
    // other modes identified by string "MODE" can be implemented by adding
    // void method ac_esto_"MODE"(String arg)
    //
    public void ac_esto_a(String arg) {
        java.util.StringTokenizer st = new java.util.StringTokenizer(arg);
        if(st.countTokens() != 3) {
            reply(err("ESTO",arg));
            return;
        }
        String extended_store_mode=st.nextToken();
        if(!extended_store_mode.equalsIgnoreCase("a")) {
            reply("504 ESTO is not implemented for store mode: "+extended_store_mode);
            return;
        }
        String offset = st.nextToken();
        String filename = st.nextToken();
        long asm_offset;
        try {
            asm_offset = Long.parseLong(offset);
        }
        catch(Exception e) {
            String err = "501 ESTO Adjusted Store Mode: invalid offset " + offset;
            esay(err);
            reply(err);
            return;
        }
        if(asm_offset != 0) {
            reply("504 ESTO Adjusted Store Mode does not work with nonzero offset: "+offset);
            return;
        }
        say(" Performing esto in \"a\" mode with offset = "+offset);
        ac_stor(filename);
    }

    //
    // this is the implementation for the ERET with mode "p"
    // "p" is partiall retrieve mode
    // other modes identified by string "MODE" can be implemented by adding
    // void method ac_eret_"MODE"(String arg)
    //
    public void ac_eret_p(String arg) {
        java.util.StringTokenizer st = new java.util.StringTokenizer(arg);
        if(st.countTokens() != 4) {
            reply(err("ERET",arg));
            return;
        }
        String extended_retrieve_mode=st.nextToken();
        if(!extended_retrieve_mode.equalsIgnoreCase("p")) {
            reply("504 ERET is not implemented for retrieve mode: "+extended_retrieve_mode);
            return;
        }
        String offset = st.nextToken();
        String size = st.nextToken();
        String filename = st.nextToken();
        try {
            prm_offset = Long.parseLong(offset);
        }
        catch(Exception e) {
            String err = "501 ERET Partial Retrieve Mode: invalid offset " + offset;
            esay(err);
            reply(err);
            return;
        }
        try {
            prm_size = Long.parseLong(size);
        }
        catch(Exception e) {
            String err = "501 ERET Partial Retrieve Mode: invalid size " + offset;
            esay(err);
            reply(err);
            return;
        }
        say(" Performing eret in \"p\" mode with offset = "+offset+" size "+size);
        ac_retr(filename);
    }

    public void ac_retr(String arg) {

        _info = new DoorRequestInfoMessage(
                this.getNucleus().getCellName()+"@"+
                this.getNucleus().getCellDomainName() ) ;
        _info.setClient(_engine.getInetAddress().getHostName());
        _info.setTransactionTime( System.currentTimeMillis()  );
        // some requests do not have a pnfsId yet, fill it with dummy
        _info.setPnfsId( new PnfsId("000000000000000000000000") );

        String path = absolutePath( arg);
        if( path != null ) {
            _info.setPath(path);
        }

        try {

            if (arg.equals("")){
                reply(err("RETR",""));
                return;
            }

            if (_skipBytes > 0){
                reply("504 RESTART not implemented");
                _info.setResult(504, "RESTART not implemented");
                this.sendDoorRequestInfo();
                return;
            }

            if (_XferMode.equals("E") && _mode==Mode.PASSIVE) {
                reply("500 Cannot do passive retrieve in E mode");
                _info.setResult(500, "Cannot do passive retrieve in E mode");
                this.sendDoorRequestInfo();
                return;
            }

            if ( _PwdRecord == null ) {
                reply("530 Not logged in.");
                _info.setResult(530, "Not logged in.");
                this.sendDoorRequestInfo();
                return;
            }

            _info.setOwner(_dnUser == null? _user : _dnUser);
            _info.setGid( _PwdRecord.GID);
            _info.setUid( _PwdRecord.UID);

            _CurDirV = (_CurDirV == null?"/":_CurDirV);
            FsPath relativeToRootPath = new FsPath(_CurDirV);
            relativeToRootPath.add(arg);

            if(useEncpScripts) {
                File f = new File(path);
                if ( !f.exists() ) {
                    reply("500 File "+relativeToRootPath+" not found");
                    _info.setResult(500, "File "+path+" not found" );
                    this.sendDoorRequestInfo();
                    return;
                }
                if(f.isDirectory() ) {
                    reply("500 File "+relativeToRootPath+" is a directory, we don't allow that");
                    _info.setResult(500, "File "+path+" is a directory" );
                    this.sendDoorRequestInfo();
                    return;
                }
                long flength = f.length();

                String cmd = _EncpPutCmd + " chkr " +
                _PwdRecord.UID + " " +
                _PwdRecord.GID + " " +
                path;
                if( spawn(cmd, 1000) != 0 )            {
                    reply("553 Permission denied");
                    _info.setResult(500, "Permission denied for path : " + path);
                    this.sendDoorRequestInfo();
                    return;
                }
            }
            else {
                try {
                    if(!_permissionHandler.canRead(_PwdRecord.UID,_PwdRecord.GID,path)) {
                        reply("553 Permission denied");
                        _info.setResult(500, "Permission denied for path : " + path);
                        this.sendDoorRequestInfo();
                        return;
                    }
                }
                catch(CacheException ce) {
                    reply("553 Permission denied, reason: "+ce);
                    _info.setResult(500, "Permission denied for path : " + path + " (" + ce.getMessage() + ")");
                    this.sendDoorRequestInfo();
                    return;
                }
            }
            if (_TLog != null) {
                _TLog.error("incomplete transaction");
            }
            if (_TLogRoot != null) {
                SetTLog(new FTPTransactionLog(_TLogRoot, this));
                say("door will log ftp transactions to " + _TLogRoot);
            } else {
                say("tlog is not specified, door will not log ftp transactions");
            }
            say(" _user=" + _user);
            say(" vpath=" + relativeToRootPath);
            say(" addr=" + _engine.getInetAddress().toString());

            pnfsId = null;

            // for monitoring
            transferStartedAt = System.currentTimeMillis();
            _transferStarted = true;
            transferState = "waiting for storage info";

            //XXX When we upgrade to the GSSAPI version of GSI
            //we need to revisit this code and put something more useful
            //in the userprincipal spot

            startTlog(path,"read");
            say("_TLog begin done");
            PnfsGetStorageInfoMessage  storageInfoMsg = null ;
            try {
                storageInfoMsg = _pnfs.getStorageInfoByPath(path);
                transferState = "received storage info";
            } catch (CacheException ce) {
                String error = "550 Error retrieving " + path + ": " + ce;
                esay(error);
                reply(error);
                if (_TLog != null) {
                    _TLog.error(error);
                }
                _transferStarted = false;
                _info.setResult(550, "Error retrieving " + path + ": "
                        + ce.getMessage());
                this.sendDoorRequestInfo();
                return;
            }


            StorageInfo storageInfo = storageInfoMsg.getStorageInfo() ;
            _info.setPnfsId( storageInfoMsg.getPnfsId() );
            long    fileSize        = storageInfo.getFileSize() ;
            if(prm_offset == -1) {
                prm_offset = 0;
            }
            if(prm_size == -1) {
                prm_size = fileSize;
            }
            if(prm_offset < 0) {
                String err = "500 ERET prm offset is "+prm_offset;
                esay(err);
                reply(err);
                if ( _TLog != null ) {
                    _TLog.error(err);
                }
                _transferStarted = false;
                _info.setResult(500, "ERET prm offset is "+prm_offset );
                this.sendDoorRequestInfo();
                return;
            }
            if(prm_size < 0) {
                String err = "500 prm_size is "+prm_size;
                esay(err);
                reply(err);
                if ( _TLog != null ) {
                    _TLog.error(err);
                }
                _transferStarted = false;
                _info.setResult(500, "500 prm_size is "+prm_size );
                this.sendDoorRequestInfo();
                return;
            }

            if(prm_offset+prm_size > fileSize) {
                String err = "500 invalid prm_offset="+prm_offset+ " and prm_size "+
                prm_size +" for file of size "+fileSize;
                esay(err);
                reply(err);
                if ( _TLog != null ) {
                    _TLog.error(err);
                }
                _transferStarted = false;
                _info.setResult(500, "invalid prm_offset="+prm_offset+ " and prm_size "+
                        prm_size +" for file of size "+fileSize );
                this.sendDoorRequestInfo();
                return;
            }


            pnfsId      = storageInfoMsg.getPnfsId() ;
            pool=null;
            moverId = null;
            CellMessage poolManagerCellMessage;
            CellMessage poolManagerCellReply;
            CellPath poolManagerCellPath;
            PoolMgrSelectReadPoolMsg poolManagerMessage;
            PoolMgrSelectReadPoolMsg poolManagerReply;
            Object poolManagerReplyObject;

            poolManagerCellPath =
                new CellPath(_poolManagerName);
            String _pool_connects_to_host;
            int _pool_connects_to_port;
            /* use client real IP during selection */
            if (_mode == Mode.PASSIVE || _mode == Mode.RELAY) {
                _pool_connects_to_port = _adapter.getPoolListenerPort();
            } else {
                _pool_connects_to_port = _client_data_port;
            }
            _pool_connects_to_host = _client_data_host;
            GFtpProtocolInfo protocolInfo =
                new GFtpProtocolInfo(
                        "GFtp",
                        1,
                        0,
                        _pool_connects_to_host,
                        _pool_connects_to_port,
                        _parallelStart,
                        _parallelMin,
                        _parallelMax,
                        _bufSize,
                        prm_offset,
                        prm_size) ;

            poolManagerMessage =
                new PoolMgrSelectReadPoolMsg(
                        storageInfoMsg.getPnfsId(),
                        storageInfo ,
                        protocolInfo ,
                        0L );

            poolManagerCellMessage =
                new CellMessage( poolManagerCellPath,
                        poolManagerMessage);

            int retry;
            boolean ok = false;
            String errmsg="";

            synchronized(this) {
                for (retry = 0; retry < __maxRetries; ++retry) {
                    say("looking for pool for "+pnfsId);
                    transferState = "waiting for poolManager response";
                    pool = null;
                    try{
                        poolManagerCellReply =
                            sendAndWait( poolManagerCellMessage,
                                    __poolManagerTimeout*1000);
                        if( poolManagerCellReply == null ){
                            errmsg = "no pool information for " + pnfsId +"from PoolManager in " + UptimeParser.valueOf(__poolManagerTimeout*(retry+1));
                            esay(errmsg);
                            pool = null;
                        } else {
                            say("Reply= "+poolManagerCellReply);
                            poolManagerReplyObject =
                                poolManagerCellReply.getMessageObject();
                            if( ! (poolManagerReplyObject instanceof
                                    PoolMgrSelectReadPoolMsg               )){
                                esay("Unexpected message  class "+
                                        poolManagerReplyObject.getClass());
                                esay("source = "+
                                        poolManagerCellReply.getSourceAddress());
                            } else {
                                poolManagerReply =
                                    (PoolMgrSelectReadPoolMsg)poolManagerReplyObject;
                                say("poolManagerReply = "+poolManagerReply);
                                if (poolManagerReply.getReturnCode() != 0){
                                    errmsg = "Pool manager error: "+
                                    poolManagerReply.getErrorObject();
                                    esay(errmsg);
                                } else {
                                    pool = poolManagerReply.getPoolName();
                                    say("Positive reply from pool "+pool);
                                }
                            }
                        }
                    } catch (InterruptedException ice) {
                        // operation interrupted 
                        esay("pool request interrupted");
                        return;
                    } catch (Exception e) {
                        errmsg = "Can't send message to pool manager "+e;
                        esay(errmsg);
                        esay(e) ;
                        pool = null;
                    }

                    if (pool != null) {
                        reply("150 Opening BINARY data connection for "+path, false);
                        /*
                         *  if door in passive mode , e.g. adapter is used
                         *  then tell pool to connect to the adapter
                         */

                        if (_mode==Mode.PASSIVE || _mode==Mode.RELAY) {
                            protocolInfo = new GFtpProtocolInfo(
                                    "GFtp", 
                                    1, 
                                    0,
                                    _local_host, 
                                    _pool_connects_to_port,
                                    _parallelStart, 
                                    _parallelMin, 
                                    _parallelMax,
                                    _bufSize, 
                                    prm_offset, 
                                    prm_size);
                        }
                        protocolInfo.setMode(_XferMode);

                        try {
                            askForFile(pool, pnfsId, storageInfo, protocolInfo, false);
                            ok = true;
                            break;
                        } catch (Exception e){
                            errmsg = "Error sending message to pool "+pool+": "+e;
                            pool = null;
                            say(errmsg);
                        }
                    }
                    if (!ok){
                        say("retrying "+retry);
                        pool = null;
                    }
                    try {
                        Thread.sleep(__retryWait*1000);
                    } catch (InterruptedException e){
                        ///
                    }

                }  //end of retry loop

                if (pool == null){
                    ok = false;
                    errmsg = "No pools available";
                }

                if (ok) {
                    transferState = "waiting for mover completion pool \""+pool+"\", moverId="+moverId;
                    _transferInProgress = true;
                    if (_mode==Mode.PASSIVE) {
                        say("Starting adaptor...");
                        _adapter.setDirPoolToClient();
                        _adapter.start();
                    } else if (_mode==Mode.RELAY) {
                        say("Starting active adapter...");
                        if (!_adapter.isAlive())
                            _adapter.start();
                        // TODO Tell the adapter the destination address to connect
                        ((ActiveAdapter)_adapter).setDestination(_client_data_host, _client_data_port);
                    } else {
                        say("Active mode transfer...");
                    }
                    // no perf markers on retry

                    //if ( _perfMarkerConf.use && _XferMode.equals("E") ) {
                    //     /** @todo: done ### ac_retr - breadcrumb - performance markers */
                    //    _perfMarkerEngine = new PerfMarkerEngine( protocolInfo.getMax() ) ;
                    //    _perfMarkerEngine.startEngine();
                    //}

                }
                else {
                    _transferStarted = false;
                    if(_TLog != null) {
                        _TLog.error(errmsg);
                    }
                    if (!errmsg.equals("")){
                        reply("425 Cannot open port: " +errmsg);
                    } else {
                        reply("425 Cannot open port");
                    }
                    _info.setResult(425,  "425 Cannot open port: " +errmsg);
                    this.sendDoorRequestInfo();

                }

            } //synchronized(this)

            return;
        }
        finally {
            prm_offset=-1;
            prm_size=-1;
        }
    }

    private void deleteEntry(String path){
        try {
            _pnfs.deletePnfsEntry( path ) ;
        }catch(CacheException ingnored) {}
        /*
        CellPath pnfsCellPath;
        CellMessage pnfsCellMessage;
        PnfsDeleteEntryMessage pnfsMessage;

        pnfsCellPath = new CellPath(_pnfsManagerName);
        pnfsMessage = new PnfsDeleteEntryMessage(path);
        pnfsCellMessage = new CellMessage(pnfsCellPath, pnfsMessage);

        say("deleting PNFS entry "+path);

        try {
        sendMessage(pnfsCellMessage);
        } catch (Exception e){
        say ("Cannot send message " +e);
        }
         */
    }

    private void setLength(PnfsId pnfsId, long length){
        CellPath pnfsCellPath;
        CellMessage pnfsCellMessage;
        PnfsSetLengthMessage pnfsMessage;

        pnfsCellPath = new CellPath(_pnfsManagerName);
        pnfsMessage = new PnfsSetLengthMessage(pnfsId,length);
        pnfsCellMessage = new CellMessage(pnfsCellPath, pnfsMessage);

        say("setting length of "+pnfsId+" to "+length);

        try {
            sendMessage(pnfsCellMessage);
        } catch (Exception e){
            say("Cannot send message " +e);
        }
    }

    public abstract void startTlog(String path,String action);

    public void ac_stor(String arg){
        GFtpProtocolInfo protocolInfo =null;
        pnfsId=null;
        transferStartedAt = System.currentTimeMillis();

        _info = new DoorRequestInfoMessage(
                this.getNucleus().getCellName()+"@"+
                this.getNucleus().getCellDomainName() ) ;
        _info.setTransactionTime( transferStartedAt  );
        _info.setClient(_engine.getInetAddress().getHostName());
        // some requests do not have a pnfsId yet, fill it with dummy
        _info.setPnfsId( new PnfsId("000000000000000000000000") );

        String path = absolutePath( arg);
        if(path != null) {
            _info.setPath(path);
        }

        if( _ReadOnly || _PwdRecord.isWeak() || _PwdRecord.isReadOnly() ) {
            println("500 Command disabled");
            _info.setResult(500, "Command disabled");
            this.sendDoorRequestInfo();
            return;
        }
        if (arg.equals("")){
            reply(err("STOR",""));
            return;
        }

        if ( _PwdRecord == null ) {
            reply("530 Not logged in.");
            _info.setResult(530, "Not logged in.");
            this.sendDoorRequestInfo();
            return;
        }

        _info.setOwner(_dnUser == null? _user : _dnUser);
        _info.setGid( _PwdRecord.GID);
        _info.setUid( _PwdRecord.UID);

        if ( _client_data_host == null ) {
            reply("504 Host somehow not set");
            _info.setResult(504, "Host somehow not set");
            this.sendDoorRequestInfo();
            return;
        }

        if (_mode!=Mode.PASSIVE && _XferMode.equals("E")) {
            reply("504 Cannot store in active E mode");
            _info.setResult(504, "Cannot store in active E mode");
            this.sendDoorRequestInfo();
            return;
        }

        say("We're going to try to receive using " + _XferMode );


        if ( _PwdRecord.isAnonymous() ) {
            println("554 Anonymous write access not permitted");
            _info.setResult(554, "Anonymous write access not permitted");
            this.sendDoorRequestInfo();
            return;
        }
        if (_skipBytes>0){
            reply("504 RESTART not implemented for STORE");
            _skipBytes=0;
            _info.setResult(504, "RESTART not implemented for STORE");
            this.sendDoorRequestInfo();
            return;
        }

        _isPut = true ;
        _path = path;

        if(_TLogRoot != null) {
            SetTLog(new FTPTransactionLog(_TLogRoot,this));
            say("door will log ftp transactions to "+_TLogRoot);
        }
        else{
            say("tlog is not specified, door will not log ftp transactions");
        }

        // for monitoring
        transferStartedAt = System.currentTimeMillis();
        _transferStarted = true;
        transferState = "waiting for storage info";

        say(" _user="+_user);
        say(" _path="+_path);
        say(" addr="+_engine.getInetAddress().toString());
        //XXX When we upgrade to the GSSAPI version of GSI
        //we need to revisit this code and put something more useful
        //in the userprincipal spot
        startTlog(_path,"write");
        say("_TLog begin done");
        spaceReservationInfo=null;
        if(space_reservation_enabled) {
            say("space reservation is enabled");
            try {
                transferState = "waiting for space reservation info";
                say("Requesting space reservation info from SpaceManager");
                spaceReservationInfo =
                    new SpaceManagerGetInfoAndLockReservationByPathMessage(_path);
                CellMessage reply = sendAndWait(new CellMessage(
                        new CellPath("SpaceManager") ,
                        spaceReservationInfo
                ),
                spaceManagerTimeout*1000
                ) ;
                Object replyObject = reply.getMessageObject();

                if( ! ( replyObject
                        instanceof
                        SpaceManagerGetInfoAndLockReservationByPathMessage ) ) {
                    throw new
                    Exception( "Not a SpaceManagerGetInfoAndLockReservationByPathMessage : "+
                            replyObject.getClass().getName() ) ;
                }
                spaceReservationInfo =
                    (SpaceManagerGetInfoAndLockReservationByPathMessage)
                    replyObject;
                if(spaceReservationInfo.getReturnCode() != 0) {
                    esay(" getting SpaceManagerGetInfoAndLockReservationByPathMessage failed : "+
                            spaceReservationInfo.getErrorObject());
                    spaceReservationInfo = null;
                }
                say("Received space reservation info from SpaceManager:"+spaceReservationInfo);


            }
            catch (Exception e){
                String errmsg = "Can't send message to SpaceManager "+e;
                esay(errmsg);
                esay(e) ;
            }
            if(spaceReservationInfo == null) {
                esay("spaceReservationInfo == null");
            }

            if( space_reservation_strict && spaceReservationInfo == null) {
                String ftpErrorMsg = "550 Space retrieval failure or Space not reserved for this path";
                stor_error(ftpErrorMsg);
                _info.setResult(550, "Space retrieval failure or Space not reserved for this path: " + _path);
                this.sendDoorRequestInfo();
                return;

            }
        }  else {  // space reservation is not enabled
            say("space reservation is not enabled");
        }

        PnfsGetStorageInfoMessage pnfsEntry = null ;
        // check if the user has permission to create the file
        if(useEncpScripts) {
            transferState = "checking permissions via encp script";
            try {
                // Save it into enstore
                String cmd;
                int returnCode = 0;

                cmd = _EncpPutCmd + " chkc " +
                _PwdRecord.UID + " " +
                _PwdRecord.GID + " " +
                _path;
                Process p = _runtime.exec(cmd);
                p.waitFor();
                returnCode = p.exitValue();
                p.destroy();
                if ( returnCode != 0 ) {

                    String ftpErrorMsg = "550 Permission denied";
                    reply(ftpErrorMsg);
                    esay( ftpErrorMsg ) ;
                    if ( _TLog != null ) {
                        _TLog.error(ftpErrorMsg);
                    }
                    _transferStarted = false;
                    _info.setResult(550, "Permission denied for path: " + _path);
                    this.sendDoorRequestInfo();
                    return;
                }
            }
            catch ( Exception e ) {
                String ftpErrorMsg = "451 error: " + e;
                stor_error(ftpErrorMsg);
                _info.setResult(451, "Internal error [" + e.getMessage() + "] path: " + _path);
                this.sendDoorRequestInfo();
                return;
            }


            transferState = "creating pnfs entry";
            try  {
                pnfsEntry = _pnfs.createPnfsEntry( _path,
                        _PwdRecord.UID,
                        _PwdRecord.GID,
                        0644 ) ;
                _pnfsEntryIncomplete = true;  // pnfs entry created, but file transfer not yet successfully completed
            } catch (FileExistsCacheException fnfe ) {                
                String errmsg = "553 "+_path+": Cannot create file: " +  fnfe.getMessage() ;
                stor_error(errmsg);
                _info.setResult(553,_path+": Cannot create file: "+ fnfe.getMessage() );
                this.sendDoorRequestInfo();
                return;
            }catch (DirNotExistsCacheException dnee) {
                String errmsg = "553 "+_path+": Cannot create file: " +  dnee.getMessage() ;
                stor_error(errmsg);
                _info.setResult(553,_path+": Cannot create file: "+ dnee.getMessage() );
                this.sendDoorRequestInfo();
                return;
            }catch ( NotDirCacheException nde ) {
                String errmsg = "553 "+_path+": Cannot create file: " +  nde.getMessage() ;
                stor_error(errmsg);
                _info.setResult(553,_path+": Cannot create file: "+ nde.getMessage() );
                this.sendDoorRequestInfo();
                return;                
            } catch( CacheException ce ){
                String errmsg = "553 "+_path+": Cannot create file: "+ce ;
                stor_error(errmsg);
                _info.setResult(553,_path+": Cannot create file: "+ce.getMessage() );
                this.sendDoorRequestInfo();
                return;
            }

        }
        else { //useEncpScripts is false
            try {
                transferState = "checking permissions via permission handler";
                say("checking permissions via permission handler for path:"+_path);
                if(_permissionHandler.canWrite(_PwdRecord.UID,_PwdRecord.GID, _path)) {

                    // if spaceReservation is nonnull
                    // then this means that the pnfs entry was already created
                    // by the space reservation
                    // module
                    transferState = "creating pnfs entry";
                    pnfsEntry = _pnfs.createPnfsEntry( _path,
                            _PwdRecord.UID,
                            _PwdRecord.GID,
                            0644 ) ;
                    _pnfsEntryIncomplete = true;  // pnfs entry created, but file transfer not yet successfully completed
                }
                else {
                    String ftpErrorMsg = "550 Permission denied";
                    stor_error(ftpErrorMsg);
                    _info.setResult(550, "Permission denied for "+_path);
                    this.sendDoorRequestInfo();
                    return;
                }
            } catch (FileExistsCacheException fnfe ) {                
                String errmsg = "553 "+_path+": file exist: " +  fnfe.getMessage() ;
                stor_error(errmsg);
                _info.setResult(553,_path+": Cannot create file: "+ fnfe.getMessage() );
                this.sendDoorRequestInfo();
                return;
            }catch (DirNotExistsCacheException dnee) {
                String errmsg = "553 "+_path+": parent do not exist: " +  dnee.getMessage() ;
                stor_error(errmsg);
                _info.setResult(553,_path+": Cannot create file: "+ dnee.getMessage() );
                this.sendDoorRequestInfo();
                return;
            }catch ( NotDirCacheException nde ) {
                String errmsg = "553 "+_path+": parent not a directory : " +  nde.getMessage() ;
                stor_error(errmsg);
                _info.setResult(553,_path+": Cannot create file: "+ nde.getMessage() );
                this.sendDoorRequestInfo();
                return;                
            } catch( CacheException ce ){
                String errmsg = "553 "+_path+": Cannot create file: "+ce ;
                stor_error(errmsg);
                _info.setResult(553,_path+": Cannot create file: "+ce.getMessage() );
                this.sendDoorRequestInfo();
                return;
            }

        }

        pnfsId       = pnfsEntry.getPnfsId() ;
        StorageInfo     storageInfo  = pnfsEntry.getStorageInfo() ;

        _info.setPnfsId( pnfsId );



        String _pool_connects_to_host;
        int _pool_connects_to_port;
        /* use client real IP during selection */
        if (_mode == Mode.PASSIVE || _mode == Mode.RELAY) {
            _pool_connects_to_port = _adapter.getPoolListenerPort();
        }
        else {
            _pool_connects_to_port = _client_data_port;
        }
        _pool_connects_to_host = _client_data_host;

        protocolInfo = new GFtpProtocolInfo(
                "GFtp",1,0,
                _pool_connects_to_host, _pool_connects_to_port, _parallelStart, _parallelMin, _parallelMax, _bufSize, 0,
                0) ;

        protocolInfo.setMode(_XferMode);

        say( "Pnfs entry created : "+pnfsId ) ;
        transferState = "setting checksum in pnfs";
        if ( _checkSum != null && pnfsId != null ) {
            // Send checksum to PNFS manager
            try {
                PnfsFlagMessage flag =
                    new PnfsFlagMessage( pnfsId , "c" , "put" ) ;
                flag.setReplyRequired( false );
                flag.setValue( "1:" + _checkSum );

                sendMessage(
                        new CellMessage(
                                new CellPath("PnfsManager") ,
                                flag
                        )
                );
            }
            catch(Exception eee ) {
                String errmsg = "Failed to send crc to PnfsManager : "+eee;
                stor_error(errmsg);
                _info.setResult(451, "Failed to send crc to PnfsManager : "+eee.getMessage());
                this.sendDoorRequestInfo();
                return;
            }
            finally {
                _checkSum = null;
            }

        }
        synchronized(this) {

            try{
                say("Get the related StorageInfo");
                if( storageInfo == null )
                    throw new
                    CacheException( 44 , "Couldn't get StorageInfo for : "+pnfsId) ;

                say( "Got storageInfo : "+storageInfo ) ;
                pool=null;
                moverId = null;
                if(space_reservation_enabled && spaceReservationInfo != null) {
                    pool = spaceReservationInfo.getPool();
                    storageInfo.setKey("use-preallocated-space",
                            Long.toString(spaceReservationInfo.getAvailableLockedSize()));
                    esay("setting storage info key use-preallocated-space to "+
                            spaceReservationInfo.getAvailableLockedSize());
                    if(space_reservation_strict) {
                        esay("setting storage info key use-max-space to "+
                                spaceReservationInfo.getAvailableLockedSize());
                        storageInfo.setKey("use-max-space",
                                Long.toString(spaceReservationInfo.getAvailableLockedSize()));
                    }
                }
                else {
                    transferState = "waiting for read pool selection by PoolManager";
                    pool= askForReadPool(
                            pnfsId ,
                            storageInfo ,
                            protocolInfo ,
                            true ) ;
                }


                /*
                 *  if door in passive mode , e.g. adapter is used
                 *  then tell pool to connect to the adapter
                 */

                if (_mode == Mode.PASSIVE || _mode == Mode.RELAY) {
                    protocolInfo = new GFtpProtocolInfo("GFtp", 1, 0,
                            _local_host, _pool_connects_to_port,
                            _parallelStart, _parallelMin, _parallelMax,
                            _bufSize, 0, 0);
                    protocolInfo.setMode(_XferMode);
                }      

                reply("150 Opening BINARY data connection for "+_path, false );
                askForFile( pool , pnfsId , storageInfo , protocolInfo , true  ) ;

            }catch( Exception ce ){
                ce.printStackTrace();
                String errmsg = "425 Cannot open port: " +ce ;
                stor_error(errmsg);
                _info.setResult(425, "Cannot open port: " +ce.getMessage());
                this.sendDoorRequestInfo();
                return;
            }
            _transferInProgress = true;
        }

        if (_mode==Mode.PASSIVE) {
            say("Setting direction adaptor...");
            _adapter.setDirClientToPool();
            say("Starting adapter");
            if(  _XferMode.equals("E") ) {
                _adapter.setModeE(true);
                _adapter.setMaxStreams(_maxStreamsPerClient);
            }
            _adapter.start();
            say("Thread started");

        } else if (_mode==Mode.RELAY) {
            say("Starting active adapter...");
            if (!_adapter.isAlive())
                _adapter.start();
            // TODO Tell the adapter the destination address to connect
            ((ActiveAdapter)_adapter).setDestination(_client_data_host, _client_data_port);
        } 

        if ( _perfMarkerConf.use && _XferMode.equals("E") ) {
            /** @todo: done ### ac_stor - breadcrumb - performance markers */
            _perfMarkerEngine = new PerfMarkerEngine( _parallelMax ) ;
            _perfMarkerEngine.startEngine();
        }
        return;
    }

    private void stor_error(String errorreply) {
        if(space_reservation_enabled && spaceReservationInfo != null) {
            SpaceManagerUnlockSpaceMessage unlockSpace =
                new SpaceManagerUnlockSpaceMessage(spaceReservationInfo.getSpaceToken(),
                        spaceReservationInfo.getAvailableLockedSize());
            try {
                sendMessage(new CellMessage(
                        new CellPath("SpaceManager") ,
                        unlockSpace
                ));
            }
            catch (Exception e){
                String errmsg = "Can't send message to SpaceManager "+e;
                esay(errmsg);
                esay(e) ;
            }
            spaceReservationInfo = null;

        }
//      Clean-up after file transfer error
        if(_path != null && _pnfsEntryIncomplete) {
            deleteEntry(_path);
            _path = null;
            _pnfsEntryIncomplete = false;

        }

        esay( errorreply ) ;
        reply(errorreply);
        if ( _TLog != null ) {
            _TLog.error(errorreply);
        }
        _transferStarted = false;
        _transferInProgress = false;

    }

    public void ac_size(String arg){
        if (arg.equals("")){
            reply(err("SIZE",""));
            return;
        }

        if ( _PwdRecord == null ) {
            reply("530 Not logged in.");
            return;
        }

        String path = absolutePath( arg);
        long filelength =0;
        if(useEncpScripts) {
            File f = new File(path);
            if( !f.exists() ) {
                reply("500 File not found");
                return;
            }

            String cmd = _EncpPutCmd + " chkr " +
            _PwdRecord.UID + " " +
            _PwdRecord.GID + " " +
            path;
            if( spawn(cmd, 1000) != 0 ) {
                reply("553 Permission denied");
                return;
            }
            filelength = f.length();

        }
        else {
            try {
                PnfsGetStorageInfoMessage info = _pnfs.getStorageInfoByPath(path);
                if(_permissionHandler.canRead(_PwdRecord.UID, _PwdRecord.GID, path ) ) {
                    filelength = info.getMetaData().getFileSize();
                }
                else {
                    reply("553 Permission denied");
                    return;
                }
            }
            catch(CacheException ce) {
                reply("553 Permission denied, reason: "+ce);
                return;
            }
        }
        reply("213 " + filelength);
    }

    public void ac_mdtm(String arg){
        if (arg.equals("")){
            reply(err("MDTM",""));
            return;
        }

        if ( _PwdRecord == null ) {
            reply("530 Not logged in.");
            return;
        }

        String path = absolutePath( arg);
        long modification_time = 0;

        if(useEncpScripts) {
            File f = new File(path);
            if( !f.exists() ) {
                reply("500 File not found");
                return;
            }

            String cmd = _EncpPutCmd + " chkr " +
            _PwdRecord.UID + " " +
            _PwdRecord.GID + " " +
            path;
            if( spawn(cmd, 1000) != 0 ) {
                reply("553 Permission denied");
                return;
            }
            modification_time = f.lastModified();

        }
        else {
            try {
                PnfsGetStorageInfoMessage info = _pnfs.getStorageInfoByPath(path);
                if(_permissionHandler.canRead(_PwdRecord.UID, _PwdRecord.GID, path ) ) {
                    modification_time = info.getMetaData().getLastModifiedTime();
                }
                else {
                    reply("553 Permission denied");
                    return;
                }
            }
            catch(CacheException ce) {
                reply("553 Permission denied, reason: "+ce);
                return;
            }
        }
        /*
         *from the mdtm spec at http://www.ietf.org/internet-drafts/draft-ietf-ftpext-mlst-16.txt
         The syntax of a time value is:

        time-val       = 14DIGIT [ "." 1*DIGIT ]

   The leading, mandatory, fourteen digits are to be interpreted as, in
   order from the leftmost, four digits giving the year, with a range of
   1000--9999, two digits giving the month of the year, with a range of
   01--12, two digits giving the day of the month, with a range of
   01--31, two digits giving the hour of the day, with a range of
   00--23, two digits giving minutes past the hour, with a range of
   00--59, and finally, two digits giving seconds past the minute, with
   a range of 00--60 (with 60 being used only at a leap second).  Years
   in the tenth century, and earlier, cannot be expressed.  This is not
   considered a serious defect of the protocol.
         */
        java.text.DateFormat df = new java.text.SimpleDateFormat("yyyyddhhmmss");
        String time_val = df.format(new java.util.Date(modification_time));
        reply("213 " + time_val);
    }

    private class FilenameMatcher implements FilenameFilter {
        private Pattern _toMatch;

        /**
         * the pattern is of the type, used in unix shell to match files
         * where ? corresponds to any symbol and
         *       * corresponds to 0 or more symbols
         *
         * to convert it to regular expression pattern,
         * we substitute ? with . and * with .*
         */
        FilenameMatcher(String pattern) {
            pattern = pattern.replaceAll("\\?", ".");
            pattern = pattern.replaceAll("\\*",".*");
            _toMatch = Pattern.compile(pattern);
        }

        public boolean accept(File dir,
                String name) {
            Matcher m = _toMatch.matcher(name);

            return m.matches();
        }
    }

    public void ac_list(String arg) {
        Args args = new Args(arg);
        boolean long_format = true;
        if(!args.options().isEmpty()) {
            long_format = false;
        }
        if(args.getOpt("l") != null ) {
            long_format = true;
        }

        list(args,long_format);
    }

    public void list(Args args,boolean listLong) {
        say("list args = \""+args+"\"; Long format ? "+listLong);
        FilenameMatcher filenameMatcher = null;
        String arg;
        if(args.argc() == 0) {
            arg = ".";
        }
        else {
            arg = args.argv(0);
        }

        boolean isPattern =  arg.indexOf('*') != -1 || arg.indexOf('?') != -1 ||
        (arg.indexOf('[') != -1 && arg.indexOf(']') != -1);
        PnfsFile f = null;


        if(isPattern) {
            // Convert relative paths to full paths relative to base path
            if (! arg.startsWith("/"))
            { arg = _CurDirV + "/" + arg ;
            }
            FsPath parent_path = new FsPath(arg);
            List l = parent_path.getPathItemsList();
            String pattern = (String) l.get(l.size()-1);
            parent_path.add("..");
            String parent = parent_path.toString();
            if(parent.indexOf('*') != -1 || parent.indexOf('?') != -1 ||
                    (parent.indexOf('[') != -1 && parent.indexOf(']') != -1)) {
                reply(" 451 Parent Path Pattern Matching is not supported");
                return;
            }
            String absolute_parent_path = absolutePath( parent_path.toString());
            if (absolute_parent_path == null) {
                FsPath relativeToRootPath = new FsPath(_CurDirV);
                relativeToRootPath.add(parent_path.toString());
                reply("451 "+relativeToRootPath+" not found.");
                return;
            }
            f = new PnfsFile(absolute_parent_path);
            if(!f.isDirectory()) {
                reply("451 Cannot list file according to pattern \""+pattern+
                        "\" in "+parent+" which not a directory");
                return;
            }

            filenameMatcher = new FilenameMatcher(pattern);

        }
        else {
            String absolutepath = absolutePath( arg);
            if (absolutepath == null) {
                FsPath relativeToRootPath = new FsPath(_CurDirV);
                relativeToRootPath.add(arg);
                reply("451 "+relativeToRootPath+" not found.");
                return;
            }
            f = new PnfsFile(absolutepath);
        }

        if ( ! f.exists()) {
            reply("451 "+arg+"  not found");
            return;
        }

        if( ! f.isPnfs() ) {
            reply("451 "+ arg + " : non pnfs path. Access deny");
            return;
        }

        boolean isDirectory = f.isDirectory();
        File files[];
        if(isDirectory) {
            if(filenameMatcher != null) {
                files = f.listFiles(filenameMatcher);
            }
            else {
                files = f.listFiles();
            }
        }
        else {

            files = new File[1];
            files[0]= f;
        }


        StringBuffer result = new StringBuffer();
        for (int i=0; i<files.length; ++i){
            File nextf = files[i];
            int line_length=0;
            if (listLong){
                try {
                    result.append(nextf.isDirectory()?'d':'-');
                    line_length++;
                    result.append( _permissionHandler.canRead(_PwdRecord.UID, _PwdRecord.GID, nextf.getAbsolutePath() )?'r':'-');
                    line_length++;
                    result.append( _permissionHandler.canWrite(_PwdRecord.UID, _PwdRecord.GID, nextf.getAbsolutePath() )?'w':'-');
                    line_length++;
                    result.append("               ");
                    line_length+= 15;
                    long length =  nextf.length();
                    String length_str = Long.toString(length);
                    result.append(length_str);
                    line_length +=length_str.length();
                } catch (Exception e){
                    result.append('?');
                }

                while (line_length<30){
                    line_length++;
                    result.append(' ');
                }
            }
            result.append(nextf.getName());
            result.append('\r').append('\n');
        }


        OutputStream ostream = null;
        reply("150 Opening ASCII data connection for file list", false);
        try{
            if (_mode==Mode.PASSIVE)
                _dataSocket = _adapter.acceptOnClientListener();
            else {
                _dataSocket = new Socket(_client_data_host, _client_data_port);
                say("Send LIST output to: "+_dataSocket);
            }
        } catch (IOException e) {
            reply("425 Cannot open port");
            return;
        }
        try {
            ostream = _dataSocket.getOutputStream();
            ostream.write(result.toString().getBytes());
            _dataSocket.close();
            if (_mode==Mode.PASSIVE) {
                say("Waiting for adaptor...");
                while( _adapter.isAlive() ) {
                    try {
                        _adapter.join();
                    } catch (Exception e) {
                        say("Join error: " + e);
                    }
                }
            }
            _dataSocket=null;
            reply("226 ASCII transfer complete");
        } catch (Exception e) {
            try { _dataSocket.close(); }
            catch (Exception ee) {}
            _dataSocket=null;
            reply("426 Transfer aborted, closing connection");
            return;
        }
    }

    public void ac_nlst(String arg){
        Args args = new Args(arg);
        list(args,false);
    }

//  ---------------------------------------------
//  QUIT: close command channel.
//  If transfer is in progress, wait for it to finish, so set pending_quit state.
//  The delayed QUIT has not been directly implemented yet, instead...
//  Equivalent: let the data channel and pnfs entry clean-up code take care of clean-up.
//  ---------------------------------------------
    public void ac_quit( String arg ) throws CommandExitException {
        reply("221 Goodbye");
        throw new CommandExitException( "" , 0 );
    }

//  --------------------------------------------
//  BYE: synonym for QUIT
//  ---------------------------------------------
    public void ac_bye( String arg ) throws CommandExitException {
        reply("221 Goodbye");
        throw new CommandExitException( "" , 0 );
    }

//  --------------------------------------------
//  ABOR: close data channels, but leave command channel open
//  ---------------------------------------------
    public void ac_abor(String arg){
        synchronized(this) {
            // Data transfer in progress: Send mover kill to pool, and send response 426 to client
            if (_transferInProgress){
                if( _transferInProgress && (pool != null) && (moverId != null) ){
                    esay("sending mover kill to pool "+pool+" for moverId="+moverId );
                    PoolMoverKillMessage killMessage = new PoolMoverKillMessage(pool,moverId.intValue());
                    killMessage.setReplyRequired(false);
                    try { sendMessage( new CellMessage( new CellPath(pool), killMessage )  ); }
                    catch(Exception e) { esay(e); }
                }
                reply("426 Transfer aborted");
            }

            // In any case, close data socket and send response 226 to client
            if (_dataSocket != null){
                try { _dataSocket.close(); }
                catch (Exception e) {}
                _dataSocket=null;
                reply("226 Closing data connection, abort successful");
            }
            else {
                reply("226 Abort successful");
            }
        } // sychronized
    }

//  --------------------------------------------
    public String err( String cmd, String arg){
        String msg="500 '" + cmd;
        if (arg.length()>0)
            msg = msg + " " + arg;
        msg = msg + "': command not understood";
        return msg;
    }
    public String ok( String cmd){
        return "200 "+cmd+" command successful";
    }
    private void   askForFile( String       pool ,
            PnfsId       pnfsId ,
            StorageInfo  storageInfo ,
            ProtocolInfo protocolInfo ,
            boolean      isWrite      ) throws Exception {

        say("Trying pool "+pool+" for "+(isWrite?"Write":"Read"));
        transferState = "waiting for first response from pool \""+pool+"\"";

        PoolIoFileMessage poolMessage  =
            isWrite ?
                    (PoolIoFileMessage)
                    new PoolAcceptFileMessage(
                            pool,
                            pnfsId.toString() ,
                            protocolInfo ,
                            storageInfo     )
        :
            (PoolIoFileMessage)
            new PoolDeliverFileMessage(
                    pool,
                    pnfsId.toString() ,
                    protocolInfo ,
                    storageInfo     );

                            if( _ioQueueName != null ){
                                poolMessage.setIoQueueName( _ioQueueName ) ;
                            }

                            sessionId = next();
                            poolMessage.setId(sessionId);
                            // let pool know which requst triggered transfer
                            if( _info != null ) {
                                poolMessage.setInitiator( _info.getTransaction() );
                            }

                            CellPath toPool = null ;
                            if( _poolProxy == null ){
                                toPool = new CellPath(pool);
                            }else{
                                toPool = new CellPath(_poolProxy);
                                toPool.add(pool);
                            }

                            CellMessage reply = sendAndWait(
                                    new CellMessage(
                                            toPool ,
                                            poolMessage
                                    )  ,
                                    __poolTimeout*1000
                            ) ;
                            if( reply == null) {
                                throw new
                                Exception( "Pool request timed out : "+pool ) ;
                            }

                            Object replyObject = reply.getMessageObject();

                            if( ! ( replyObject instanceof PoolIoFileMessage ) )
                                throw new
                                Exception( "Illegal Object received : "+
                                        replyObject.getClass().getName());

                            PoolIoFileMessage poolReply = (PoolIoFileMessage)replyObject;

                            if (poolReply.getReturnCode() != 0) {
                                throw new
                                Exception( "Pool error: "+poolReply.getErrorObject() ) ;
                            }
                            moverId = new Integer(poolReply.getMoverId());
                            say("Pool "+pool+" will deliver file "+pnfsId+" moverId =");
                            transferState = "waiting for mover completion pool \""+pool+"\", moverId="+moverId;

    }

    private String askForReadPool( PnfsId       pnfsId ,
            StorageInfo  storageInfo ,
            ProtocolInfo protocolInfo ,
            boolean      isWrite       ) throws Exception {

        //
        // ask for a pool
        //
        PoolMgrSelectPoolMsg request =
            isWrite ?
                    (PoolMgrSelectPoolMsg)
                    new PoolMgrSelectWritePoolMsg(
                            pnfsId,
                            storageInfo,
                            protocolInfo ,
                            0L                 )
        :
            (PoolMgrSelectPoolMsg)
            new PoolMgrSelectReadPoolMsg(
                    pnfsId  ,
                    storageInfo,
                    protocolInfo ,
                    0L                 );

                            say("PoolMgrSelectPoolMsg: " + request.toString() );
                            CellMessage reply =
                                sendAndWait(
                                        new CellMessage(  _poolManagerPath, request ) ,
                                        __poolManagerTimeout*1000
                                );

                            say("CellMessage (reply): " + reply.toString() );
                            if( reply == null )
                                throw new
                                Exception("PoolMgrSelectReadPoolMsg timed out") ;

                            Object replyObject = reply.getMessageObject();

                            if( ! ( replyObject instanceof  PoolMgrSelectPoolMsg ) )
                                throw new
                                Exception( "Not a PoolMgrSelectPoolMsg : "+
                                        replyObject.getClass().getName() ) ;

                            request =  (PoolMgrSelectPoolMsg)replyObject;

                            say("poolManagerReply = "+request);

                            if( request.getReturnCode() != 0 )
                                throw new
                                Exception( "Pool manager error: "+
                                        request.getErrorObject() ) ;

                            String pool = request.getPoolName();
                            say("Positive reply from pool "+pool);

                            return pool ;

    }


    private void sendDoorRequestInfo() {
        try{
            this.sendMessage( new CellMessage( new CellPath("billing") ,  _info ) ) ;
        }catch(Exception ee){
            this.esay("Couldn't send billing info : "+ee );
        }
    }

    private long TO_GetMoverLs = __poolTimeout*1000L; // Timeout for Pool to reply

    private class PerfMarkerEngine implements Runnable {
        GFtpPerfMarkersBlock _perfMarkersBlock  = null;
        Thread  _myThread   = null;
        boolean _stopThread = false;
        boolean _running    = false;
        int     _maxStreams = 1;

        public PerfMarkerEngine( int maxStreams ) {
            _maxStreams = maxStreams;
            // Put all byte counters into the same stream #0
            //_perfMarkersBlock = new GFtpPerfMarkersBlock( _maxStreams );
            _perfMarkersBlock = new GFtpPerfMarkersBlock( 1 );
            _myThread = getNucleus().newThread( this, "gftp-PerfMarkerEngine");
        }

        public boolean isAlive() {
            return (_myThread == null ) ?
                    false
                    : _myThread.isAlive();
        }

        public Thread getThread() { return _myThread; };

        public void startEngine() {
            if( _myThread == null ) {
                esay("PerfMarkerEngine: fatal - thread was not created yet");
                return;
            }
            _running = true;
            _myThread.start();
        }

        /** @return true if thread was alive (and you would like to join). */
        public boolean stopEngine() {
            if ( _myThread != null && _myThread.isAlive() ) {
                _stopThread = true;
                _myThread.interrupt();
                return true;
            }
            else
                return false;
        }

        protected void sendMarkersToControlChannel() {
            // Send markers back to client

            // Send First channel only (commulative result)
            reply(_perfMarkersBlock.markers(0).getReply(),false);
            /* multiple channels
                int n = _perfMarkersBlock.getCount();

                for (int j = 0; j < n; j++) {
                    reply(_perfMarkersBlock.markers(j).getReply());
                }
             */
        }

        protected void sendMarkersToControlChannel( IoJobInfo ioJobInfo ) {
            long bytes     = ioJobInfo.getBytesTransferred();
            long timeStamp = ioJobInfo.getLastTransferred();


            if (_perfMarkersBlock != null) {
                // put all counts to the first channel
                _perfMarkersBlock.markers(0).setBytesWithTime(bytes,
                        timeStamp);
                reply(_perfMarkersBlock.markers(0).getReply(),false);

                /* muiltiple channels
                int n = _perfMarkersBlock.getCount();
                if (n > 0) {
                    long delta = bytes / n;
                    long first = delta + bytes % n;
                    _perfMarkersBlock.markers(0).setBytesWithTime(first,
                            timeStamp);
                    // ... starting from the _second_ marker
                    for (int j = 1; j < n; j++) {
                        _perfMarkersBlock.markers(j).setBytesWithTime(delta,
                                timeStamp);
                    }

                    // Send markers back to client
                    for (int j = 0; j < n; j++) {
                        reply(_perfMarkersBlock.markers(j).getReply());
                    }

                }
                 */
            }
        }

        public void run() {
            if (_myThread == null) {
                esay("PerfMarkerEngine: fatal - thread was not created yet, stop");
                return;
            }

            if (pool == null) {
                esay("PerfMarkerEngine: 'pool' is not defined, stop");
                return;
            }
            if (moverId == null) {
                esay("PerfMarkerEngine: 'moverId' is not defined, stop");
                return;
            }

            /* For the first time, send markers with zero counts
             * - requirement of the standard
             */
            sendMarkersToControlChannel();

            if (! _transferInProgress
                    || ! _transferStarted ){
                /** @todo - shall we report anything ? */
                return;
            }
            /* send request to pool and reply with updates to control channel
             * do it at least once, so when _stopThread is set we will send request one more time
             * and do last update to control channel
             * -- required by standard
             */
            boolean stopRequests = false;
            do {
                try {
                    // do not delay last pass
                    if ( ! _stopThread )
                        Thread.sleep(_perfMarkerConf.period);

                    stopRequests = doPass()
                    || _stopThread;
                } catch (InterruptedException ex) {
                    say("PerfMarkerEngine thread was interrupted, stop requests");
                    stopRequests = true;
                    break;
                } catch (Exception ex) {
                    say("PerfMarkerEngine thread got exception, continue. ex='"
                            + ex +"'");
                }
            }
            while ( ! stopRequests ) ;

            _running = false;

            // Send LAST reply to control channel

            /** @todo FIX ME we are driven by replies to messages
            send last message only when we got xxx meaasage and do shutdown
             */
            sendMarkersToControlChannel();

        }

        /** @return true if error and we need to stop sending messages to pool
         * (and markers to control channel)
         */

        private boolean doPass() {
            boolean gotException = false;

            // Send request for the next block

            CellMessage msg = new CellMessage(new CellPath(pool),
                    "mover ls -binary " + moverId);
            CellMessage repMsg = null;

            try {
                repMsg = sendAndWait(msg, TO_GetMoverLs);
            } catch (Exception ex) {
                esay( "PerfMarkerEngine: Can not send performance markers request - got exception " +
                        ex);
                gotException = true;
            }

            if( gotException )
                return (false); // continue sending requests

            // Process wrong or missing replies:
            if (repMsg == null) {
                esay( "PerfMarkerEngine: Sending performance markers request to pool timed out");
                return (false); // continue sending requests
            }

            if (!(repMsg.getMessageObject() instanceof IoJobInfo)) {
                Object o = msg.getMessageObject();
                if (o instanceof Exception) {
                    esay("PerfMarkerEngine: reply is exception " +
                            ((Exception) o).getMessage());
                } else if (o instanceof String) {
                    esay("PerfMarkerEngine: reply is error message '" +
                            o.toString() + "'");
                } else {
                    esay("PerfMarkerEngine: reply is unexpected class : " +
                            o.getClass().getName());
                }
                return (true); // stop sending requests
            }

            // Reply OK:
            // Check job status

            IoJobInfo ioJobInfo = (IoJobInfo) repMsg.getMessageObject();

            String status = ioJobInfo.getStatus();

            if (status != null) {
                if (status.equals("A")) {
                    // "Active" job
                    sendMarkersToControlChannel(ioJobInfo);
                } else if (status.equals("K") || status.equals("R")) {
                    // "Killed" or "Removed" job
                    return (true); // stop sending requests

                } else if (status.equals("W")) {
                    // "Waiting" job
                    return (false); // continue sending requests
                } else {
                    esay("PerfMarkerEngine: wrong mover status '" + status + "'");
                    return (true); // stop sending requests
                }
            }
            // else status is null :
            // ... skip, wait transfer to start
            return (false); // continue sending requests

        } //doPass()

    } // PerfMarkersEngine


    /**
     * 
     * interrupt parent thread in case if IO errors on control line ( connection close )
     *
     */
    private static class AsciiCommandPoller implements Runnable {


        private final SyncFifo2 _commands = new SyncFifo2();
        private final BufferedReader _in;
        private final Thread _parent;

        AsciiCommandPoller(BufferedReader in, Thread parent) {
            _in = in;
            _parent = parent;
        }


        public void run() {


            boolean done = false;
            while (!done ) {
                try {
                    String command =  _in.readLine();
                    _commands.push( command );
                    if(command == null ) {
                        done = true;
                        _parent.interrupt();
                    }
                } catch (IOException e) {
                    _parent.interrupt();
                    done = true;
                }
            }

        }

        String nextCommand() {
            return (String)_commands.pop();
        }

    }

}
