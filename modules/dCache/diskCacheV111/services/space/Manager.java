//______________________________________________________________________________
//
// Space Manager - cell that handles space reservation management in SRM
//                 essentially a layer on top of a database
// database schema is described in ManagerSchemaConstants
//
// there are three essential tables:
//
//      +------------+  +--------+  +------------+
//      |srmlinkgroup|-<|srmspace|-<|srmspacefile|
//      +------------+  +--------+  +------------+
// srmlinkgroup contains field that caches sum(size-usedsize) of all space
// reservations belonging to the linkgroup. Field is called reservedspaceinbytes
//
// srmspace  contains fields that caches sum(size) of all files from srmspace
// that belong to this space reservation. Fields are usedspaceinbytes
//  (for files in state STORED) and allocatespaceinbytes
//  (for files in states RESERVED or TRANSFERRING)
//
// each time a space reservation is added/removed , reservedspaceinbytes in
// srmlinkgroup is updated
//
// each time a file is added/removed, usedspaceinbytes, allocatespaceinbytes and
// reservedspaceinbytes are updated depending on file state
//
//                                    Dmitry Litvintsev (litvinse@fnal.gov)
// $Id: Manager.java 9764 2008-07-07 17:48:24Z litvinse $
// $Author: litvinse $
//______________________________________________________________________________
package diskCacheV111.services.space;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Date;
import java.util.Collections;
import diskCacheV111.services.space.message.*;
import  dmg.cells.nucleus.SystemCell;
import  dmg.cells.nucleus.Cell;
//import  dmg.util.*;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellVersion;
import dmg.util.Args;
import dmg.cells.nucleus.ExceptionEvent;
import java.sql.*;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.PoolMgrGetPoolLinkGroups;
import diskCacheV111.vehicles.PoolLinkGroupInfo;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.PnfsSetStorageInfoMessage;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.GridProtocolInfo;
import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FQAN;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.Pgpass;
import diskCacheV111.util.VOInfo;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.DBManager;
import diskCacheV111.util.IoPackage;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.namespace.NameSpaceProvider;
import org.dcache.util.JdbcConnectionPool;
import org.apache.log4j.Logger;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.GroupList;

/**
 *   <pre> Space Manager dCache service provides ability
 *    \to reserve space in the pool linkGroups
 *
 *
 * @author  timur
 */
public final class Manager
        extends CellAdapter
        implements Runnable {
	private long spaceReservationCleanupPeriodInSeconds = 60*60;
	private String jdbcUrl;
	private String jdbcClass;
	private String user;
	private String pass;
	private String pwdfile;
	private long updateLinkGroupsPeriod = 3*60*1000;
	private long expireSpaceReservationsPeriod     = 3*60*1000;

	private boolean deleteStoredFileRecord = false;
	private String pnfsManager = "PnfsManager";
	private String poolManager = "PoolManager";
	private String quotaManager = "QuotaManager";
	private Thread updateLinkGroups;
	private Thread expireSpaceReservations;
	private AccessLatency defaultLatency = AccessLatency.NEARLINE;
	private RetentionPolicy defaultPolicy = RetentionPolicy.CUSTODIAL;
	private boolean reserveSpaceForNonSRMTransfers;
	private boolean returnFlushedSpaceToReservation=true;
	private boolean returnRemovedSpaceToReservation=true;
	private boolean cleanupExpiredSpaceFiles=true;
	private String linkGroupAuthorizationFileName = null;
	private boolean spaceManagerEnabled =true;
	private boolean matchVoGroupAndVoRole=false;
	public static final int currentSchemaVersion = 3;
	private int previousSchemaVersion;
	private Args _args;

	JdbcConnectionPool connection_pool;
	DBManager manager;
	private static Logger logger = Logger.getLogger(Manager.class);

	public Manager(String name, String argString) throws Exception {
		super( name ,Manager.class.getName(), argString , false );
		_args     = getArgs();
		jdbcUrl   = _args.getOpt("jdbcUrl");
		jdbcClass = _args.getOpt("jdbcDriver");
		user      = _args.getOpt("dbUser");
		pass      = _args.getOpt("dbPass");
		pwdfile   = _args.getOpt("pgPass");
		String pgBasedPass = null;
		if (pwdfile != null && !(pwdfile.trim().equals(""))) {
			Pgpass pgpass = new Pgpass(pwdfile);      //VP
			pgBasedPass = pgpass.getPgpass(jdbcUrl, user);   //VP
		}
		if(pgBasedPass != null) {
			pass = pgBasedPass;
		}
		manager = DBManager.getInstance();
		manager.initConnectionPool(jdbcUrl, jdbcClass, user, pass);
		connection_pool = manager.getConnectionPool();
		spaceManagerEnabled =
			isOptionSetToTrueOrYes("spaceManagerEnabled",spaceManagerEnabled);
		if (logger.isDebugEnabled()) { 
                        logger.debug("USING LOGGER spaceManagerEnabled="+spaceManagerEnabled);
                }
		if(_args.getOpt("poolManager") != null) {
			poolManager = _args.getOpt("poolManager");
		}
		if(_args.getOpt("quotaManager") != null) {
			quotaManager = _args.getOpt("quotaManager");
		}
		if(_args.getOpt("pnfsManager") != null) {
			pnfsManager = _args.getOpt("pnfsManager");
		}
		if(_args.getOpt("updateLinkGroupsPeriod") != null) {
			updateLinkGroupsPeriod = Long.parseLong(_args.getOpt("updateLinkGroupsPeriod"));
		}
		if(_args.getOpt("expireSpaceReservationsPeriod") != null) {
			expireSpaceReservationsPeriod = Long.parseLong(_args.getOpt("expireSpaceReservationsPeriod"));
		}
		if(_args.getOpt("cleanupPeriod") != null) {
			spaceReservationCleanupPeriodInSeconds = Long.parseLong(_args.getOpt("cleanupPeriod"));
		}
		if(_args.getOpt("defaultRetentionPolicy") != null) {
			defaultPolicy = RetentionPolicy.getRetentionPolicy(_args.getOpt("defaultRetentionPolicy"));
		}
		if(_args.getOpt("defaultAccessLatency") != null) {
			defaultLatency = AccessLatency.getAccessLatency(_args.getOpt("defaultAccessLatency"));
		}
		if(_args.getOpt("defaultAccessLatencyForSpaceReservation") != null) {
			defaultLatency = AccessLatency.getAccessLatency(
                    _args.getOpt("defaultAccessLatencyForSpaceReservation"));
		}
		if(_args.getOpt("reserveSpaceForNonSRMTransfers") != null) {
			reserveSpaceForNonSRMTransfers=
				_args.getOpt("reserveSpaceForNonSRMTransfers").equalsIgnoreCase("true");
		}
		if(_args.getOpt("deleteStoredFileRecord") != null) {
			deleteStoredFileRecord=
				_args.getOpt("deleteStoredFileRecord").equalsIgnoreCase("true");
		}
		if(_args.getOpt("cleanupExpiredSpaceFiles") != null) {
			cleanupExpiredSpaceFiles=
				_args.getOpt("cleanupExpiredSpaceFiles").equalsIgnoreCase("true");
		}
		if(_args.getOpt("returnFlushedSpaceToReservation") != null) {
			returnFlushedSpaceToReservation=
				_args.getOpt("returnFlushedSpaceToReservation").equalsIgnoreCase("true");
		}
		if(_args.getOpt("returnRemovedSpaceToReservation") != null) {
			returnRemovedSpaceToReservation=
				_args.getOpt("returnRemovedSpaceToReservation").equalsIgnoreCase("true");
		}
		if(_args.getOpt("matchVoGroupAndVoRole") != null) {
			matchVoGroupAndVoRole=
				_args.getOpt("matchVoGroupAndVoRole").equalsIgnoreCase("true");
		}
		if(_args.getOpt("linkGroupAuthorizationFileName") != null) {
			String tmp = _args.getOpt("linkGroupAuthorizationFileName").trim();
			if(tmp.length() >0) {
				linkGroupAuthorizationFileName = tmp;
			}
		}
		if(deleteStoredFileRecord  && returnRemovedSpaceToReservation) {
			throw new IllegalArgumentException(
                    "configuration conflict: returnRemovedSpaceToReservation == " +
                    "true and deleteStoredFileRecord == true");
		}
		try {
			dbinit();
		}
		catch (Throwable t) {
			logger.error("error starting space.Manager");
			logger.error(t.getMessage());
			start();
			kill();
		}
		start();
		(updateLinkGroups = getNucleus().newThread(this,"UpdateLinkGroups")).start();
		(expireSpaceReservations = getNucleus().newThread(this,"ExpireThreadReservations")).start();

	}


	private boolean isOptionSetToTrueOrYes(String value) {
		String tmpstr = _args.getOpt(value);
		return tmpstr != null &&
			(tmpstr.equalsIgnoreCase("true") ||
			 tmpstr.equalsIgnoreCase("on")   ||
			 tmpstr.equalsIgnoreCase("yes")  ||
			 tmpstr.equalsIgnoreCase("enabled") ) ;
	}

	private boolean isOptionSetToTrueOrYes(String value, boolean default_value) {
		String tmpstr = _args.getOpt(value);
		if( tmpstr != null) {
			return
				tmpstr.equalsIgnoreCase("true") ||
				tmpstr.equalsIgnoreCase("on")   ||
				tmpstr.equalsIgnoreCase("yes")  ||
				tmpstr.equalsIgnoreCase("enabled") ;
		}
		else {
			return default_value;
		}
	}


	public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.63 $" ); }

	public void getInfo(java.io.PrintWriter printWriter) {
		printWriter.println("space.Manager "+getCellName());
		printWriter.println("spaceManagerEnabled="+spaceManagerEnabled);
		printWriter.println("JdbcUrl="+jdbcUrl);
		printWriter.println("jdbcClass="+jdbcClass);
		printWriter.println("databse user="+user);
		printWriter.println("reservation space cleanup period in secs : "+spaceReservationCleanupPeriodInSeconds);
		printWriter.println("updateLinkGroupsPeriod="+updateLinkGroupsPeriod);
		printWriter.println("expireSpaceReservationsPeriod="+expireSpaceReservationsPeriod);
		printWriter.println("deleteStoredFileRecord="+deleteStoredFileRecord);
		printWriter.println("pnfsManager="+pnfsManager);
		printWriter.println("poolManager="+poolManager);
                printWriter.println("defaultLatencyForSpaceReservations="+defaultLatency);
		printWriter.println("reserveSpaceForNonSRMTransfers="+reserveSpaceForNonSRMTransfers);
		printWriter.println("returnFlushedSpaceToReservation="+returnFlushedSpaceToReservation);
		printWriter.println("returnRemovedSpaceToReservation="+returnRemovedSpaceToReservation);
		printWriter.println("linkGroupAuthorizationFileName="+linkGroupAuthorizationFileName);
	}

	public String hh_release = " <spaceToken> [ <bytes> ] # release the space " +
		"reservation identified by <spaceToken>" ;

	public String ac_release_$_1_2(Args args) throws Exception {
		boolean force = args.getOpt("force") != null;
		long reservationId = Long.parseLong( args.argv(0));
		Long spaceToReleaseInBytes = null;
		if(args.argc() == 1) {
			updateSpaceState(reservationId,SpaceState.RELEASED);
			StringBuffer sb = new StringBuffer();
			Space space = getSpace(reservationId);
			space.toStringBuffer(sb);
			return sb.toString();
		}
		else {
			return "partial release is not supported yet";
		}
	}

	public String hh_update_space_reservation = " [-size=<size>]  [-lifetime=<lifetime>] [-vog=<vogroup>] [-vor=<vorole>] <spaceToken> \n"+
                "                                                     # set new size and/or lifetime for the space token \n " +
                "                                                     # valid examples of size: 1000, 100kB, 100KB, 100KiB, 100MB, 100MiB, 100GB, 100GiB, 10.5TB, 100TiB \n" +
                "                                                     # see http://en.wikipedia.org/wiki/Gigabyte for explanation \n"+
                "                                                     # lifetime is in seconds (\"-1\" means infinity or permanent reservation";

	public final long stringToSize(String s)  throws Exception {
		long size=0L;
		int endIndex=0;
		int startIndex=0;
		if (s.endsWith("kB") || s.endsWith("KB")) {
			endIndex=s.indexOf("KB");
			if (endIndex==-1) {
				endIndex=s.indexOf("kB");
			}
			String sSize = s.substring(startIndex,endIndex);
			size    = sSize.equals("") ? 1000L : (long)(Double.parseDouble(sSize)*1.e+3+0.5);
		}
		else if (s.endsWith("KiB")) {
			endIndex=s.indexOf("KiB");
			String sSize = s.substring(startIndex,endIndex);
			size    = sSize.equals("") ? 1024L : (long)(Double.parseDouble(sSize)*1024.+0.5);
		}
		else if (s.endsWith("MB")) {
			endIndex=s.indexOf("MB");
			String sSize = s.substring(startIndex,endIndex);
			size    = sSize.equals("") ? 1000000L : (long)(Double.parseDouble(sSize)*1.e+6+0.5);
		}
		else if (s.endsWith("MiB")) {
			endIndex=s.indexOf("MiB");
			String sSize = s.substring(startIndex,endIndex);
			size    = sSize.equals("") ? 1048576L : (long)(Double.parseDouble(sSize)*1048576.+0.5);
		}
		else if (s.endsWith("GB")) {
			endIndex=s.indexOf("GB");
			String sSize = s.substring(startIndex,endIndex);
			size    = sSize.equals("") ? 1000000000L : (long)(Double.parseDouble(sSize)*1.e+9+0.5);
		}
		else if (s.endsWith("GiB")) {
			endIndex=s.indexOf("GiB");
			String sSize = s.substring(startIndex,endIndex);
			size    = sSize.equals("") ? 1073741824L : (long)(Double.parseDouble(sSize)*1073741824.+0.5);
		}
		else if (s.endsWith("TB")) {
			endIndex=s.indexOf("TB");
			String sSize = s.substring(startIndex,endIndex);
			size    = sSize.equals("") ? 1000000000000L : (long)(Double.parseDouble(sSize)*1.e+12+0.5);
		}
		else if (s.endsWith("TiB")) {
			endIndex=s.indexOf("TiB");
			String sSize = s.substring(startIndex,endIndex);
			size    = sSize.equals("") ? 1099511627776L : (long)(Double.parseDouble(sSize)*1099511627776.+0.5);
		}
		else {
			size = Long.parseLong(s);
		}
		if (size<0L) {
			throw new IllegalArgumentException("size have to be non-negative");
		}
		return size;
	}


	public String ac_update_space_reservation_$_1(Args args) throws Exception {
		long reservationId = Long.parseLong(args.argv(0));
                String sSize     = args.getOpt("size");
                String sLifetime = args.getOpt("lifetime");
                String voRole      = args.getOpt("vor");
                String voGroup     = args.getOpt("vog");
                if (sLifetime==null&&
                    sSize==null&&
                    voRole==null&&
                    voGroup==null) {
                        return "Need to specify at least one option \"-lifetime\", \"-size\" \"-vog\" or \"-vor\". If -lifetime=\"-1\"  then the reservation will not expire";
                }
                Long longLifetime = null;
                if (sLifetime != null) {
                        long lifetime = Long.parseLong(sLifetime);
                        longLifetime = ( lifetime == -1 ) ? new Long(-1) : new Long ( lifetime * 1000 );
                }
                if (voRole!=null||voGroup!=null) {
                        // check that linkgroup allows these role/group combination
		try {
                                Space space = getSpace(reservationId);
                                long lid = space.getLinkGroupId();
                                LinkGroup lg = getLinkGroup(lid);
                                boolean foundMatch=false;
                                // this will keep the same group/role
                                // if one of then is not specified:
                                if (voGroup==null) voGroup=space.getVoGroup();
                                if (voRole==null)  voRole=space.getVoRole();
                                for (VOInfo info : lg.getVOs()) {
                                        String group = info.getVoGroup();
                                        String role  = info.getVoRole();
                                        if ((group.equals(voGroup)||group.equals("*"))&&
                                            (role==null||role.equals(voRole)||role.equals("*"))) {
                                                foundMatch=true;
                                                break;
                                        }
                                }
                                if (!foundMatch) {
                                StringBuilder sb = new StringBuilder();
                                for (VOInfo info : lg.getVOs()) {
                                        sb.append(info).append('\n');
                                }
                                        throw new IllegalArgumentException("cannot change voGroup:voRole to "+
                                                                           voGroup+":"+voRole+
                                                                           ". Supported vogroup:vorole pairs for this spacereservation\n"+
                                                                           sb.toString());
                                                }
                                        }
                        catch (SQLException e) {
                                return e.toString();   
                        }
                        catch (Exception  e) {
                                return e.toString();
                        }
                }
		try {
			updateSpaceReservation(reservationId,
					       voGroup,
					       voRole,
					       null,
					       null,
					       null,
					       (sSize != null ?  Long.valueOf(stringToSize(sSize)) : null),
					       longLifetime,
					       null,
					       null);
		}
		catch (SQLException e) {
			return e.toString();
		}
		StringBuffer sb = new StringBuffer();
		StringBuffer id = new StringBuffer();
		id.append(reservationId);
		listSpaceReservations(false,
				      id.toString(),
				      null,
				      null,
				      null,
				      null,
				      null,
				      sb);
		return sb.toString();
	}

	public String hh_update_link_groups = " #triggers update of the link groups";
	public String ac_update_link_groups_$_0(Args args) throws Exception {
		synchronized(updateLinkGroupsSyncObject) {
			updateLinkGroupsSyncObject.notify();
		}
		return "update started";
	}

	public String hh_ls = " [-lg=LinkGroupName] [-lgid=LinkGroupId] [-vog=vogroup] [-vor=vorole] [-desc=description] [-l] <id> # list space reservations";

	public String ac_ls_$_0_1(Args args) throws Exception {
		String lgName        = args.getOpt("lg");
		String lgid          = args.getOpt("lgid");
		String voGroup       = args.getOpt("vog");
		String voRole        = args.getOpt("vor");
		String description   = args.getOpt("desc");
		boolean isLongFormat = args.getOpt("l") != null;
		String id = null;
		if (args.argc() == 1) {
			id = args.argv(0);
		}
		StringBuffer sb = new StringBuffer();
		if (description != null && id !=null ) {
			sb.append("Do not handle \"desc\" and id simultaneously\n");
			return sb.toString();
		}

		if (lgName==null&&lgid==null&&voGroup==null&&description==null) {
			sb.append("Reservations:\n");
		}
		listSpaceReservations(isLongFormat,
				      id,
				      lgName,
				      lgid,
				      description,
				      voGroup,
				      voRole,
				      sb);
		if (lgName==null&&lgid==null&&voGroup==null&&description==null&id==null) {
			sb.append("\n\nLinkGroups:\n");
			listLinkGroups(isLongFormat,false,id,sb);
		}
		return sb.toString();
	}


	private void listSpaceReservations(boolean isLongFormat,
					   String id,
					   String linkGroupName,
					   String linkGroupId,
					   String description,
					   String group,
					   String role,
					   StringBuffer sb) throws Exception {
		IoPackage pkg = new  SpaceReservationIO();
		HashSet spaces = null;
		long lgId = 0;
		LinkGroup lg = null;
		if (linkGroupId!=null) {
			lgId = Long.parseLong(linkGroupId);
			lg   = getLinkGroup(lgId);
		}
		if (linkGroupName!=null) {
			lg = getLinkGroupByName(linkGroupName);
			if (lgId!=0) {
				if (lg.getId()!=lgId) {
					sb.append("Cannot find LinkGroup with id=").append(linkGroupId).append(" and name=").append(linkGroupName);
					return;
				}
			}
		}
		if (lg!=null) {
			sb.append("Found LinkGroup:\n");
			lg.toStringBuffer(sb);
			sb.append("\n");
		}

		if(id != null) {
			Long longid = Long.valueOf(id);
			try {
				spaces=manager.selectPrepared(pkg,
							      SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_ID,
							      longid);
				if (spaces.isEmpty()==true) {
					if(lg==null) {
						sb.append("Space with id=").append(id).append(" not found ");
					}
					else {
						sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain space with id=").append(id);
					}
					return;
				}
				for (Iterator i=spaces.iterator();i.hasNext();) {
					Space space = (Space)i.next();
					if (lg!=null) {
						if (space.getLinkGroupId()!=lg.getId()) {
							sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain space with id=").append(id);
						}
					}
					else {
						space.toStringBuffer(sb);
					}
					sb.append('\n');
				}
				return;
			}
			catch(SQLException e) {
				if(lg==null) {
					sb.append("Space with id=").append(id).append(" not found ");
				}
				else {
					sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain space with id=").append(id);
				}
				return;
			}
		}
		if (linkGroupName==null&&linkGroupId==null&&description==null&&group==null&&role==null){
			try {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("executing statement: "+SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS);
                                }
				spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS);
				int count = spaces.size();
				long totalReserved = 0;
				for (Iterator i=spaces.iterator(); i.hasNext();) {
					Space space = (Space)i.next();
					totalReserved += space.getSizeInBytes();
					space.toStringBuffer(sb);
					sb.append('\n');
				}
				sb.append("total number of reservations: ").append(count).append('\n');
				sb.append("total number of bytes reserved: ").append(totalReserved);
				return;

			}
			catch(SQLException sqle) {
				sb.append(sqle.getMessage());
				return;
			}
		}
		if (description==null&&group==null&&role==null&&lg!=null) {
			try {
				spaces=manager.selectPrepared(pkg,
							      SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_LINKGROUP_ID,
							      lg.getId());
				if (spaces.isEmpty()==true) {
					sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain any space reservations\n");
					return;
				}
				for (Iterator i=spaces.iterator();i.hasNext();) {
					Space space = (Space)i.next();
					space.toStringBuffer(sb);
					sb.append('\n');
				}
				return;
			}
			catch(SQLException e) {
				sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain any space reservations\n");
				return;
			}

		}
		if (description!=null) {
			try {
				if (lg==null) {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_DESC);
                                        }
                                        spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_DESC,
									  description);
				}
				else {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_DESC_AND_LINKGROUP_ID);
                                        }
					spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_DESC_AND_LINKGROUP_ID,
								      description,
								      lg.getId());
				}
				if (spaces.isEmpty()==true) {
					if (lg==null) {
						sb.append("Space with description ").append(description).append(" not found ");
					}
					else {
						sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain space with description ").append(description);
					}
					return;
				}
				for (Iterator i=spaces.iterator();i.hasNext();) {
					Space space = (Space)i.next();
					space.toStringBuffer(sb);
					sb.append('\n');
				}
				return;
			}
			catch(SQLException e) {
				if (lg==null) {
					sb.append("Space with description ").append(description).append(" not found ");
				}
				else {
					sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain space with description ").append(description);
				}
				return;
			}
		}
		if (role!=null&&group!=null) {
			try {
				if (lg==null) {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_VOROLE);
                                        }
					spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_VOROLE,
								      group,
								      role);
				}
				else {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_VOROLE_AND_LINKGROUP_ID);
                                        }
					spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_VOROLE_AND_LINKGROUP_ID,
								      group,
								      role,
								      lg.getId());

				}
				if (spaces.isEmpty()==true) {
					if (lg==null) {
						sb.append("Space with vorole ").append(role).append(" and vogroup ").append(group).append(" not found ");
					}
					else {
						sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain space with vorole ").append(role).append(" and vogroup ").append(group);
					}
					return;
				}
				for (Iterator i=spaces.iterator();i.hasNext();) {
					Space space = (Space)i.next();
					space.toStringBuffer(sb);
					sb.append('\n');
				}
				return;
			}
			catch(SQLException e) {
				if (lg==null) {
					sb.append("Space with vorole ").append(role).append(" and vogroup ").append(group).append(" not found ");
				}
				else {
					sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(lg.getName()).append(" does not contain space with vorole ").append(role).append(" and vogroup ").append(group);
				}
				return;
			}
		}
		if (group!=null) {
			try {
				if (lg==null) {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP);
                                        }
					spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP,
								      group);
				}
				else {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_LINKGROUP_ID);
                                        }
					spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_LINKGROUP_ID,
								      group,
								      lg.getId());
				}
				if (spaces.isEmpty()==true) {
					if (lg==null) {
						sb.append("Space with vogroup ").append(group).append(" not found ");
					}
					else {
						sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(" does not contain space with vogroup=").append(group);
					}
					return;
				}
				for (Iterator i=spaces.iterator();i.hasNext();) {
					Space space = (Space)i.next();
					space.toStringBuffer(sb);
					sb.append('\n');
				}
				return;
			}
			catch(SQLException e) {
				if (lg==null) {
					sb.append("Space with vogroup ").append(group).append(" not found ");
				}
				else {
					sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(" does not contain space with vogroup=").append(group);
				}
				return;
			}
		}
		if (role!=null) {
			try {
				if (lg==null) {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOROLE);
                                        }
					spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOROLE,
							      group);
				}
				else {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOROLE_AND_LINKGROUP_ID);
                                        }
					spaces=manager.selectPrepared(pkg,SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOROLE_AND_LINKGROUP_ID,
								      group,
								      lg.getId());
				}
				if (spaces.isEmpty()==true) {
					if (lg==null) {
						sb.append("Space with vogroup ").append(group).append(" not found ");
					}
					else {
						sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(" does not contain space with vorole=").append(role);
					}
					return;
				}
				for (Iterator i=spaces.iterator();i.hasNext();) {
					Space space = (Space)i.next();
					space.toStringBuffer(sb);
					sb.append('\n');
				}
				return;
			}
			catch(SQLException e) {
				if (lg==null) {
					sb.append("Space with vogroup ").append(group).append(" not found ");
				}
				else {
					sb.append("LinkGroup with id=").append(lg.getId()).append(" and name=").append(" does not contain space with vorole=").append(role);
				}
				return;
			}
		}

	}

	private void listLinkGroups(boolean isLongFormat,
				    boolean all,
				    String id,
				    StringBuffer sb) throws Exception {
		IoPackage pkg = new LinkGroupIO();
		HashSet groups = null;
		if(id != null) {
			long longid = Long.parseLong(id);
			try {
				LinkGroup lg=getLinkGroup(longid);
				lg.toStringBuffer(sb);
				sb.append('\n');
				return;
			}
			catch(SQLException e) {
				sb.append("LinkGroup  with id=").append(id).append(" not found ");
				return;
			}
		}
		try {
			if(all) {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("executing statement: "+LinkGroupIO.SELECT_ALL_LINKGROUPS);
                                }
				groups=manager.selectPrepared(pkg,
							      LinkGroupIO.SELECT_ALL_LINKGROUPS);
			}
			else {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("executing statement: "+LinkGroupIO.SELECT_CURRENT_LINKGROUPS+" ?,"+latestLinkGroupUpdateTime);
                                }
				groups=manager.selectPrepared(pkg,
							      LinkGroupIO.SELECT_CURRENT_LINKGROUPS,
							      latestLinkGroupUpdateTime);
			}
			int count = groups.size();
			long totalReservable = 0L;
			long totalReserved   = 0L;
			for (Iterator i=groups.iterator(); i.hasNext();) {
				LinkGroup g = (LinkGroup)i.next();
				totalReservable  += g.getAvailableSpaceInBytes();
				totalReserved    += g.getReservedSpaceInBytes();
				g.toStringBuffer(sb);
				sb.append('\n');
			}
			sb.append("total number of linkGroups: ").append(count).append('\n');
			sb.append("total number of bytes reservable: ").append(totalReservable).append('\n');
			sb.append("total number of bytes reserved  : ").append(totalReserved).append('\n');
			sb.append("last time all link groups were updated: ").append((new Date(latestLinkGroupUpdateTime)).toString()).append("(").append(latestLinkGroupUpdateTime).append(")");
			return;
		}
		catch(SQLException sqle) {
			sb.append(sqle.getMessage());
		}
	}

	public String hh_ls_link_groups = " [-l] [-a]  <id> # list link groups";
	public String ac_ls_link_groups_$_0_1(Args args) throws Exception {
		boolean isLongFormat = args.getOpt("l") != null;
		boolean all = args.getOpt("a") != null;
		String id = null;
		if (args.argc() == 1) {
			id = args.argv(0);
		}
		StringBuffer sb = new StringBuffer();
		sb.append("\n\nLinkGroups:\n");
		listLinkGroups(isLongFormat,all,id,sb);
		return sb.toString();
	}

	public String hh_ls_file_space_tokens = " <pnfsId>|<pnfsPath> # list space tokens " +
		"that contain a file";

	public String ac_ls_file_space_tokens_$_1(Args args) throws Exception {
		String  pnfsPath = args.argv(0);
		PnfsId pnfsId = null;
		try {
			pnfsId = new PnfsId(pnfsPath);
			pnfsPath = null;
		}
		catch(Exception e) {
			pnfsId = null;
		}
		StringBuffer sb = new StringBuffer();
		long[] tokens= getFileSpaceTokens(pnfsId, pnfsPath);
		if(tokens != null && tokens.length >0) {
			for(long token:tokens) {
				sb.append('\n').append(token);
			}
		}
		else {
			sb.append("\nno space tokens found for file:").append( args.argv(0));
		}
		return sb.toString();
	}

	public String hh_reserve = "  [-vog=voGroup] [-vor=voRole] " +
		"[-acclat=AccessLatency] [-retpol=RetentionPolicy] [-desc=Description] " +
		" [-lgid=LinkGroupId]" +
		" [-lg=LinkGroupName]" +
		" <sizeInBytes> <lifetimeInSecs (use quotes around negative one)> \n"+
                " default value for AccessLatency is "+defaultLatency+ "\n"+
                " default value for RetentionPolicy is "+defaultPolicy;

	public String ac_reserve_$_2(Args args) throws Exception {
		long sizeInBytes=0L;
		try {
			sizeInBytes=stringToSize(args.argv(0));
		}
		catch (Exception e) {
			return "Cannot convert size specified ("+ args.argv(0)+") to non-negative number. \n"+
			       "Valid definition of size:\n"+
				"\t\t - a number of bytes (long integer less than 2^64) \n"+
				"\t\t - 100kB, 100KB, 100KiB, 100MB, 100MiB, 100GB, 100GiB, 10.5TB, 100TiB \n"+
				"see http://en.wikipedia.org/wiki/Gigabyte for explanation";
		}
		long lifetime=Long.parseLong(args.argv(1));
		if(lifetime > 0) {
			lifetime *= 1000;
		}
		String voGroup       = args.getOpt("vog");
		String voRole        = args.getOpt("vor");
		String description   = args.getOpt("desc");
		String latencyString = args.getOpt("acclat");
		String policyString  = args.getOpt("retpol");

		AccessLatency latency = latencyString==null?
                    defaultLatency:AccessLatency.getAccessLatency(latencyString);
		RetentionPolicy policy = policyString==null?
                    defaultPolicy:RetentionPolicy.getRetentionPolicy(policyString);

		String lgIdString = args.getOpt("lgid");
		String lgName     = args.getOpt("lg");
		if(lgIdString != null && lgName != null) {
			return "Error: both exclusive options -lg and -lgid are specified";
		}
		long reservationId;
		if(lgIdString == null && lgName == null) {
			try {
				reservationId = reserveSpace(voGroup,
							     voRole,
							     sizeInBytes,
							     latency ,
							     policy,
							     lifetime,
							     description);
			}
			catch (Exception e) {
				return "Failed to find likgroup taht can accommodate this space reservation. \n"+
					e.getMessage()+'\n'+
					"check that you have any link groups that satisfy the following criteria: \n"+
					"\t can fit the size you are requesting ("+sizeInBytes+")\n"+
					"\t vogroup,vorole you specified ("+voGroup+","+voRole+") are allowed, and \n"+
					"\t retention policy and access latency you specified ("+policyString+","+latencyString+") are allowed \n";
			}
		}
		else {
			long lgId;
			LinkGroup lg=null;
			if (lgIdString != null){
				lgId =Long.parseLong(lgIdString);
				lg   = getLinkGroup(lgId);
				if(lg ==null) {
					return "Error, could not find link group with id = "+lgIdString+'\n';
				}
			}
			else {
				lg = getLinkGroupByName(lgName);
				if(lg ==null) {
					return "Error, could not find link group with name = '"+lgName+"'\n";
				}
				lgId = lg.getId();
			}

			Long[] linkGroups = findLinkGroupIds(sizeInBytes,
							     voGroup,
							     voRole,
							     latency,
							     policy);
			if(linkGroups.length == 0) {
				return "Link Group "+lg+" is found, but it cannot accommodate the reservation requested, \n"+
					"check that the link group satisfies the following criteria: \n"+
					"\t it can fit the size you are requesting ("+sizeInBytes+")\n"+
					"\t vogroup,vorole you specified ("+voGroup+","+voRole+") are allowed, and \n"+
					"\t retention policy and access latency you specified ("+policyString+","+latencyString+") are allowed \n";
			}

			boolean yes=false;
			for(int i=0;i<linkGroups.length;i++) {
				if (linkGroups[i]==lgId) {
					yes=true;
					break;
				}
			}
			if (!yes) {
				return "Link Group "+lg+" is found, but it cannot accommodate the reservation requested, \n"+
					"check that the link group satisfies the following criteria: \n"+
					"\t it can fit the size you are requesting ("+sizeInBytes+")\n"+
					"\t vogroup,vorole you specified ("+voGroup+","+voRole+") are allowed, and \n"+
					"\t retention policy and access latency you specified ("+policyString+","+latencyString+") are allowed \n";
			}
			reservationId = reserveSpaceInLinkGroup(
				lgId,
				voGroup,
				voRole,
				sizeInBytes,
				latency ,
				policy,
				lifetime,
				description);
		}
		StringBuffer sb = new StringBuffer();
		Space space = getSpace(reservationId);
                space.toStringBuffer(sb);
		return sb.toString();
	}

	public String hh_listInvalidSpaces = " [-e] [-r] <n>" +
		" # e=expired, r=released, default is both, n=number of rows to retrieve";

	private static final int RELEASED = 1;
	private static final int EXPIRED  = 2;

	private static final String[] badSpaceType= { "released",
                                                     "expired",
                                                     "released or expired" };
	public String ac_listInvalidSpaces_$_0_3( Args args )
		throws Exception {
		int argCount       = args.optc();
		boolean doExpired  = args.getOpt( "e" ) != null;
		boolean doReleased = args.getOpt( "r" ) != null;
		int nRows = 1000;
		if (args.argc()>0) {
			nRows = Integer.parseInt(args.argv(0));
		}
		if (nRows < 0 ) {
			return "number of rows must be non-negative";
		}
		int listOptions = RELEASED | EXPIRED;
		if ( doExpired || doReleased ) {
			listOptions = 0;
			if ( doExpired ) {
				listOptions = EXPIRED;
				--argCount;
			}
			if ( doReleased ) {
				listOptions |= RELEASED;
				--argCount;
			}
		}
		if ( argCount != 0 ) {
			return "Unrecognized option.\nUsage: listInvalidSpaces" +
				hh_listInvalidSpaces;
		}
		List< Space > expiredSpaces = listInvalidSpaces( listOptions , nRows );
		if ( expiredSpaces.isEmpty() ) {
			return "There are no " + badSpaceType[ listOptions-1 ] + " spaces.";
		}
		StringBuffer report = new StringBuffer();
		for ( Space es : expiredSpaces ) {
			report.append( es.toString() ).append( '\n' );
		}
		return report.toString();
	}

	public static final String SELECT_INVALID_SPACES = "SELECT * FROM "+ManagerSchemaConstants.SpaceTableName + " WHERE state = ";

	public List< Space > listInvalidSpaces( int spaceTypes , int nRows)
		throws SQLException,
		Exception {
		String query;
		switch ( spaceTypes ) {
		case EXPIRED: // do just expired
			query = SELECT_INVALID_SPACES + SpaceState.EXPIRED.getStateId();
			break;
		case RELEASED: // do just released
			query = SELECT_INVALID_SPACES + SpaceState.RELEASED.getStateId();
			break;
		case RELEASED | EXPIRED: // do both
			query = SELECT_INVALID_SPACES + SpaceState.EXPIRED.getStateId() +
				" OR state = " + SpaceState.RELEASED.getStateId();
			break;
		default: // something is broken
			String msg = "listInvalidSpaces: got invalid space type "
				+ spaceTypes;
			throw new Exception( msg );
		}
		Connection con = null;
		// Note that we return an empty list if "set" is empty.
		List< Space > result = new ArrayList< Space >();
		try {
                        if (logger.isDebugEnabled()) {
                                logger.debug( "executing statement: " + query );
                        }
			con = connection_pool.getConnection();
			PreparedStatement sqlStatement = con.prepareStatement( query );
			con.setAutoCommit(false);
			sqlStatement.setFetchSize(10000);
			sqlStatement.setMaxRows(nRows);
			ResultSet set = sqlStatement.executeQuery();
			while ( set.next() ) {
				Space space = new Space(
					set.getLong( "id" ),
					set.getString( "voGroup" ),
					set.getString( "voRole" ),
					RetentionPolicy.getRetentionPolicy( set.getInt( "retentionPolicy" ) ),
					AccessLatency.getAccessLatency( set.getInt( "accessLatency" ) ),
					set.getLong( "linkGroupId" ),
					set.getLong( "sizeInBytes" ),
					set.getLong( "creationTime" ),
					set.getLong( "lifetime" ),
					set.getString( "description" ),
					SpaceState.getState( set.getInt( "state" )),
					set.getLong("usedspaceinbytes"),
					set.getLong("allocatedspaceinbytes"));
				result.add( space );
			}
			set.close();
			sqlStatement.close();
			connection_pool.returnConnection( con );
			con = null;
		}
		catch ( SQLException sqe ) {
			if (con!=null) {
				con.rollback();
				connection_pool.returnFailedConnection( con );
				con = null;
			}
			throw sqe;
		}
		finally {
			if ( con != null ) {
				connection_pool.returnConnection( con );
			}
		}
		return result;
	}


	public static String SELECT_FILES_IN_SPACE =  "SELECT * FROM "+ManagerSchemaConstants.SpaceFileTableName +" WHERE spaceReservationId = ?";

	public String hh_listFilesInSpace = " <space-id>";
	// @return a string containing a newline-separated list of the files in
	//         the space specified by <i>space-id</i>.

	public String ac_listFilesInSpace_$_1( Args args )
		throws Exception {
		long spaceId = Long.parseLong( args.argv( 0 ) );
		// Get a list of the Invalid spaces
		List< File > filesInSpace = listFilesInSpace( spaceId );
		if ( filesInSpace.isEmpty() ) {
			return "There are no files in this space.";
		}
		// For each space, convert it to a string, one per line.
		StringBuffer report = new StringBuffer();
		for ( File file : filesInSpace ) {
			report.append( file.toString() ).append( '\n' );
		}
		return report.toString();
	}

	// This method returns an array of all the files in the specified space.
	public List< File > listFilesInSpace( long spaceId )
		throws SQLException {
		List< File > result = new ArrayList< File >();
		try {
			HashSet set = manager.selectPrepared(new FileIO(),
							     FileIO.SELECT_BY_SPACERESERVATION_ID,
							     spaceId);
			for (Iterator i=set.iterator();i.hasNext();){
				File f=(File)i.next();
				result.add(f);
			}
		}
		catch ( SQLException sqe ) {
			logger.warn( sqe.getMessage() );
		}
		return result;
	}



	public String hh_removeFilesFromSpace = " [-r] [-t] [-s] [-f] <Space Token>" +
		"# remove expired files from space, -r(reserved) -t(transferring) -s(stored) -f(flushed)";

	public String ac_removeFilesFromSpace_$_1_4( Args args )
		throws Exception {
		long spaceId = Long.parseLong(args.argv(0));
		int optCount           = args.optc();
		StringBuffer sb = new StringBuffer();
		if (optCount==0) {
			sb.append("No option specified, will remove expired RESERVED and TRANSFERRING files\n");
		}
		boolean doReserved     = args.getOpt( "r" ) != null;
		boolean doTransferring = args.getOpt( "t" ) != null;
		boolean doStored       = args.getOpt( "s" ) != null;
		boolean doFlushed      = args.getOpt( "f" ) != null;
		HashSet spaces=manager.selectPrepared(new SpaceReservationIO(),
						      SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_ID,
						      spaceId);
		if (spaces.isEmpty()==true) {
			sb.append("Space with "+spaceId+" does not exist\n");
			return sb.toString();
		}
		for (Iterator i=spaces.iterator(); i.hasNext(); ) {
			Space space = (Space)i.next();
			HashSet files = manager.selectPrepared(new FileIO(),
							       FileIO.SELECT_EXPIRED_SPACEFILES1,
							       System.currentTimeMillis(),
							       space.getId());
			for (Iterator j=files.iterator(); j.hasNext(); ) {
				File file = (File)j.next();
				if (optCount==0) {
					if (file.getState()==FileState.STORED || file.getState()==FileState.FLUSHED) continue;
				}
				else {
					if (!doReserved && file.getState()==FileState.RESERVED)         continue;
					if (!doTransferring && file.getState()==FileState.TRANSFERRING) continue;
					if (!doStored && file.getState()==FileState.STORED) continue;
					if (!doFlushed && file.getState()==FileState.FLUSHED) continue;
				}
				try {
					removeFileFromSpace(file.getId());
				}
				catch (SQLException e) {
					sb.append("Failed to remove file "+file+"\n");
					logger.warn(e.getMessage());
					continue;
				}
			}
		}
		return sb.toString();
	}

	public String hh_remove_file = " -id=<file id> | -pnfsId=<pnfsId>  " +
		"# remove file by spacefile id or pnfsid";

	public String ac_remove_file( Args args )
		throws Exception {
                String sid     = args.getOpt("id");
                String sPnfsId = args.getOpt("pnfsId");
                if (sid!=null&&sPnfsId!=null) {
                        return "do not handle \"-id\" and \"-pnfsId\" options simultaneously";
                }
                if (sid!=null) {
                        long id = Long.parseLong(sid);
                        removeFileFromSpace(id);
                        return "removed file with id="+id;
                }
                if (sPnfsId!=null) {
                        PnfsId pnfsId = new PnfsId(sPnfsId);
                        File f = getFile(pnfsId);
                        removeFileFromSpace(f.getId());
                        return "removed file with pnfsId="+pnfsId;
                }
                return "please specify  \"-id=\" or \"-pnfsId=\" option";
        }

    private static final Object fixMissingSizeLock = new Object();
    private static final String SELECT_RECORDS_WITH_MISSING_SIZE =
            String.format("select f.* from srmspacefile f, srmspace s where f.state=%d and f.sizeinbytes=0 and f.spacereservationid=s.id and s.state=%d", FileState.STORED.getStateId(), SpaceState.RESERVED.getStateId());
    private void fixMissingSize()
    {
        synchronized (fixMissingSizeLock) {
            try {
                PnfsHandler pnfs =
                    new PnfsHandler(Manager.this,
                                    new CellPath("PnfsManager"));
                logger.info("fix missing size: Searching for files...");
                HashSet<File> files =
                    manager.select(new FileIO(), SELECT_RECORDS_WITH_MISSING_SIZE);
                int counter = 0;
                for (File file: files) {
                    if (counter % 1000 == 0) {
                        logger.info(String.format("fix missing size: Processed %d of %d files.", counter, files.size()));
                    }

                    PnfsGetFileMetaDataMessage msg =
                        pnfs.getFileMetaDataById(file.getPnfsId());
                    long size = msg.getMetaData().getFileSize();
                    updateSpaceFile(file.getId(),
                                    null,
                                    null,
                                    null,
                                    size,
                                    null,
                                    null);
                    counter++;
                }
                logger.info("fix missing size: Done");
            } catch (SQLException e) {
                logger.error("Failure in 'fix missing size': " + e.getMessage());
            } catch (CacheException e) {
                logger.error("Failure in 'fix missing size': " + e.getMessage());
            }
        }
    }

    public static final String hh_fix_missing_size =
        "# See full help for details";
    public static final String fh_fix_missing_size =
        "Cleans up after a bug that was present in dCache 1.9.1-1 to 1.9.1-3. That \n" +
        "bug caused files to be registered with a wrong size in the space manager.\n" +
        "Warning: This command may take a long time to complete and may consume a\n" +
        "         lot of memory. Progress information can be found in the log file.";
    public String ac_fix_missing_size(Args args)
    {
        new Thread()
        {
            public void run()
            {
                fixMissingSize();
            }
        }.start();
        return "Command is executed in a background thread.";
    }

	private static final String selectNextToken = "SELECT nexttoken  FROM "+ManagerSchemaConstants.SpaceManagerNextIdTableName;
	private static final String insertNextToken = "INSERT INTO "+ManagerSchemaConstants.SpaceManagerNextIdTableName+
		" (nexttoken) VALUES ( 0 )";

	private void dbinit() throws SQLException {
		logger.debug("WE ARE IN DBINIT");
		String tables[] = {ManagerSchemaConstants.SpaceManagerSchemaVersionTableName,
				   ManagerSchemaConstants.SpaceManagerNextIdTableName,
				   ManagerSchemaConstants.LinkGroupTableName,
				   ManagerSchemaConstants.LinkGroupVOsTableName,
				   ManagerSchemaConstants.RetentionPolicyTableName,
				   ManagerSchemaConstants.AccessLatencyTableName,
				   ManagerSchemaConstants.SpaceTableName,
				   ManagerSchemaConstants.SpaceFileTableName};

		String createTables[] = {ManagerSchemaConstants.CreateSpaceManagerSchemaVersionTable,
					 ManagerSchemaConstants.CreateSpaceManagerNextIdTable,
					 ManagerSchemaConstants.CreateLinkGroupTable,
					 ManagerSchemaConstants.CreateLinkGroupVOsTable,
					 ManagerSchemaConstants.CreateRetentionPolicyTable,
					 ManagerSchemaConstants.CreateAccessLatencyTable,
					 ManagerSchemaConstants.CreateSpaceTable,
					 ManagerSchemaConstants.CreateSpaceFileTable};
		Map<String,Boolean> created = new Hashtable<String,Boolean>();
		for (int i =0; i<tables.length;++i) {
			created.put(tables[i], Boolean.FALSE);
			try {
				manager.createTable(tables[i],createTables[i]);
				created.put(tables[i], Boolean.TRUE);
			}
			catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
                updateSchemaVersion(created);
                Object obj = manager.selectPrepared(1,selectNextToken);
                if (obj == null) {
                        manager.insert(insertNextToken);
                }
                insertRetentionPolicies();
                insertAccessLatencies();
	}

	private static final String selectVersion = "SELECT version FROM "+
		ManagerSchemaConstants.SpaceManagerSchemaVersionTableName ;

	public static final String updateVersion = "UPDATE "+
		ManagerSchemaConstants.SpaceManagerSchemaVersionTableName+
		" SET version= "+currentSchemaVersion;

	private static final String insertVersion = "INSERT INTO "+ManagerSchemaConstants.SpaceManagerSchemaVersionTableName +
		" (version) VALUES ( "+currentSchemaVersion+" )";

	private void updateSchemaVersion (Map<String,Boolean> created)
		throws SQLException {
		if(!created.get(ManagerSchemaConstants.SpaceManagerSchemaVersionTableName)) {
			Object o = manager.selectPrepared(1,selectVersion);
			if (o!=null) {
				previousSchemaVersion = ((Integer)o).intValue();
				if (previousSchemaVersion<currentSchemaVersion) {
					manager.update(updateVersion);
				}
			}
			else {
				// nothing is found in the schema version table,
				// pretend it was just created
				created.put(ManagerSchemaConstants.SpaceManagerSchemaVersionTableName, Boolean.TRUE);
			}
		}
		if(created.get(ManagerSchemaConstants.SpaceManagerSchemaVersionTableName)) {
			manager.insert(insertVersion);
			if(created.get(ManagerSchemaConstants.LinkGroupTableName)) {
				//everything is created for the first time
				previousSchemaVersion = currentSchemaVersion;
			}
			else {
				//database was created when the Schema Version was not in existence
				previousSchemaVersion = 0;
			}
		}
		if(previousSchemaVersion == currentSchemaVersion) {
			manager.createIndexes(ManagerSchemaConstants.SpaceFileTableName,"spacereservationid","state","pnfspath","pnfsid","creationtime","lifetime");
			manager.createIndexes(ManagerSchemaConstants.SpaceTableName,"linkgroupid","state","description","lifetime","creationtime");
			return;
		}
		//
		// Apply schema modifications
		//
		try {
			updateSchema();
		}
		catch (SQLException e){
			logger.error("failed to update schema from "+previousSchemaVersion+" to "+currentSchemaVersion);
			logger.error(e.getMessage());
		}
	}

	private static final String alterLinkGroupTable =
		"ALTER TABLE " + ManagerSchemaConstants.LinkGroupTableName+
		" ADD COLUMN  onlineAllowed INT,"+
		" ADD COLUMN  nearlineAllowed INT,"+
		" ADD COLUMN  replicaAllowed INT,"+
		" ADD COLUMN  outputAllowed INT,"+
		" ADD COLUMN  custodialAllowed INT";
	private static final String updateLinkGroupTable = "UPDATE  "+ ManagerSchemaConstants.LinkGroupTableName+
		"\n SET onlineAllowed = 1 ," +
		"\n     nearlineAllowed = 1 ,"+
		"\n     replicaAllowed = CASE WHEN hsmType= 'None' THEN 1 ELSE 0 END,"+
		"\n     outputAllowed = CASE WHEN hsmType= 'None' THEN 1 ELSE 0 END,"+
		"\n     custodialAllowed = CASE WHEN hsmType= 'None' THEN 0 ELSE 1 END";
	private static final String alterLinkGroupTable1 = "ALTER TABLE " + ManagerSchemaConstants.LinkGroupTableName+
		" DROP  COLUMN  hsmType ";

	private void updateSchema() throws SQLException{
		if(previousSchemaVersion == currentSchemaVersion) {
			return;
		}
		logger.info("updating Schema, previous schema version number="+previousSchemaVersion+", updadting to current version number "+currentSchemaVersion);
		if(previousSchemaVersion == 0) {
			manager.batchUpdates(alterLinkGroupTable,
					     updateLinkGroupTable,
					     alterLinkGroupTable1);
			previousSchemaVersion=1;
		}
		if (previousSchemaVersion==1) {
			manager.batchUpdates("ALTER TABLE " +ManagerSchemaConstants.LinkGroupTableName+ " ADD COLUMN  reservedspaceinbytes BIGINT",
					     "ALTER TABLE " +ManagerSchemaConstants.SpaceTableName    +
					     " ADD COLUMN  usedspaceinbytes      BIGINT,"+
					     " ADD COLUMN  allocatedspaceinbytes  BIGINT");
			manager.createIndexes(ManagerSchemaConstants.SpaceFileTableName,"spacereservationid","state","pnfspath","pnfsid","creationtime","lifetime");
			manager.createIndexes(ManagerSchemaConstants.SpaceTableName,"linkgroupid","state","description","lifetime","creationtime");
			//
			// Now we need to calculate space one by and as
			// doing it in one go takes too long
			HashSet spaces = null;
			try {
				spaces=manager.selectPrepared( new  SpaceReservationIO(),
							       SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS);
				for (Iterator i=spaces.iterator(); i.hasNext();) {
					Space space = (Space)i.next();
					try {
						manager.update(ManagerSchemaConstants.POPULATE_USED_SPACE_IN_SRMSPACE_TABLE_BY_ID,
							       space.getId(),
							       space.getId(),
							       space.getId());
					}
					catch(SQLException e) {
						logger.error("failed to execute "+ManagerSchemaConstants.POPULATE_USED_SPACE_IN_SRMSPACE_TABLE_BY_ID+",?="+space.getId());
					}
				}
			}
			catch (SQLException e) {
				logger.error(e.getMessage());
			}
			//
			// Do the same with linkgroups
			//
			HashSet groups = null;
			try {
				groups=manager.selectPrepared( new LinkGroupIO(),
							       LinkGroupIO.SELECT_ALL);

				for (Iterator i=groups.iterator(); i.hasNext();) {
					LinkGroup group = (LinkGroup)i.next();
					try {
						manager.update(ManagerSchemaConstants.POPULATE_RESERVED_SPACE_IN_SRMLINKGROUP_TABLE_BY_ID,
							       group.getId(),
							       group.getId());
					}
					catch(SQLException e) {
						logger.error("failed to execute "+ManagerSchemaConstants.POPULATE_RESERVED_SPACE_IN_SRMLINKGROUP_TABLE_BY_ID+",?="+group.getId());
					}
				}
			}
			catch (SQLException e) {
				logger.error(e.getMessage());
			}
			previousSchemaVersion=2;
		}
		if (previousSchemaVersion==2) {
			manager.batchUpdates("ALTER TABLE " +ManagerSchemaConstants.SpaceFileTableName+ " ADD COLUMN  deleted INTEGER");
			previousSchemaVersion=3;
		}
	}

	private static final String countPolicies =
		"SELECT count(*) from "+ManagerSchemaConstants.RetentionPolicyTableName;

	private static final String insertPolicy = "INSERT INTO "+
		ManagerSchemaConstants.RetentionPolicyTableName+
		" (id, name) VALUES (?,?)" ;

	private void insertRetentionPolicies() throws  SQLException{
		RetentionPolicy[] policies = RetentionPolicy.getAllPolicies();
		Object o = manager.selectPrepared(1,countPolicies);
		if (o!=null && ((Long) o).longValue() == policies.length) {
			return;
		}
		for(int i = 0; i<policies.length; ++i) {
			try {
				manager.insert(insertPolicy,policies[i].getId(),policies[i].toString());
			}
			catch(SQLException sqle) {
				logger.error(sqle.getMessage());
			}
		}
	}


	private static final String countLatencies =
		"SELECT count(*) from "+ManagerSchemaConstants.AccessLatencyTableName;

	private static final String insertLatency = "INSERT INTO "+
		ManagerSchemaConstants.AccessLatencyTableName+
		" (id, name) VALUES (?,?)";

    	private void insertAccessLatencies() throws  SQLException {
		AccessLatency[] latencies = AccessLatency.getAllLatencies();
		Object o = manager.selectPrepared(1,countLatencies);
		if (o!=null && ((Long) o).longValue() == latencies.length) {
			return;
		}
		for(int i = 0; i<latencies.length; ++i) {
			try {
				manager.insert(insertLatency,latencies[i].getId(),latencies[i].toString());
			}
			catch(SQLException sqle) {
				logger.error(sqle.getMessage());
			}
		}
	}

//
// the code below is left w/o changes for now
//

	public static final String selectNextIdForUpdate =
		"SELECT * from "+ManagerSchemaConstants.SpaceManagerNextIdTableName+" FOR UPDATE ";

	public static final long NEXT_LONG_STEP=10000;

	public static final String increaseNextId = "UPDATE "+ManagerSchemaConstants.SpaceManagerNextIdTableName+
		" SET NextToken=NextToken+"+NEXT_LONG_STEP;
	private long nextLongBase;
	private long _nextLongBase = 0;
	private long nextLongIncrement=NEXT_LONG_STEP; //trigure going into database
	// on startup


	public synchronized  long getNextToken() throws SQLException  {
		if(nextLongIncrement >= NEXT_LONG_STEP) {
			nextLongIncrement =0;
			incrementNextLongBase();
		}
		long nextLong = nextLongBase +(nextLongIncrement++);;
                if (logger.isDebugEnabled()) {
                        logger.debug(" return nextLong="+nextLong);
                }
		return nextLong;
	}

	public synchronized  long getNextToken(Connection connection) throws SQLException  {
		if(nextLongIncrement >= NEXT_LONG_STEP) {
			nextLongIncrement =0;
			try {
				incrementNextLongBase(connection);
			}
			catch(SQLException e) {
				e.printStackTrace();
				if (connection!=null) {
					try {
						connection.rollback();
					}
					catch(Exception e1) { }
				}
				nextLongBase = _nextLongBase;
			}
			_nextLongBase = nextLongBase+ NEXT_LONG_STEP;
		}

		long nextLong = nextLongBase +(nextLongIncrement++);;
                if (logger.isDebugEnabled()) {
                        logger.debug(" return nextLong="+nextLong);
                }
		return nextLong;
	}

	private void incrementNextLongBase(Connection connection) throws SQLException{
		PreparedStatement s = connection.prepareStatement(selectNextIdForUpdate);
                if (logger.isDebugEnabled()) {
                        logger.debug("getNextToken trying "+selectNextIdForUpdate);
                }
		ResultSet set = s.executeQuery();
		if(!set.next()) {
			s.close();
			throw new SQLException("table "+ManagerSchemaConstants.SpaceManagerNextIdTableName+" is empty!!!");
		}
		nextLongBase = set.getLong(1);
		s.close();
                if (logger.isDebugEnabled()) {
                        logger.debug("nextLongBase is ="+nextLongBase);
                }
		s = connection.prepareStatement(increaseNextId);
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+increaseNextId);
                }
		int i = s.executeUpdate();
		s.close();
		connection.commit();
	}

	private void incrementNextLongBase() {
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			incrementNextLongBase(connection);
		}
		catch(SQLException e) {
			e.printStackTrace();
			if (connection!=null) {
				try {
					connection.rollback();
				}
				catch(Exception e1) { }
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			nextLongBase = _nextLongBase;
		}
		finally {
		    if(connection != null) {
			    connection_pool.returnConnection(connection);

		    }
		}
		_nextLongBase = nextLongBase+ NEXT_LONG_STEP;
	}

//
// unchanged code ends here
//
	public static final String selectLinkGroupVOs =
		"SELECT VOGroup,VORole FROM "+ManagerSchemaConstants.LinkGroupVOsTableName+
		" WHERE linkGroupId=?";

	public static final String onlineSelectionCondition =
		"lg.onlineallowed = 1 ";
	public static final String nearlineSelectionCondition =
		"lg.nearlineallowed = 1 ";
	public static final String replicaSelectionCondition =
		"lg.replicaallowed = 1 ";
	public static final String outputSelectionCondition =
		"lg.outputallowed = 1 ";
	public static final String custodialSelectionCondition =
		"lg.custodialAllowed = 1 ";

	public static final String voGroupSelectionCondition =
		" ( lgvo.VOGroup = ? OR lgvo.VOGroup = '*' ) ";
	public static final String voRoleSelectionCondition =
		" ( lgvo.VORole = ? OR lgvo.VORole = '*' ) ";

	public static final String spaceCondition  = " lg.freespaceinbytes-lg.reservedspaceinbytes >= ? ";
	public static final String orderBy = " order by available desc ";

	public static final String selectLinkGroupInfoPart1 = "SELECT lg.*,"+
		"lg.freespaceinbytes-lg.reservedspaceinbytes as available "+
		"\n from srmlinkgroup lg, srmlinkgroupvos lgvo"+
		"\n where lg.id=lgvo.linkGroupId  and  lg.lastUpdateTime >= ? ";

	public static final String selectOnlineReplicaLinkGroup =
		selectLinkGroupInfoPart1+" and "+
		onlineSelectionCondition + " and "+
		replicaSelectionCondition + " and "+
		voGroupSelectionCondition + " and "+
		voRoleSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectOnlineOutputLinkGroup  =
		selectLinkGroupInfoPart1+" and "+
		onlineSelectionCondition + " and "+
		outputSelectionCondition + " and "+
		voGroupSelectionCondition + " and "+
		voRoleSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectOnlineCustodialLinkGroup  =
		selectLinkGroupInfoPart1+" and "+
		onlineSelectionCondition + " and "+
		custodialSelectionCondition + " and "+
		voGroupSelectionCondition + " and "+
		voRoleSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectNearlineReplicaLinkGroup  =
		selectLinkGroupInfoPart1+" and "+
		nearlineSelectionCondition + " and "+
		replicaSelectionCondition + " and "+
		voGroupSelectionCondition + " and "+
		voRoleSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectNearlineOutputLinkGroup =
		selectLinkGroupInfoPart1+" and "+
		nearlineSelectionCondition + " and "+
		outputSelectionCondition + " and "+
		voGroupSelectionCondition + " and "+
		voRoleSelectionCondition + " and "+
		spaceCondition +
		orderBy;


	public static final String selectNearlineCustodialLinkGroup =
		selectLinkGroupInfoPart1+" and "+
		nearlineSelectionCondition + " and "+
		custodialSelectionCondition + " and "+
		voGroupSelectionCondition + " and "+
		voRoleSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectAllOnlineReplicaLinkGroup =
		selectLinkGroupInfoPart1+" and "+
		onlineSelectionCondition + " and "+
		replicaSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectAllOnlineOutputLinkGroup  =
		selectLinkGroupInfoPart1+" and "+
		onlineSelectionCondition + " and "+
		outputSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectAllOnlineCustodialLinkGroup  =
		selectLinkGroupInfoPart1+" and "+
		onlineSelectionCondition + " and "+
		custodialSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectAllNearlineReplicaLinkGroup  =
		selectLinkGroupInfoPart1+" and "+
		nearlineSelectionCondition + " and "+
		replicaSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	public static final String selectAllNearlineOutputLinkGroup =
		selectLinkGroupInfoPart1+" and "+
		nearlineSelectionCondition + " and "+
		outputSelectionCondition + " and "+
		spaceCondition +
		orderBy;


	public static final String selectAllNearlineCustodialLinkGroup =
		selectLinkGroupInfoPart1+" and "+
		nearlineSelectionCondition + " and "+
		custodialSelectionCondition + " and "+
		spaceCondition +
		orderBy;

	//
	// the function below returns list of linkgroup ids that correspond
	// to linkgroups that satisfy retention policy/access latency criteria,
        // voGroup/voRoles criteria and have sufficient space to accommodate new
        // space reservation. Sufficient space is defined as lg.freespaceinbytes-lg.reservedspaceinbytes
        // we do not use select for update here as we do not want to lock many
        // rows.

	private Long[] findLinkGroupIds(
		long sizeInBytes,
		String voGroup,
		String voRole,
		AccessLatency al,
		RetentionPolicy rp) throws SQLException {
		try {
                        if (logger.isDebugEnabled()) {
                                logger.debug("findLinkGroupIds(sizeInBytes="+sizeInBytes+
                                             ", voGroup="+voGroup+" voRole="+voRole+
                                             ", AccessLatency="+al+
                                             ", RetentionPolicy="+rp+
                                             ")");
                        }
			String select;
			if(al.equals(AccessLatency.ONLINE)) {
				if(rp.equals(RetentionPolicy.REPLICA)) {
					select = selectOnlineReplicaLinkGroup;
				}
				else
					if ( rp.equals(RetentionPolicy.OUTPUT)) {
						select = selectOnlineOutputLinkGroup;
					}
					else {
						select = selectOnlineCustodialLinkGroup;
					}

			}
			else {
				if(rp.equals(RetentionPolicy.REPLICA)) {
					select = selectNearlineReplicaLinkGroup;
				}
				else
					if ( rp.equals(RetentionPolicy.OUTPUT)) {
						select = selectNearlineOutputLinkGroup;
					}
					else {
						select = selectNearlineCustodialLinkGroup;
					}
			}
			IoPackage pkg = new LinkGroupIO();
			HashSet groups = null;
                        if (logger.isDebugEnabled()) {
                                logger.debug("executing statement: "+select+
                                             "?="+latestLinkGroupUpdateTime+
                                             "?="+voGroup+
                                             "?="+voRole+
                                             "?="+sizeInBytes
                                             );
                        }
			groups=manager.selectPrepared(pkg,
						      select,
						      latestLinkGroupUpdateTime,
						      voGroup,
						      voRole,
						      sizeInBytes);
			java.util.Set idset = new java.util.HashSet();
			for(Iterator i=groups.iterator();i.hasNext();) {
			       	LinkGroup s = (LinkGroup)i.next();
				idset.add(s.getId());
			}
			return (Long[])idset.toArray(new Long[0]);
		}
		catch(SQLException sqle) {
			logger.error("select failed with "+sqle.getMessage());
			throw sqle;
		}
	}

	private Set<LinkGroup> findLinkGroupIds(
		long sizeInBytes,
		AccessLatency al,
		RetentionPolicy rp) throws SQLException {
		try {
                        if (logger.isDebugEnabled()) {
                                logger.debug("findLinkGroupIds(sizeInBytes="+sizeInBytes+
                                             ", AccessLatency="+al+
                                             ", RetentionPolicy="+rp+
                                             ")");
                        }
			String select;
			if(al.equals(AccessLatency.ONLINE)) {
				if(rp.equals(RetentionPolicy.REPLICA)) {
					select = selectAllOnlineReplicaLinkGroup;
				}
				else
					if ( rp.equals(RetentionPolicy.OUTPUT)) {
						select = selectAllOnlineOutputLinkGroup;
					}
					else {
						select = selectAllOnlineCustodialLinkGroup;
					}

			}
			else {
				if(rp.equals(RetentionPolicy.REPLICA)) {
					select = selectAllNearlineReplicaLinkGroup;
				}
				else
					if ( rp.equals(RetentionPolicy.OUTPUT)) {
						select = selectAllNearlineOutputLinkGroup;
					}
					else {
						select = selectAllNearlineCustodialLinkGroup;
					}
			}
			IoPackage pkg = new LinkGroupIO();
			HashSet groups = null;
            if (logger.isDebugEnabled()) {
                    logger.debug("executing statement: "+select+
                                 "?="+latestLinkGroupUpdateTime+
                                 "?="+sizeInBytes
                                 );
            }
			groups=manager.selectPrepared(pkg,
						      select,
						      latestLinkGroupUpdateTime,
						      sizeInBytes);
			java.util.Set<LinkGroup> linkGroups = new java.util.HashSet();
			for(Iterator i=groups.iterator();i.hasNext();) {
                    LinkGroup s = (LinkGroup)i.next();
                    linkGroups.add(s);
			}
			return linkGroups;
		}
		catch(SQLException sqle) {
			logger.error("select failed with "+sqle.getMessage());
			throw sqle;
		}
	}

	public Space getSpace(long id)  throws SQLException{
                if (logger.isDebugEnabled()) {
                        logger.debug("Executing: "+SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_ID+",?="+id);
                }
		HashSet spaces=manager.selectPrepared(new SpaceReservationIO(),
						      SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_ID,
						      id);
		if (spaces.isEmpty()==true) {
			throw new SQLException("space reservation with id="+id+" not found");
		}
		return (Space)spaces.toArray()[0];
	}

	public LinkGroup getLinkGroup(long id)  throws SQLException{
		HashSet groups=manager.selectPrepared(new LinkGroupIO(),
					      LinkGroupIO.SELECT_LINKGROUP_BY_ID,
					      id);
		if (groups.isEmpty()==true) {
			throw new SQLException("linkGroup with id="+id+" not found");
		}
		return (LinkGroup)groups.toArray()[0];
	}

	public LinkGroup getLinkGroupByName(String name)  throws SQLException{
		HashSet groups=manager.selectPrepared(new LinkGroupIO(),
						      LinkGroupIO.SELECT_LINKGROUP_BY_NAME,
						      name);
		if (groups.isEmpty()==true) {
			throw new SQLException("linkGroup with name="+name+" not found");
		}
		return (LinkGroup)groups.toArray()[0];
	}

//------------------------------------------------------------------------------
// select for update functions
//------------------------------------------------------------------------------
	public LinkGroup selectLinkGroupForUpdate(Connection connection,long id,long sizeInBytes)  throws SQLException{
		Object o = null;
		try {
			o = manager.selectForUpdate(connection,
						    new LinkGroupIO(),
						    LinkGroupIO.SELECT_LINKGROUP_INFO_FOR_UPDATE,
						    id,
						    sizeInBytes);
		}
		catch (SQLException e) {
			throw new SQLException("There is no linkgroup with id="+id+" and available space="+sizeInBytes);
		}
		return (LinkGroup)o;
	}



	public LinkGroup selectLinkGroupForUpdate(Connection connection,long id)  throws SQLException{
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+LinkGroupIO.SELECT_LINKGROUP_FOR_UPDATE_BY_ID+",?="+id);
                }
		Object o = null;
		try {
			o = manager.selectForUpdate(connection,
						    new LinkGroupIO(),
						    LinkGroupIO.SELECT_LINKGROUP_FOR_UPDATE_BY_ID,
						    id);
		}
		catch (SQLException e) {
			throw new SQLException("There is no linkgroup with id="+id);
		}
		return (LinkGroup)o;
	}

	public Space selectSpaceForUpdate(Connection connection,long id,long sizeInBytes)  throws SQLException{
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+SpaceReservationIO.SELECT_FOR_UPDATE_BY_ID_AND_SIZE+",?="+id+","+sizeInBytes);
                }
		Object o = null;
		try {
			o = manager.selectForUpdate(connection,
						    new SpaceReservationIO(),
						    SpaceReservationIO.SELECT_FOR_UPDATE_BY_ID_AND_SIZE,
						    id,
						    sizeInBytes);
		}
		catch (SQLException e) {
			throw new SQLException("There is no space reservation with id="+id+" and available size="+sizeInBytes);
		}
		return (Space)o;
	}

	public Space selectSpaceForUpdate(Connection connection,long id)  throws SQLException{
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+SpaceReservationIO.SELECT_FOR_UPDATE_BY_ID+",?="+id);
                }
		Object o = null;
		try {
			o = manager.selectForUpdate(connection,
						    new SpaceReservationIO(),
						    SpaceReservationIO.SELECT_FOR_UPDATE_BY_ID,
						    id);
		}
		catch (SQLException e){
			throw new SQLException("There is no space reservation with id="+id);
		}
		return (Space)o;
	}

	public File selectFileForUpdate(Connection connection,String pnfsPath)  throws SQLException{
		pnfsPath =new FsPath(pnfsPath).toString();
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+FileIO.SELECT_FOR_UPDATE_BY_PNFSPATH+",?="+pnfsPath);
                }
		Object o = null;
		try {
			o = manager.selectForUpdate(connection,
						    new FileIO(),
						    FileIO.SELECT_FOR_UPDATE_BY_PNFSPATH,
						    pnfsPath);
		}
		catch (SQLException e){
			throw new SQLException("There is no file with pnfspath="+pnfsPath);
		}
		return (File)o;
	}

	public File selectFileForUpdate(Connection connection,PnfsId pnfsId)  throws SQLException{
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+FileIO.SELECT_FOR_UPDATE_BY_PNFSID+",?="+pnfsId);
                }
		Object o = null;
		try {
			o = manager.selectForUpdate(connection,
						    new FileIO(),
						    FileIO.SELECT_FOR_UPDATE_BY_PNFSID,
						    pnfsId.toString());
		}
		catch (SQLException e){
			throw new SQLException("There is no file with pnfsid="+pnfsId);
		}
		return (File)o;
	}

	public File selectFileForUpdate(Connection connection,long id)  throws SQLException{
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+FileIO.SELECT_FOR_UPDATE_BY_ID+",?="+id);
                }
		Object o = null;
		try {
			o = manager.selectForUpdate(connection,
						    new FileIO(),
						    FileIO.SELECT_FOR_UPDATE_BY_ID,
						    id);
		}
		catch (SQLException e){
			throw new SQLException("There is no file with id="+id);
		}
		return (File)o;
	}

	public File selectFileFromSpaceForUpdate(Connection connection,
                                                String pnfsPath,
                                                long reservationId) throws SQLException{
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+
                                     FileIO.SELECT_TRANSIENT_FILES_BY_PNFSPATH_AND_RESERVATIONID+
                                     ",?="+pnfsPath+","+reservationId);
                }
		return (File)manager.selectForUpdate(connection,
                                                     new FileIO(),
                                                     FileIO.SELECT_TRANSIENT_FILES_BY_PNFSPATH_AND_RESERVATIONID,
                                                     pnfsPath, reservationId);
	}


	public void removeFileFromSpace(long id) throws SQLException {
		boolean found = false;
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			File f = selectFileForUpdate(connection,id);
			removeFileFromSpace(connection,f);
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;
		}
		catch(SQLException sqle) {
			logger.error("delete failed with "+sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	public void removeFileFromSpace(Connection connection,
					File f) throws SQLException {
		Space space = selectSpaceForUpdate(connection,f.getSpaceId());
		int rc = manager.delete(connection,FileIO.DELETE,f.getId());
		if(rc!=1){
			throw new SQLException("delete returned row count ="+rc);
		}
		if(f.getState() == FileState.RESERVED ||
		   f.getState() == FileState.TRANSFERRING) {
			decrementAllocatedSpaceInSpaceReservation(connection,space,f.getSizeInBytes());
		}
		else if (f.getState() == FileState.STORED) {
			decrementUsedSpaceInSpaceReservation(connection,space,f.getSizeInBytes());
			incrementFreeSpaceInLinkGroup(connection,space.getLinkGroupId(),f.getSizeInBytes()); // keep freespaceinbytes in check
		}

	}


//------------------------------------------------------------------------------
	public void updateSpaceState(long id,SpaceState spaceState) throws SQLException {
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			updateSpaceReservation(connection,
					       id,
					       null,
					       null,
					       null,
					       null,
					       null,
					       null,
					       null,
					       null,
					       Integer.valueOf(spaceState.getStateId()));
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;
			return;
		}
		catch(SQLException sqle) {
			logger.error("update failed with "+sqle.getMessage());
			if(connection != null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	public void updateSpaceLifetime(Connection connection,long id, long newLifetime)
		throws SQLException {
		updateSpaceReservation(connection,
				       id,
				       null,
				       null,
				       null,
				       null,
				       null,
				       null,
				       Long.valueOf(newLifetime),
				       null,
				       null);
	}

	public void updateSpaceReservation(Connection connection,
					   long id,
					   String voGroup,
					   String voRole,
					   RetentionPolicy retentionPolicy,
					   AccessLatency accessLatency,
					   Long linkGroupId,
					   Long sizeInBytes,
					   Long lifetime,
					   String description,
					   Integer state) throws SQLException {

		Space space = selectSpaceForUpdate(connection,id);
		updateSpaceReservation(connection,
				       voGroup,
				       voRole,
				       retentionPolicy,
				       accessLatency,
				       linkGroupId,
				       sizeInBytes,
				       lifetime,
				       description,
				       state,
				       space);
	}

	public void updateSpaceReservation(Connection connection,
					   String voGroup,
					   String voRole,
					   RetentionPolicy retentionPolicy,
					   AccessLatency accessLatency,
					   Long linkGroupId,
					   Long sizeInBytes,
					   Long lifetime,
					   String description,
					   Integer state,
					   Space space) throws SQLException {
		if (voGroup!=null)         space.setVoGroup(voGroup);
		if (voRole!=null)          space.setVoRole(voRole);
		if (retentionPolicy!=null) space.setRetentionPolicy(retentionPolicy);
		if (accessLatency!=null)   space.setAccessLatency(accessLatency);
		long deltaSize = 0;
		long oldSize =  space.getSizeInBytes();
		LinkGroup group = null;
		if (sizeInBytes != null)  {
			if (sizeInBytes.longValue() < space.getUsedSizeInBytes()+space.getAllocatedSpaceInBytes()) {
				long usedSpace = space.getUsedSizeInBytes()+space.getAllocatedSpaceInBytes();
				throw new SQLException("Cannot downsize space reservation below "+usedSpace+"bytes, remove files first ");
			}
			deltaSize = sizeInBytes.longValue()-oldSize;
			space.setSizeInBytes(sizeInBytes.longValue());
			group = selectLinkGroupForUpdate(connection,
							 space.getLinkGroupId());
			if (group.getAvailableSpaceInBytes()<deltaSize) {
				throw new SQLException("No space available to resize space reservation");
			}
		}
		if(lifetime!=null)         space.setLifetime(lifetime.longValue());
		if(description!= null)     space.setDescription(description);
		SpaceState oldState = space.getState();
		if(state != null)  {
			if (SpaceState.isFinalState(oldState)==true) {
				throw new SQLException("change from "+oldState+" to "+state+" is not allowed");
			}
			if (group==null) {
				group = selectLinkGroupForUpdate(connection,
								 space.getLinkGroupId());
			}
			space.setState(SpaceState.getState(state.intValue()));
		}
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+SpaceReservationIO.UPDATE+",?="+space.getId());
                }
		manager.update(connection,
			       SpaceReservationIO.UPDATE,
			       space.getVoGroup(),
			       space.getVoRole(),
			       space.getRetentionPolicy().getId(),
			       space.getAccessLatency().getId(),
			       space.getLinkGroupId(),
			       space.getSizeInBytes(),
			       space.getCreationTime(),
			       space.getLifetime(),
			       space.getDescription(),
			       space.getState().getStateId(),
			       space.getId());

		if (state==null) {
			if (sizeInBytes != null) {
				if (deltaSize!=0) {
					if (!SpaceState.isFinalState(space.getState())) {
						incrementReservedSpaceInLinkGroup(connection,
										  group.getId(),
										  deltaSize);
					}
				}
			}
		}
		else {
			if (SpaceState.isFinalState(space.getState())) {
				decrementReservedSpaceInLinkGroup(connection,
								  group.getId(),
								  oldSize-space.getUsedSizeInBytes());
			}
		}
	}

	public void updateSpaceReservation(long id,
					   String voGroup,
					   String voRole,
					   RetentionPolicy retentionPolicy,
					   AccessLatency accessLatency,
					   Long linkGroupId,
					   Long sizeInBytes,
					   Long lifetime,
					   String description,
					   Integer state) throws SQLException {
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			updateSpaceReservation(connection,
					       id,
					       voGroup,
					       voRole,
					       retentionPolicy,
					       accessLatency,
					       linkGroupId,
					       sizeInBytes,
					       lifetime,
					       description,
					       state);
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;
		}
		catch(SQLException sqle) {
			logger.error("update failed with "+sqle.getMessage());
			if(connection != null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	public void expireSpaceReservations()  {
                if (logger.isDebugEnabled()) {
                        logger.debug("expireSpaceReservations()...");
                }
		try {
			if (cleanupExpiredSpaceFiles) {
				long time = System.currentTimeMillis();
                                if (logger.isDebugEnabled()) {
                                        logger.debug("Executing: "+SpaceReservationIO.SELECT_SPACE_RESERVATIONS_FOR_EXPIRED_FILES+"?="+time);
                                }
				HashSet spaces = manager.selectPrepared(new SpaceReservationIO(),
									SpaceReservationIO.SELECT_SPACE_RESERVATIONS_FOR_EXPIRED_FILES,
									time);
				for (Iterator i=spaces.iterator(); i.hasNext(); ) {
					Space space = (Space)i.next();
					//
					// for each space make a list of files in this space and clean them up
					//
					HashSet files = manager.selectPrepared(new FileIO(),
									       FileIO.SELECT_EXPIRED_SPACEFILES,
									       System.currentTimeMillis(),
									       space.getId());
					for (Iterator j=files.iterator(); j.hasNext(); ) {
						File file = (File)j.next();
						try {
							removeFileFromSpace(file.getId());
						}
						catch (SQLException e) {
							logger.error("Failed to remove file "+file+" due to "+e.getMessage());
							continue;
						}
					}
				}
			}
                        if (logger.isDebugEnabled()) {
                                logger.debug("Executing: "+SpaceReservationIO.SELECT_EXPIRED_SPACE_RESERVATIONS1);
                        }
			HashSet spaces = manager.selectPrepared(new SpaceReservationIO(),
								SpaceReservationIO.SELECT_EXPIRED_SPACE_RESERVATIONS1,
								System.currentTimeMillis());
			for (Iterator i=spaces.iterator(); i.hasNext(); ) {
				Space space = (Space)i.next();
				try {
					updateSpaceReservation(space.getId(),
							       null,
							       null,
							       null,
							       null,
							       null,
							       null,
							       null,
							       null,
							       Integer.valueOf(SpaceState.EXPIRED.getStateId()));
				}
				catch (SQLException e) {
					logger.error("Failed to expire space resevation ="+space+" ("+e.getMessage()+")");
					continue;
				}
			}
		}
		catch(SQLException sqle) {
			logger.error("expireSpaceReservations failed with "+sqle.getMessage());
		}
	}

	public long insertSpaceReservation(
		String voGroup,
		String voRole,
		RetentionPolicy retentionPolicy,
		AccessLatency accessLatency,
		long linkGroupId,
		long sizeInBytes,
		long lifetime,
		String description,
		int state,
		long used,
		long allocated
		) throws SQLException {
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			long id = getNextToken(connection);
			insertSpaceReservation(connection,
					       id,
					       voGroup,
					       voRole,
					       retentionPolicy,
					       accessLatency,
					       linkGroupId,
					       sizeInBytes,
					       lifetime,
					       description,
					       state,
					       used,
					       allocated);
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;
			return id;
		}
		catch(SQLException sqle) {
			logger.error(sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	public void insertSpaceReservation(
		Connection connection,
		long id,
		String voGroup,
		String voRole,
		RetentionPolicy retentionPolicy,
		AccessLatency accessLatency,
		long linkGroupId,
		long sizeInBytes,
		long lifetime,
		String description,
		int state,
		long used,
		long allocated) throws SQLException {
		long creationTime=System.currentTimeMillis();
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+SpaceReservationIO.INSERT);
                }
		LinkGroup g = selectLinkGroupForUpdate(connection,linkGroupId,sizeInBytes);
		int rc=manager.insert(connection,
				      SpaceReservationIO.INSERT,
				      id,
				      voGroup,
				      voRole,
				      retentionPolicy==null? 0 : retentionPolicy.getId(),
				      accessLatency==null? 0 : accessLatency.getId(),
				      linkGroupId,
				      sizeInBytes,
				      creationTime,
				      lifetime,
				      description,
				      state,
				      used,
				      allocated);
		if (rc!=1) {
			throw new SQLException("insert returned row count ="+rc);
		}
		//
		// Now increment reservedspaceinbytes
		//
		incrementReservedSpaceInLinkGroup(connection,linkGroupId,sizeInBytes);
	}

	//
	// functions for infoProvider
	//

       	public void getValidSpaceTokens(GetSpaceTokensMessage msg) throws SQLException {
		HashSet<Space> spaces = null;
		if(msg.getSpaceTokenId()!=null) {
			spaces = new HashSet<Space>();
			Space space = getSpace(msg.getSpaceTokenId().longValue());
			spaces.add(space);
		}
		else {
                        if (logger.isDebugEnabled()) {
                                logger.debug("executing statement: "+SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS);
                        }
			spaces=manager.selectPrepared(new SpaceReservationIO(),
							      SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS);

		}
		msg.setSpaceTokenSet(spaces);
	}


       	public void getValidSpaceTokenIds(GetSpaceTokenIdsMessage msg) throws SQLException {
		HashSet<Space> spaces = null;
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS);
                }
		spaces=manager.selectPrepared(new SpaceReservationIO(),
					      SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS);
		if (spaces != null) {
			long[] ids = new long[spaces.size()];
			int j=0;
			for (Iterator i=spaces.iterator(); i.hasNext();) {
				Space s = (Space)i.next();
				ids[j++]=s.getId();
			}
			msg.setSpaceTokenIds(ids);
		}
	}

	public void getLinkGroups(GetLinkGroupsMessage msg) throws SQLException {
		HashSet<LinkGroup> groups = null;
		if (msg.getLinkgroupidId()!=null) {
			groups = new HashSet<LinkGroup>();
			LinkGroup lg = getLinkGroup(msg.getLinkgroupidId().longValue());
			groups.add(lg);
		}
		else {
                        if (logger.isDebugEnabled()) {
                                logger.debug("executing statement: "+LinkGroupIO.SELECT_ALL_LINKGROUPS);
                        }
			groups=manager.selectPrepared(new LinkGroupIO(),
							      LinkGroupIO.SELECT_ALL_LINKGROUPS);
		}
		msg.setLinkGroupSet(groups);
	}

	public void getLinkGroupNames(GetLinkGroupNamesMessage msg) throws SQLException {
		HashSet<LinkGroup> groups = null;
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+LinkGroupIO.SELECT_ALL_LINKGROUPS);
                }
		groups=manager.selectPrepared(new LinkGroupIO(),
					      LinkGroupIO.SELECT_ALL_LINKGROUPS);
		if (groups!=null) {
			String[] names = new String[groups.size()];
			int j=0;
			for (Iterator i=groups.iterator(); i.hasNext();) {
				LinkGroup g = (LinkGroup)i.next();
				names[j++]=g.getName();
			}
			msg.setLinkGroupNames(names);
		}
	}


	public void getLinkGroupIds(GetLinkGroupIdsMessage msg) throws SQLException {
		HashSet<LinkGroup> groups = null;
                if (logger.isDebugEnabled()) {
                        logger.debug("executing statement: "+LinkGroupIO.SELECT_ALL_LINKGROUPS);
                }
		groups=manager.selectPrepared(new LinkGroupIO(),
					      LinkGroupIO.SELECT_ALL_LINKGROUPS);
		if (groups!=null) {
			long[] ids = new long[groups.size()];
			int j=0;
			for (Iterator i=groups.iterator(); i.hasNext();) {
				LinkGroup g = (LinkGroup)i.next();
				ids[j++]=g.getId();
			}
			msg.setLinkGroupIds(ids);
		}
	}

        public static final String SELECT_SPACE_TOKENS_BY_DESCRIPTION =
                "SELECT * FROM "+ManagerSchemaConstants.SpaceTableName +
                " WHERE  state = ? AND description = ?";

        public static final String SELECT_SPACE_TOKENS_BY_VOGROUP =
                "SELECT * FROM "+ManagerSchemaConstants.SpaceTableName +
                " WHERE  state = ? AND voGroup = ?";

        public static final String SELECT_SPACE_TOKENS_BY_VOROLE =
                "SELECT * FROM "+ManagerSchemaConstants.SpaceTableName +
                " WHERE  state = ? AND  voRole = ?";

        public static final String SELECT_SPACE_TOKENS_BY_VOGROUP_AND_VOROLE =
                "SELECT * FROM "+ManagerSchemaConstants.SpaceTableName +
                " WHERE  state = ? AND voGroup = ? AND voRole = ?";

    private Set findSpacesByVoGroupAndRole(String voGroup, String voRole)
            throws SQLException {
        if (voGroup != null && !voGroup.isEmpty() &&
                voRole != null && !voRole.isEmpty()) {
            return manager.selectPrepared(
                    new SpaceReservationIO(),
                    SELECT_SPACE_TOKENS_BY_VOGROUP_AND_VOROLE,
                    SpaceState.RESERVED.getStateId(),
                    voGroup,
                    voRole);
        } else {
            if (voGroup != null && !voGroup.isEmpty()) {
                return manager.selectPrepared(
                        new SpaceReservationIO(),
                        SELECT_SPACE_TOKENS_BY_VOGROUP,
                        SpaceState.RESERVED.getStateId(),
                        voGroup);
            }
            if (voRole != null && !voRole.isEmpty()) {
                return manager.selectPrepared(
                        new SpaceReservationIO(),
                        SELECT_SPACE_TOKENS_BY_VOROLE,
                        SpaceState.RESERVED.getStateId(),
                        voRole);
            }
        }
        return Collections.EMPTY_SET;
    }

	public long[] getSpaceTokens(AuthorizationRecord authRecord,
  				     String description)  throws SQLException{

        Set spaces = new HashSet();
        if(description == null) {
            String voGroup=authRecord.getVoGroup();
            String voRole =authRecord.getVoRole();

            spaces.addAll(findSpacesByVoGroupAndRole(voGroup, voRole));

            for(GroupList groupList: authRecord.getGroupLists()) {
                FQAN voAttribute = new FQAN(groupList.getAttribute());
                voGroup = voAttribute.getGroup();
                voRole = voAttribute.getRole();
                spaces.addAll(findSpacesByVoGroupAndRole(voGroup, voRole));
            }

        }
        else {
            Set foundSpaces =manager.selectPrepared(new  SpaceReservationIO(),
                                SELECT_SPACE_TOKENS_BY_DESCRIPTION,
                                SpaceState.RESERVED.getStateId(),
                                description);
            if(foundSpaces != null) {
                spaces.addAll(foundSpaces);
            }
        }

        Set<Long> tokenSet = new HashSet<Long>();
		for (Iterator i=spaces.iterator(); i.hasNext();){
			Space space = (Space)i.next();
			tokenSet.add(space.getId());
		}
		Long[] tokensLong =  tokenSet.toArray(new Long[0]);
		long[] tokens = new long[tokensLong.length];
		for(int i = 0; i< tokens.length;++i) {
			tokens[i] = tokensLong[i].longValue();
		}
		return tokens;
	}

        public static final String SELECT_SPACE_FILE_BY_PNFSID =
                "SELECT * FROM "+ ManagerSchemaConstants.SpaceFileTableName + " WHERE pnfsId = ? ";

        public static final String SELECT_SPACE_FILE_BY_PNFSPATH =
                "SELECT * FROM "+ ManagerSchemaConstants.SpaceFileTableName + " WHERE pnfsPath = ? ";

        public static final String SELECT_SPACE_FILE_BY_PNFSID_AND_PNFSPATH =
                "SELECT * FROM "+ ManagerSchemaConstants.SpaceFileTableName + " WHERE pnfsId = ? AND pnfsPath = ?";


	public long[] getFileSpaceTokens( PnfsId pnfsId,
					  String pnfsPath)  throws SQLException{

                if (pnfsId==null&&pnfsPath==null) { 
                        throw new IllegalArgumentException("getFileSpaceTokens: all arguments are nulls, not supported");
                }
                HashSet files = null;
                IoPackage pkg = new FileIO();
                if (pnfsId != null && pnfsPath != null) {
                        files = manager.selectPrepared(pkg,
                                                       SELECT_SPACE_FILE_BY_PNFSID_AND_PNFSPATH,
                                                       pnfsId.toString(),
                                                       new FsPath(pnfsPath).toString());
                }
                else {
                        if (pnfsId != null) {
                                files = manager.selectPrepared(pkg,
                                                               SELECT_SPACE_FILE_BY_PNFSID,
                                                               pnfsId.toString());
                        }
                        if (pnfsPath != null) {
                                files = manager.selectPrepared(pkg,
                                                               SELECT_SPACE_FILE_BY_PNFSPATH,
                                                               new FsPath(pnfsPath).toString());
                        }
                }
                Set<Long> tokenSet = new HashSet<Long>();
                for (Iterator i=files.iterator();i.hasNext();){
			File f=(File)i.next();
			tokenSet.add(f.getSpaceId());
		}
		Long[] tokensLong =  tokenSet.toArray(new Long[0]);
		long[] tokens = new long[tokensLong.length];
		for(int i = 0; i< tokens.length;++i) {
			tokens[i] = tokensLong[i].longValue();
		}
		return tokens;
	}


	public void deleteSpaceReservation(Connection connection, Space space) throws SQLException {
		manager.delete(connection,SpaceReservationIO.DELETE_SPACE_RESERVATION,space.getId());
		decrementReservedSpaceInLinkGroup(connection,
						  space.getLinkGroupId(),
						  space.getSizeInBytes()-space.getUsedSizeInBytes());
	}

	public void deleteSpaceReservation(long id)
 		throws SQLException {
 		boolean found = false;
 		Connection connection = null;
 		try {
 			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			Space space = selectSpaceForUpdate(connection,id);
			deleteSpaceReservation(connection,space);
 			connection.commit();
 			connection_pool.returnConnection(connection);
 			connection = null;
 		}
 		catch(SQLException sqle) {
 			logger.error(sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
 			throw sqle;
 		}
 		finally {
 			if(connection != null) {
 				connection_pool.returnConnection(connection);
 			}
 		}
	}

	public void updateSpaceFile(
		long id,
		String voGroup,
		String voRole,
		PnfsId pnfsId,
		Long sizeInBytes,
		Long lifetime,
		Integer state) throws SQLException {
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			updateSpaceFile(connection,id,voGroup,voRole,pnfsId,sizeInBytes,lifetime,state);
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;
		}
		catch(SQLException sqle) {
			logger.error("update failed with "+sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	public void updateSpaceFile(
		Connection connection,
		long id,
		String voGroup,
		String voRole,
		PnfsId pnfsId,
		Long sizeInBytes,
		Long lifetime,
		Integer state) throws SQLException {
		File f = selectFileForUpdate(connection,id);
		updateSpaceFile(connection,id,voGroup,voRole,pnfsId,sizeInBytes,lifetime,state,f);
	}

	public void updateSpaceFile(
		Connection connection,
		long id,
		String voGroup,
		String voRole,
		PnfsId pnfsId,
		Long sizeInBytes,
		Long lifetime,
		Integer state,
		File f) throws SQLException {
		if (voGroup!=null) f.setVoGroup(voGroup);
		if (voRole!=null)  f.setVoRole(voRole);
		long oldSize=f.getSizeInBytes();
		long deltaSize=0;
		Space space = null;
		if (sizeInBytes!=null) {
			deltaSize = sizeInBytes.longValue()-oldSize;
			if (deltaSize!=0) {
				f.setSizeInBytes(sizeInBytes.longValue());
				//
				// idea below is questionable. We resize space reservation to fit this file. This way we
				// attempt to guarantee that there is no negative numbers in LinkGroup
				//
				Connection newConnection = null;
				try {
					newConnection =  connection_pool.getConnection();
					newConnection.setAutoCommit(false);
					space = selectSpaceForUpdate(newConnection,f.getSpaceId());
					if (deltaSize > space.getAvailableSpaceInBytes()) {
						updateSpaceReservation(newConnection,
								       null,
								       null,
								       null,
								       null,
								       null,
								       Long.valueOf(space.getSizeInBytes()+deltaSize-space.getAvailableSpaceInBytes()),
								       null,
								       null,
								       null,
								       space);
					}
					newConnection.commit();
					connection_pool.returnConnection(newConnection);
					newConnection=null;
				}
				catch (SQLException e) {
					logger.error(e.getMessage());
					if (newConnection!=null) {
						newConnection.rollback();
						connection_pool.returnFailedConnection(newConnection);
						newConnection=null;
					}
					throw e;
				}
				space = selectSpaceForUpdate(connection,f.getSpaceId());
			}
		}
		if (lifetime!=null) f.setLifetime(lifetime.longValue());
		FileState oldState = f.getState();
		if (state!=null)   {
			if ( space == null ) {
				space = selectSpaceForUpdate(connection,f.getSpaceId());
			}
			f.setState(FileState.getState(state.intValue()));
		}
		if (pnfsId!=null ) f.setPnfsId(pnfsId);
		int rc = 0;
		if (f.getPnfsId()!=null) {
			rc = manager.update(connection,
					    FileIO.UPDATE,
					    f.getVoGroup(),
					    f.getVoRole(),
					    f.getSizeInBytes(),
					    f.getLifetime(),
					    f.getPnfsId().toString(),
					    f.getState().getStateId(),
					    f.getId());
		}
		else {
			rc = manager.update(connection,
					    FileIO.UPDATE_WO_PNFSID,
					    f.getVoGroup(),
					    f.getVoRole(),
					    f.getSizeInBytes(),
					    f.getLifetime(),
					    f.getState().getStateId(),
					    f.getId());
		}
		if (rc!=1) {
			throw new SQLException("Update failed, row count="+rc);
		}
		if (state==null) {
			if (sizeInBytes!=null) {
				if (deltaSize!=0) {
					if (f.getState()==FileState.STORED) {
						incrementUsedSpaceInSpaceReservation(connection,space,deltaSize);
						decrementFreeSpaceInLinkGroup(connection,space.getLinkGroupId(),deltaSize); // keep freespaceinbytes in check
					}
					if (f.getState()==FileState.RESERVED ||
					    f.getState()==FileState.TRANSFERRING) {
						incrementAllocatedSpaceInSpaceReservation(connection,space,deltaSize);
					}
				}
			}
		}
		else {
			if (oldState==FileState.RESERVED || oldState==FileState.TRANSFERRING) {
				if (f.getState() == FileState.STORED) {
					decrementAllocatedSpaceInSpaceReservation(connection,space,oldSize);
					incrementUsedSpaceInSpaceReservation(connection,space,f.getSizeInBytes());
					decrementFreeSpaceInLinkGroup(connection,space.getLinkGroupId(),f.getSizeInBytes()); // keep freespaceinbytes in check
				}
				else if (f.getState() == FileState.FLUSHED) {
					decrementAllocatedSpaceInSpaceReservation(connection,space,oldSize);
				}
			}
			else if (oldState== FileState.STORED) {
				if (f.getState() == FileState.FLUSHED) {
					decrementUsedSpaceInSpaceReservation(connection,space,oldSize);
					incrementFreeSpaceInLinkGroup(connection,space.getLinkGroupId(),f.getSizeInBytes()); // keep freespaceinbytes in check
				}
				else if (f.getState()==FileState.RESERVED || f.getState()==FileState.TRANSFERRING) {
					// this should not happen
					decrementUsedSpaceInSpaceReservation(connection,space,oldSize);
					incrementAllocatedSpaceInSpaceReservation(connection,space,f.getSizeInBytes());
					incrementFreeSpaceInLinkGroup(connection,space.getLinkGroupId(),f.getSizeInBytes()); // keep freespaceinbytes in check
				}
			}
		}
	}

	public void removePnfsIdOfFileInSpace(
		Connection connection,
		long id,
		Integer state) throws SQLException {
		if (state==null) {
			manager.update(connection,
				       FileIO.REMOVE_PNFSID_ON_SPACEFILE,
				       id);
		}
		else {
			manager.update(connection,
				       FileIO.REMOVE_PNFSID_AND_CHANGE_STATE_SPACEFILE,
				       id,
                                       state.intValue());
		}
	}

	public long insertFileInSpace(
		String voGroup,
		String voRole,
		long spaceReservationId,
		long sizeInBytes,
		long lifetime,
		String pnfsPath,
		PnfsId pnfsId,
		int state) throws SQLException,SpaceException {
		pnfsPath =new FsPath(pnfsPath).toString();
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			long id = getNextToken(connection);
			insertFileInSpace(connection,id,voGroup,voRole,spaceReservationId,sizeInBytes,lifetime,pnfsPath,pnfsId,state);
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;
			return id;
		}
		catch(SQLException sqle) {
			logger.error("insert failed with "+sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		catch(SpaceException e ) {
			logger.error("insert failed with "+e.getMessage());
			if (connection!=null){
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw e;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	public void insertFileInSpace( Connection connection,
				       long id,
				       String voGroup,
				       String voRole,
				       long spaceReservationId,
				       long sizeInBytes,
				       long lifetime,
				       String pnfsPath,
				       PnfsId pnfsId,
				       int state) throws SQLException, SpaceException {
		pnfsPath =new FsPath(pnfsPath).toString();
		HashSet files = manager.selectPrepared(new FileIO(),
						       FileIO.SELECT_TRANSFERRING_OR_RESERVED_BY_PNFSPATH,
						       pnfsPath);
		if (files!=null&&files.isEmpty()==false) {
			throw new SQLException("Already have "+files.size()+" record(s) with pnfsPath="+pnfsPath);
		}
		long creationTime=System.currentTimeMillis();
		int rc=0;
//		Space space = selectSpaceForUpdate(connection,spaceReservationId,sizeInBytes);
		Space space = selectSpaceForUpdate(connection,spaceReservationId,0L); // a hack needed to get a better error code from comparison below
		if (matchVoGroupAndVoRole==true) {
			if (voGroup==null) {
				if (space.getVoGroup()!=null) {
					if (!space.getVoGroup().equals("")&&!space.getVoGroup().equals("*")) {
						throw new SpaceAuthorizationException("VO group does not match, specified null, must be "+space.getVoGroup());
					}
				}
			}
			else {
				if (space.getVoGroup()!=null) {
					if (!space.getVoGroup().equals(voGroup)&&!space.getVoGroup().equals("*")) {
						throw new SpaceAuthorizationException("VO group does not match, specified "+voGroup +", must be "+space.getVoGroup());
					}
				}
				else {
					throw new SpaceAuthorizationException("VO group does not match, specified "+voGroup +", must be null");
				}
			}
			if (voRole==null) {
				if (space.getVoRole()!=null) {
					if (!space.getVoRole().equals("")&&!space.getVoRole().equals("*")) {
						throw new SpaceAuthorizationException("VO role does not match, specified null, must be "+space.getVoRole());
					}
				}
			}
			else {
				if (space.getVoRole()!=null) {
					if (!space.getVoRole().equals(voRole)&&!space.getVoRole().equals("*")) {
						throw new SpaceAuthorizationException("VO role does not match, specified "+voRole +", must be "+space.getVoRole());
					}
				}
				else {
					throw new SpaceAuthorizationException("VO role does not match, specified "+voRole +", must be null");
				}
			}
		}
		long currentTime = System.currentTimeMillis();
		if(space.getLifetime() != -1 && space.getCreationTime()+space.getLifetime()  < currentTime) {
			throw new SpaceExpiredException("space with id="+spaceReservationId+" has expired");
		}
		if (space.getState() == SpaceState.EXPIRED) {
			throw new SpaceExpiredException("space with id="+spaceReservationId+" has expired");
		}
		if (space.getState() == SpaceState.RELEASED) {
			throw new SpaceReleasedException("space with id="+spaceReservationId+" was released");
		}
		if (space.getAvailableSpaceInBytes()<sizeInBytes) {
			throw new NoFreeSpaceException("space with id="+spaceReservationId+" does not have enough space");
		}
		if (pnfsId==null) {
                        if (logger.isDebugEnabled()) {
                                logger.debug("executing statement: "+FileIO.INSERT_WO_PNFSID);
                        }
			rc=manager.insert(connection,
					  FileIO.INSERT_WO_PNFSID,
					  id,
					  voGroup,
					  voRole,
					  spaceReservationId,
					  sizeInBytes,
					  creationTime,
					  lifetime,
					  pnfsPath,
					  state);
		}
		else {
                        if (logger.isDebugEnabled()) {
                                logger.debug("executing statement: "+FileIO.INSERT_W_PNFSID);
                        }
			rc=manager.insert(connection,
					  FileIO.INSERT_W_PNFSID,
					  id,
					  voGroup,
					  voRole,
					  spaceReservationId,
					  sizeInBytes,
					  creationTime,
					  lifetime,
					  pnfsPath,
					  pnfsId.toString(),
					  state);
		}
		if(rc!=1 ){
			throw new SQLException("insert returned row count ="+rc);
		}
		if (state == FileState.RESERVED.getStateId()||state==FileState.TRANSFERRING.getStateId()) {
			incrementAllocatedSpaceInSpaceReservation(connection,space,sizeInBytes);
		}
		else if (state == FileState.STORED.getStateId()) {
			incrementUsedSpaceInSpaceReservation(connection,space,sizeInBytes);
			decrementAllocatedSpaceInSpaceReservation(connection,space,sizeInBytes);
			decrementFreeSpaceInLinkGroup(connection,space.getLinkGroupId(),sizeInBytes); // keep freespaceinbytes in check
		}
	}

	public File getFile(PnfsId pnfsId)  throws SQLException {
		HashSet files = manager.selectPrepared(new FileIO(),
						       FileIO.SELECT_BY_PNFSID,
						       pnfsId.toString());
		if (files.isEmpty()==true) {
			throw new SQLException("file with pnfsId="+pnfsId+" is not found");
		}
		if (files.size()>1) {
			throw new SQLException("found two records with pnfsId="+pnfsId);
		}
		return (File)files.toArray()[0];
	}

	public File getFile(String pnfsPath)  throws SQLException{
		pnfsPath =new FsPath(pnfsPath).toString();
		HashSet files = manager.selectPrepared(new FileIO(),
						       FileIO.SELECT_BY_PNFSPATH,
						       pnfsPath);
		if (files.isEmpty()==true) {
			throw new SQLException("file with pnfsPath="+pnfsPath+" is not found");
		}
		if (files.size()>1) {
			throw new SQLException("found two records with pnfsPath="+pnfsPath);
		}
		return (File)files.toArray()[0];
	}

	public Set getFiles(String pnfsPath)  throws SQLException{
		pnfsPath =new FsPath(pnfsPath).toString();
		HashSet files = manager.selectPrepared(new FileIO(),
						       FileIO.SELECT_BY_PNFSPATH,
						       pnfsPath);
		if (files.isEmpty()==true) {
			throw new SQLException("file with pnfsPath="+pnfsPath+" is not found");
		}
		return files;
	}

	public File getFile(long id)  throws SQLException{
		HashSet files = manager.selectPrepared(new FileIO(),
						       FileIO.SELECT_BY_ID,
						       id);
		if (files.isEmpty()==true) {
			throw new SQLException("file with id="+id+" is not found");
		}
		if (files.size()>1) {
			throw new SQLException("found two records with id="+id);
		}
		return (File)files.toArray()[0];
	}




//------------------------------------------------------------------------------
//      F I N A L L Y
//------------------------------------------------------------------------------

	public static final void main(String[] args) throws Throwable {
		if (args==null||args.length==0) {
			System.err.println("Need to specify DB connection arguments");
			System.err.println("e.g.: -jdbcUrl=jdbc:postgresql://localhost/dcache  -jdbcDriver=org.postgresql.Driver -dbUser=srmdcache -dbPass=srmdcache");
			System.exit(1);
		}
		Args arguments = new Args(args);
		String jdbcUrl   = arguments.getOpt("jdbcUrl");
		String jdbcClass = arguments.getOpt("jdbcDriver");
		String user      = arguments.getOpt("dbUser");
		String pass      = arguments.getOpt("dbPass");
		String pwdfile   = arguments.getOpt("pgPass");
		String pgBasedPass = null;
		if (pwdfile != null && !(pwdfile.trim().equals(""))) {
			Pgpass pgpass = new Pgpass(pwdfile);      //VP
			pgBasedPass = pgpass.getPgpass(jdbcUrl, user);   //VP
		}
		if(pgBasedPass != null) {
			pass = pgBasedPass;
		}
		try {
			DBManager manager = DBManager.getInstance();
			manager.initConnectionPool(jdbcUrl, jdbcClass, user, pass);
			int previousVersion = 0;
			Object o = null;
			try {
				o = manager.selectPrepared(1,selectVersion);
			}
			catch (SQLException e) {
				System.err.println("srmspacemanagerschemaversion does not exist - do nothing");
				System.exit(0);
			}
			if (o!=null) {
				previousVersion = ((Integer)o).intValue();
			}
			if (previousVersion==1) {
				StringBuffer sb = new StringBuffer();
				sb.append("\n");
				sb.append("\t\t    VERY IMPORTANT \n");
				sb.append("\n");
				sb.append("\tWe discovered that your current space manager schema version is "+previousVersion+".\n");
				sb.append("\tWe are upgrading to schema version 2.\n");
				sb.append("\tWe are going to update information in srmspace and srmlinkgroup tables\n");
				sb.append("\n");
				sb.append("\t+--------------------------------------------------------+\n");
				sb.append("\t|      FOR THE SPACE RESERVATIONS TO WORK CORRECTLY      |\n");
				sb.append("\t| IT IS ABSOLUTELY IMPORTANT TO LET THESE QUERIES FINISH |\n");
				sb.append("\t| IT MAY TAKE A LONG TIME (MINUTES), PLEASE BEAR WITH US |\n");
				sb.append("\t|                 DO NOT INTERRUPT                       |\n");
				sb.append("\t| THIS ACTION WILL NOT BE REPEATED IN SUBSEQUENT STARTUPS|\n");
                                sb.append("\t+--------------------------------------------------------+\n");
				System.out.println(sb.toString());
				try {
					manager.batchUpdates("ALTER TABLE " +ManagerSchemaConstants.LinkGroupTableName+ " ADD COLUMN  reservedspaceinbytes BIGINT",
							     "ALTER TABLE " +ManagerSchemaConstants.SpaceTableName    +
							     " ADD COLUMN  usedspaceinbytes      BIGINT,"+
							     " ADD COLUMN  allocatedspaceinbytes  BIGINT");
					manager.createIndexes(ManagerSchemaConstants.SpaceFileTableName,"spacereservationid","state","pnfspath","pnfsid","creationtime","lifetime");
					manager.createIndexes(ManagerSchemaConstants.SpaceTableName,"linkgroupid","state","description","lifetime","creationtime");
					manager.update(Manager.updateVersion);
					HashSet spaces = null;
					try {
						spaces=manager.selectPrepared( new  SpaceReservationIO(),
									       SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS);
						if (spaces != null) {
							System.out.println("Found "+spaces.size() + " space reservations to update");
						}
						for (Iterator i=spaces.iterator(); i.hasNext();) {
							Space space = (Space)i.next();
							try {
								manager.update(ManagerSchemaConstants.POPULATE_USED_SPACE_IN_SRMSPACE_TABLE_BY_ID,
									       space.getId(),
									       space.getId(),
									       space.getId());


							}
							catch(SQLException e) {
								System.err.println("failed to execute "+ManagerSchemaConstants.POPULATE_USED_SPACE_IN_SRMSPACE_TABLE_BY_ID+",?="+space.getId());
							}
						}
					}
					catch (SQLException e) {
						e.printStackTrace();
					}
					//
					// Do the same with linkgroups
					//
					HashSet groups = null;
					try {
						groups=manager.selectPrepared( new LinkGroupIO(),
									       LinkGroupIO.SELECT_ALL);
						if (groups != null) {
							System.out.println("Found "+groups.size() + " link groups to update");
						}
						for (Iterator i=groups.iterator(); i.hasNext();) {
							LinkGroup group = (LinkGroup)i.next();
							try {
								manager.update(ManagerSchemaConstants.POPULATE_RESERVED_SPACE_IN_SRMLINKGROUP_TABLE_BY_ID,
									       group.getId(),
									       group.getId());
							}
							catch(SQLException e) {
								System.err.println("failed to execute "+ManagerSchemaConstants.POPULATE_RESERVED_SPACE_IN_SRMLINKGROUP_TABLE_BY_ID+",?="+group.getId());
							}
						}
					}
					catch (SQLException e) {
						e.printStackTrace();
					}
					System.out.println("\tSUCCESSFULLY UPDATED SPACE MANAGER SCHEMA TO VERSION 2");
					System.out.println("\tIT IS SAFE TO START SRM");
					System.out.println("\tStarting SRM\n");
					System.exit(0);
				}
				catch (SQLException e) {
					System.err.println("THINGS WENT WRONG! PLEASE DO NOT ATTEMPT TO START SRM");
					System.err.println("send mail with the output to litvinse@fnal.gov");
					System.err.println("I will try diagnose the problem and give you solution");
					System.exit(1);
				}
			}
		}
		catch (SQLException e) {
			System.err.println("Caught SQL exception");
			e.printStackTrace();
			System.exit(1);
		}
		catch (Exception e) {
			System.err.println("Caught Exception");
			e.printStackTrace();
			System.exit(1);
		}

	}

	public void messageArrived( final CellMessage cellMessage ) {
		diskCacheV111.util.ThreadManager.execute(new Runnable() {
				public void run() {
					processMessage(cellMessage);
				}
			});
	}

	private void processMessage( CellMessage cellMessage ) {
		Object object = cellMessage.getMessageObject();
                if (logger.isDebugEnabled()) {
                        logger.debug("Message  arrived: "+object +" from "+cellMessage.getSourcePath());
                }
		if (!(object instanceof Message)) {
			logger.error("Unexpected message class "+object.getClass());
			return;
		}
		Message spaceMessage = (Message)object;
		boolean replyRequired = spaceMessage.getReplyRequired();
		try {
			if(spaceMessage instanceof Reserve) {
				Reserve reserve = (Reserve) spaceMessage;
				reserveSpace(reserve);
			}
			else if(spaceMessage instanceof GetSpaceTokensMessage) {
				GetSpaceTokensMessage message = (GetSpaceTokensMessage) spaceMessage;
				getValidSpaceTokens(message);
			}
			else if(spaceMessage instanceof GetSpaceTokenIdsMessage) {
				GetSpaceTokenIdsMessage message = (GetSpaceTokenIdsMessage) spaceMessage;
				getValidSpaceTokenIds(message);
			}
			else if(spaceMessage instanceof GetLinkGroupsMessage) {
				GetLinkGroupsMessage message = (GetLinkGroupsMessage) spaceMessage;
				getLinkGroups(message);
			}
			else if(spaceMessage instanceof GetLinkGroupNamesMessage) {
				GetLinkGroupNamesMessage message = (GetLinkGroupNamesMessage) spaceMessage;
				getLinkGroupNames(message);
			}
			else if(spaceMessage instanceof GetLinkGroupIdsMessage) {
				GetLinkGroupIdsMessage message = (GetLinkGroupIdsMessage) spaceMessage;
				getLinkGroupIds(message);
			}
			else if(spaceMessage instanceof Release) {
				Release release = (Release) spaceMessage;
				releaseSpace(release);
			}
			else if(spaceMessage instanceof Use){
				Use use = (Use) spaceMessage;
				useSpace(use);
			}
			else if(spaceMessage instanceof CancelUse){
				CancelUse cancelUse = (CancelUse) spaceMessage;
				cancelUseSpace(cancelUse);
			}
			else if(spaceMessage instanceof PoolMgrSelectPoolMsg) {
				selectPool(cellMessage,false);
				replyRequired = false;
			}
			else if(spaceMessage instanceof GetSpaceMetaData){
				GetSpaceMetaData getSpaceMetaData = (GetSpaceMetaData) spaceMessage;
				getSpaceMetaData(getSpaceMetaData);
			}
			else if(spaceMessage instanceof GetSpaceTokens){
				GetSpaceTokens getSpaceTokens = (GetSpaceTokens) spaceMessage;
				getSpaceTokens(getSpaceTokens);
			}
			else if(spaceMessage instanceof ExtendLifetime){
				ExtendLifetime extendLifetime = (ExtendLifetime) spaceMessage;
				extendLifetime(extendLifetime);
			}
			else if(spaceMessage instanceof PoolFileFlushedMessage) {
				PoolFileFlushedMessage fileFlushed = (PoolFileFlushedMessage) spaceMessage;
				fileFlushed(fileFlushed);
			}
			else if(spaceMessage instanceof PoolRemoveFilesMessage) {
				PoolRemoveFilesMessage fileRemoved = (PoolRemoveFilesMessage) spaceMessage;
				fileRemoved(fileRemoved);
			}
			else if(spaceMessage instanceof GetFileSpaceTokensMessage) {
				GetFileSpaceTokensMessage getFileTokens = (GetFileSpaceTokensMessage) spaceMessage;
				getFileSpaceTokens(getFileTokens);

			}
			else if (spaceMessage instanceof PnfsSetStorageInfoMessage) {
				PnfsSetStorageInfoMessage setStorageInfoMessage = (PnfsSetStorageInfoMessage) spaceMessage;
				if (setStorageInfoMessage.getReturnCode()!=0) {
					logger.error("Failed to set storageinfo");
				}
				return;
			}
			else if (spaceMessage instanceof PnfsDeleteEntryNotificationMessage) {
				PnfsDeleteEntryNotificationMessage msg = (PnfsDeleteEntryNotificationMessage)spaceMessage;
				markFileDeleted(msg);
			}
			else {
				logger.error("unknown Space Manager message type :"+spaceMessage.getClass().getName()+" value: "+spaceMessage);
				super.messageArrived(cellMessage);
				return;
			}
		}
		catch(SpaceException se) {
                        if(spaceManagerEnabled) {
                                logger.error("SpaceException: "+se.getMessage());
                                spaceMessage.setFailed(-2,se);
                        }
                        else {
                                if (logger.isDebugEnabled()) {
                                        logger.error("SpaceException: "+se.getMessage());
                                }
                                spaceMessage.setFailed(0,se);
                        }
		}
		catch(Throwable t) {
			logger.error(t.getMessage(),t);
			spaceMessage.setFailed(-1,t);
		}
		if (replyRequired) {
			try {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("Sending reply "+spaceMessage);
                                }
				cellMessage.revertDirection();
				sendMessage(cellMessage);
			}
			catch (Exception e) {
				logger.error("Can't reply message : "+e.getMessage());
			}
		}
		else {
                        if (logger.isDebugEnabled()) {
                                logger.debug("reply is not required, finished processing");
                        }
		}
	}

	public void messageToForward(final CellMessage cellMessage ){
		diskCacheV111.util.ThreadManager.execute(new Runnable() {
				public void run() {
					processMessageToForward(cellMessage);
				}
			});
	}

	public void processMessageToForward(CellMessage cellMessage ) {
		Object object = cellMessage.getMessageObject();
                if (logger.isDebugEnabled()) {
                        logger.debug("messageToForward,  arrived: type="+
                                     object.getClass().getName()+
                                     " value="+object +" from "+
                                     cellMessage.getSourcePath()+
                                     " going to "+cellMessage.getDestinationPath()+
                                     cellMessage.isAcknowledge());
                }
		try {
			if( object instanceof PoolMgrSelectPoolMsg) {
				selectPool(cellMessage,true);
			}
			else if(object instanceof PoolAcceptFileMessage ) {
				PoolAcceptFileMessage poolRequest = (PoolAcceptFileMessage)object;
				if(poolRequest.isReply()) {
					PnfsId pnfsId = poolRequest.getPnfsId();
					//mark file as being transfered
					transferStarted(pnfsId,poolRequest.getReturnCode() == 0);
				}
				else {
					// this message on its way to the pool
					// we need to set the AccessLatency, RetentionPolicy and StorageGroup
					transferToBeStarted(poolRequest);
				}
			}
			else if ( object instanceof DoorTransferFinishedMessage) {
				DoorTransferFinishedMessage finished = (DoorTransferFinishedMessage) object;
				try {
					transferFinished(finished);
				}
				catch (Exception e) {
					//
					// we fail if we were unable to put file in space reservation (litvinse@fnal.gov)
					//
					finished.setFailed(1,e);
				}
			}
		}
		catch (Exception e){
			logger.error(e.getMessage(),e);
		}
		super.messageToForward(cellMessage) ;
	}

	public void exceptionArrived(ExceptionEvent ee) {
		logger.error("Exception Arrived: "+ee);
		super.exceptionArrived(ee);
	}

	public void returnFailedResponse(Object reason ,
					 Message spaceManagerMessage,
					 CellMessage cellMessage) {
		if( reason != null && !(reason instanceof java.io.Serializable)) {
			reason = reason.toString();
		}
		try {
			spaceManagerMessage.setReply();
			spaceManagerMessage.setFailed(1, reason);
			cellMessage.revertDirection();
			sendMessage(cellMessage);
		}
		catch(Exception e) {
			logger.error("can not send a failed response "+e.getMessage());
		}
	}

	public void returnMessage(Message message, CellMessage cellMessage) {
		try {
			message.setReply();
			cellMessage.revertDirection();
			sendMessage(cellMessage);
		}
		catch(Exception e) {
			logger.error("can not send a response "+e.getMessage());
		}
	}

	private final Object updateLinkGroupsSyncObject = new Object();
	public void run(){
		if(Thread.currentThread() == expireSpaceReservations) {
			while(true) {
				expireSpaceReservations();
				try{
					Thread.sleep(expireSpaceReservationsPeriod);
				}
				catch (InterruptedException ie) {
					logger.error("expire SpaceReservations thread has been interrupted");
					return;
				}
			}
		}
		else if(Thread.currentThread() == updateLinkGroups) {
			while(true) {
				updateLinkGroups();
				synchronized(updateLinkGroupsSyncObject) {
					try {
						updateLinkGroupsSyncObject.wait(updateLinkGroupsPeriod);
					}
					catch (InterruptedException ie) {
						logger.error("update LinkGroup thread has been interrupted");
						return;
					}
				}
			}
		}
	}

	private long latestLinkGroupUpdateTime =System.currentTimeMillis();
	private LinkGroupAuthorizationFile linkGroupAuthorizationFile;
	private long linkGroupAuthorizationFileLastUpdateTimestampt = 0;

	private void updateLinkGroupAuthorizationFile() {
		if(linkGroupAuthorizationFileName == null) {
			return;
		}
		java.io.File f = new java.io.File (linkGroupAuthorizationFileName);
		if(!f.exists()) {
			linkGroupAuthorizationFile = null;
		}
		long lastModified = f.lastModified();
		if(linkGroupAuthorizationFile == null || lastModified >= linkGroupAuthorizationFileLastUpdateTimestampt) {
			linkGroupAuthorizationFileLastUpdateTimestampt = lastModified;
			try {
				linkGroupAuthorizationFile =
					new LinkGroupAuthorizationFile(linkGroupAuthorizationFileName);
			}
			catch(Exception e) {
                                logger.error(e.getMessage());
			}
		}
	}

	private void updateLinkGroups() {
		long currentTime = System.currentTimeMillis();
		CellMessage cellMessage = new CellMessage(new CellPath(poolManager),
							  new PoolMgrGetPoolLinkGroups());
		PoolMgrGetPoolLinkGroups getLinkGroups = null;
		try {
			cellMessage = sendAndWait(cellMessage,1000*5*60);
			if(cellMessage == null ) {
				logger.error("updateLinkGroups() : request timed out");
                                return;
			}
			if (cellMessage.getMessageObject() == null ) {
				logger.error("updateLinkGroups() : reply message is null");
                                return;
			}
			if( ! (cellMessage.getMessageObject() instanceof PoolMgrGetPoolLinkGroups)){
				return;
			}
			getLinkGroups = (PoolMgrGetPoolLinkGroups)cellMessage.getMessageObject();
			if(getLinkGroups.getReturnCode() != 0) {
				logger.error("  PoolMgrGetPoolLinkGroups reply return code ="+getLinkGroups.getReturnCode() +
                                             " error Object= "+getLinkGroups.getErrorObject());
				return;
			}
		}
		catch(Exception e) {
			logger.error("update failed "+e.getMessage());
			return;
		}
		PoolLinkGroupInfo[] poolLinkGroupInfos = getLinkGroups.getPoolLinkGroupInfos();
		if(poolLinkGroupInfos.length == 0) {
			return;
		}
		updateLinkGroupAuthorizationFile();
		for (int i = 0 ; i<poolLinkGroupInfos.length; ++i) {
			PoolLinkGroupInfo info = poolLinkGroupInfos[i];
			String linkGroupName   = info.getName();
			long avalSpaceInBytes  = info.getAvailableSpaceInBytes();
			VOInfo[] vos=null;
			boolean onlineAllowed    = info.isOnlineAllowed();
			boolean nearlineAllowed  = info.isNearlineAllowed();
			boolean replicaAllowed   = info.isReplicaAllowed();
			boolean outputAllowed    = info.isOutputAllowed();
			boolean custodialAllowed = info.isCustodialAllowed();
			if(linkGroupAuthorizationFile != null) {
				LinkGroupAuthorizationRecord record =
					linkGroupAuthorizationFile.getLinkGroupAuthorizationRecord(linkGroupName);
				if(record != null) {
					FQAN[] fqans = record.getFqanArray();
					if(fqans != null  && fqans.length >0) {
						int vos_length = fqans.length;
						vos = new VOInfo[vos_length];
						for(int j = 0; j<fqans.length ; ++j) {
                            String voRole = fqans[j].getRole();
                            voRole = (voRole==null || voRole.equals("")) ? "*" : voRole;
							vos[j] = new VOInfo(fqans[j].getGroup(), voRole);
						}

					}
				}
			}
			try {
				updateLinkGroup(linkGroupName,
						    avalSpaceInBytes,
						    currentTime,
						    onlineAllowed,
						    nearlineAllowed,
						    replicaAllowed,
						    outputAllowed,
						    custodialAllowed,
						    vos);
			}
			catch(SQLException sqle) {
				logger.error("update of linkGroup "+linkGroupName+" failed with exception: "+sqle.getMessage()); 
			}
		}
		latestLinkGroupUpdateTime = currentTime;
	}

        private static final String INSERT_LINKGROUP_VO = "INSERT INTO "+ManagerSchemaConstants.LinkGroupVOsTableName +
                " ( VOGroup, VORole, linkGroupId ) VALUES ( ? , ? , ? )";

        private static final String DELETE_LINKGROUP_VO = "DELETE FROM "+ManagerSchemaConstants.LinkGroupVOsTableName +
                " WHERE VOGroup  = ? AND VORole = ? AND linkGroupId = ? ";

	private long updateLinkGroup(
		String linkGroupName,
		long freeSpace,
		long updateTime,
		boolean onlineAllowed,
		boolean nearlineAllowed,
		boolean replicaAllowed,
		boolean outputAllowed,
		boolean custodialAllowed,
		VOInfo[] linkGroupVOs
		) throws SQLException {
		long id;
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			try {
				LinkGroup group = (LinkGroup) manager.selectForUpdate(connection,
										      new LinkGroupIO(),
										      LinkGroupIO.SELECT_LINKGROUP_FOR_UPDATE_BY_NAME,
										      linkGroupName);
				id=group.getId();
			}
			catch (SQLException e) {
				logger.error(e.getMessage());
				id=getNextToken();
				try {
					manager.insert(connection,
						       LinkGroupIO.INSERT,
						       id,
						       linkGroupName,
						       freeSpace,
						       updateTime,
						       (onlineAllowed==true?1:0),
						       (nearlineAllowed==true?1:0),
						       (replicaAllowed==true?1:0),
						       (outputAllowed==true?1:0),
						       (custodialAllowed==true?1:0),
						       0);
				}
				catch (SQLException e1) {
					logger.error("Failed to insert Link Group = "+linkGroupName+" "+e1.getMessage());
					if (connection!=null) {
						connection.rollback();
						connection_pool.returnFailedConnection(connection);
						connection = null;
					}
					throw e1;
				}
			}
			manager.update(connection,
				       LinkGroupIO.UPDATE,
				       freeSpace,
				       updateTime,
				       (onlineAllowed==true?1:0),
				       (nearlineAllowed==true?1:0),
				       (replicaAllowed==true?1:0),
				       (outputAllowed==true?1:0),
				       (custodialAllowed==true?1:0),
				       id);
			PreparedStatement sqlStatement2 =
				connection.prepareStatement(selectLinkGroupVOs);
			sqlStatement2.setLong(1,id);
			ResultSet VOsSet = sqlStatement2.executeQuery();
			Set<VOInfo> insertVOs = new HashSet<VOInfo>();
			if(linkGroupVOs != null) {
				insertVOs.addAll(java.util.Arrays.asList(linkGroupVOs));
			}
			Set<VOInfo> deleteVOs = new HashSet<VOInfo>();
			while(VOsSet.next()) {
				String nextVOGroup =    VOsSet.getString(1);
				String nextVORole =    VOsSet.getString(2);
				VOInfo nextVO = new VOInfo(nextVOGroup,nextVORole);
				if(insertVOs.contains(nextVO)){
					insertVOs.remove(nextVO);
				}
				else {
					deleteVOs.add(nextVO);
				}
			}
                        VOsSet.close();
                        sqlStatement2.close();
			for(VOInfo nextVo :insertVOs ) { 
                                manager.update(connection,
                                               INSERT_LINKGROUP_VO,
                                               nextVo.getVoGroup(),
                                               nextVo.getVoRole(),
                                               id);
			}
			for(VOInfo nextVo : deleteVOs ) { 
                                manager.update(connection,
                                               DELETE_LINKGROUP_VO,
                                               nextVo.getVoGroup(),
                                               nextVo.getVoRole(),
                                               id);
			}
			connection.commit();
			connection_pool.returnConnection(connection);
                        connection=null;
			return id;
		}
		catch(SQLException sqle) {
			logger.error("update failed with "+sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	private void releaseSpace(Release release) throws
		SQLException,SpaceException {
                if (logger.isDebugEnabled()) {
                        logger.debug("releaseSpace("+release+")");
                }
		if(!spaceManagerEnabled) {
			throw new SpaceException("SpaceManager is disabled in configuration");
		}
		long spaceToken = release.getSpaceToken();
		Long spaceToReleaseInBytes = release.getReleaseSizeInBytes();
        Space space = getSpace(spaceToken);
        AuthorizationRecord authRecord =  release.getAuthRecord();
        checkReleasePermission(authRecord, space);
		if(spaceToReleaseInBytes == null) {
			updateSpaceState(spaceToken,SpaceState.RELEASED);
			return;
		}
		else {
			throw new SQLException("partial release is not supported yet");
		}
	}

	//
	// working on the core stuff:
	//

	private void reserveSpace(Reserve reserve)
		throws SQLException,java.io.IOException,SpaceException{

		if(!spaceManagerEnabled) {
			throw new SpaceException("SpaceManager is disabled in configuration");
		}

        if (reserve.getRetentionPolicy()==null) {
                throw new IllegalArgumentException("reserveSpace : retentionPolicy=null is not supported");
        }

		long reservationId = reserveSpace(reserve.getAuthRecord(),
              reserve.getSizeInBytes(),
              (reserve.getAccessLatency()==null?
               defaultLatency:reserve.getAccessLatency()),
              reserve.getRetentionPolicy(),
              reserve.getLifetime(),
              reserve.getDescription());

		reserve.setSpaceToken(reservationId);
	}

	public File reserveAndUseSpace(String pnfsPath,PnfsId pnfsId,long size,AccessLatency latency,RetentionPolicy policy,VOInfo voinfo)
		throws SQLException,java.io.IOException,SpaceException {
		long sizeInBytes = size;
		long lifetime    = 1000*60*60;
		String voGroup   = null;
		String voRole    = null;
		if(voinfo != null){
			voGroup = voinfo.getVoGroup();
			voRole = voinfo.getVoRole();
		}
		String description = null;
		long reservationId = reserveSpace(voGroup,voRole,sizeInBytes,latency,policy,lifetime,description);
		long fileId = useSpace(reservationId,voGroup,voRole,sizeInBytes,lifetime,pnfsPath,pnfsId);
		File file = getFile(fileId);
		return file;
	}

	private void useSpace(Use use)
		throws SQLException, SpaceException{
		if(!spaceManagerEnabled) {
			throw new SpaceException("SpaceManager is disabled in configuration");
		}
                if (logger.isDebugEnabled()) {
                        logger.debug("useSpace("+use+")");
                }
		long reservationId = use.getSpaceToken();
		long sizeInBytes = use.getSizeInBytes();
		String voGroup = use.getAuthRecord().getVoGroup();
		String voRole = use.getAuthRecord().getVoRole();
		String pnfsPath = use.getPnfsName();
		PnfsId pnfsId = use.getPnfsId();
		long lifetime = use.getLifetime();
		boolean overwriteFlag = use.isOverwrite();
		long fileId = useSpace(reservationId,voGroup,voRole,sizeInBytes,lifetime,pnfsPath,pnfsId);
		use.setFileId(fileId);
	}

	private void transferToBeStarted(PoolAcceptFileMessage poolRequest){
		if (!spaceManagerEnabled) return;
		PnfsId pnfsId = poolRequest.getPnfsId();
                if (logger.isDebugEnabled()) {
                        logger.debug("transferToBeStarted("+pnfsId+")");
                }
		try {
			File f  = getFile(pnfsId);
			Space s = getSpace(f.getSpaceId());
			StorageInfo info = poolRequest.getStorageInfo();
			info.setAccessLatency(s.getAccessLatency());
			info.isSetAccessLatency(true);
			info.setRetentionPolicy(s.getRetentionPolicy());
			info.isSetRetentionPolicy(true);
			//
			// send message to PnfsManager
			//
                        if (logger.isDebugEnabled()) {
                                logger.debug("transferToBeStarted(), set AL to "+
                                             s.getAccessLatency()+
                                             " RP to "+s.getRetentionPolicy()+
                                             " , sending message to "+
                                             pnfsManager);
                        }
			try {
				PnfsSetStorageInfoMessage msg = new PnfsSetStorageInfoMessage(pnfsId,info,NameSpaceProvider.SI_OVERWRITE);
				msg.setReplyRequired(false);
				sendMessage(new CellMessage(new CellPath(pnfsManager),msg));
			}
			catch (Exception e) {
				logger.error("Can't send PnfsSetStorageInfoMessage message to pnfsmanager" + e.getMessage());
			}
		}
		catch(SQLException sqle){
			logger.error("transferToBeStarted(): could not get space reservation related to this transfer ");
		}
	}

	private void transferStarted(PnfsId pnfsId,boolean success) {
                if (logger.isDebugEnabled()) {
                        logger.debug("transferStarted("+pnfsId+","+success+")");
                }
		if ( !spaceManagerEnabled) return;
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			if(!success) {
				logger.error("transfer start up failed");
				File f = selectFileForUpdate(connection,pnfsId);
				if(f == null) {
					connection.rollback();
					connection_pool.returnConnection(connection);
					connection = null;
					return;
				}
				if(f.getState() == FileState.RESERVED ||
				   f.getState() == FileState.TRANSFERRING) {
					removePnfsIdOfFileInSpace(connection,f.getId(),
								  Integer.valueOf(FileState.RESERVED.getStateId()));
					connection.commit();
					connection_pool.returnConnection(connection);
					connection = null;
				}
				else {
					connection.commit();
					connection_pool.returnConnection(connection);
					connection = null;
				}
				return;
			}
			File f = selectFileForUpdate(connection,pnfsId);
			if(f == null) {
				connection.rollback();
				connection_pool.returnConnection(connection);
				connection = null;
				return;
			}
			if(f.getState() == FileState.RESERVED ||
			   f.getState() == FileState.TRANSFERRING) {
				updateSpaceFile(connection,
						f.getId(),
						null,
						null,
						null,
						null,
						null,
						Integer.valueOf(FileState.TRANSFERRING.getStateId()),
						f);
				connection.commit();
				connection_pool.returnConnection(connection);
				connection = null;
			}
			else {
				connection.commit();
				connection_pool.returnConnection(connection);
				connection = null;
			}

		}
		catch(SQLException sqle) {
			logger.error("transferStarted failed with "+sqle.getMessage());
			if (connection!=null) {
				try {
					connection.rollback();
				}
				catch (SQLException e) {}
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	private void transferFinished(DoorTransferFinishedMessage finished) throws Exception {
		if (!spaceManagerEnabled) return;
		boolean weDeleteStoredFileRecord = deleteStoredFileRecord;
		PnfsId pnfsId = finished.getPnfsId();
		StorageInfo storageInfo = finished.getStorageInfo();
		long size = storageInfo.getFileSize();
		boolean success = finished.getReturnCode() == 0;
                if (logger.isDebugEnabled()) {
                        logger.debug("transferFinished("+pnfsId+","+success+")");
                }
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			File f = null;
			try {
				f = selectFileForUpdate(connection,pnfsId);
				if(f == null) {
					connection.rollback();
					connection_pool.returnConnection(connection);
					connection = null;
					return;
				}
			}
			catch (Exception e) {
				logger.error("file "+pnfsId+":"+e.getMessage());
				if (connection!=null) {
					connection.rollback();
					connection_pool.returnConnection(connection);
					connection = null;
				}
				return;
			}
			long spaceId = f.getSpaceId();
			if(f.getState() == FileState.RESERVED ||
			   f.getState() == FileState.TRANSFERRING) {
				if(success) {
					if(returnFlushedSpaceToReservation && weDeleteStoredFileRecord) {
						RetentionPolicy rp = getSpace(spaceId).getRetentionPolicy();
						if(rp.equals(RetentionPolicy.CUSTODIAL)) {
							//we do not delete it here, since the
							// file will get flushed and we will need
							// to account for that
							weDeleteStoredFileRecord = false;
						}
					}
					if(weDeleteStoredFileRecord) {
                                                if (logger.isDebugEnabled()) {
                                                        logger.debug("file transfered, deleting file record");
                                                }
						removeFileFromSpace(connection,f);
					}
					else {
						updateSpaceFile(connection,f.getId(),
								null,
								null,
								null,
								Long.valueOf(size),
								null,
								Integer.valueOf(FileState.STORED.getStateId()),
								f);
					}
				}
				else {
					updateSpaceFile(connection,f.getId(),
							null,
							null,
							null,
							null,
							null,
							Integer.valueOf(FileState.RESERVED.getStateId()),
							f);

				}
				connection.commit();
				connection_pool.returnConnection(connection);
				connection = null;
			}
			else {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("transferFinished("+pnfsId+"): file state=" +f.getState() );
                                }
				connection.commit();
				connection_pool.returnConnection(connection);
				connection = null;
			}
		}
		catch(SQLException sqle) {
			logger.error("transferFinished failed with "+sqle.getMessage());
			if (connection!=null) {
				try {
					connection.rollback();
				}
			catch(SQLException sqle1) {}
			connection_pool.returnFailedConnection(connection);
			connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	private void  fileFlushed(PoolFileFlushedMessage fileFlushed) throws Exception {
		if (!spaceManagerEnabled) return;
		if(!returnFlushedSpaceToReservation) {
			return;
		}
		PnfsId pnfsId = fileFlushed.getPnfsId();
		//
		// if this file is not in srmspacefile table, silently quit
		//
		HashSet files = manager.selectPrepared(new FileIO(),
						       FileIO.SELECT_BY_PNFSID,
						       pnfsId.toString());
		if (files.isEmpty()==true) return;
                if (logger.isDebugEnabled()) {
                        logger.debug("fileFlushed("+pnfsId+")");
                }
		StorageInfo storageInfo = fileFlushed.getStorageInfo();
		AccessLatency ac = storageInfo.getAccessLatency();
		if ( ac != null && ac.equals(AccessLatency.ONLINE)) {
                        if (logger.isDebugEnabled()) {
                                logger.debug("File Access latency is ONLINE fileFlushed does nothing");
                        }
			return;
		}
		long size               = storageInfo.getFileSize();
                Connection connection   = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			File f = selectFileForUpdate(connection,pnfsId);
			if(f == null) {
				connection.rollback();
				connection_pool.returnConnection(connection);
				connection = null;
                                if (logger.isDebugEnabled()) {
                                        logger.debug( "fileFlushed("+pnfsId+"): file not in a reservation, do nothing");
                                }
				return;
			}
			long spaceId = f.getSpaceId();
			if(f.getState() == FileState.STORED) {
				if(deleteStoredFileRecord) {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("returnSpaceToReservation, deleting file record");
                                        }
					removeFileFromSpace(connection,f);
				}
				else {
					updateSpaceFile(connection,
							f.getId(),
							null,
							null,
							null,
							Long.valueOf(size),
							null,
							Integer.valueOf(FileState.FLUSHED.getStateId()),
							f);
					connection.commit();
					connection_pool.returnConnection(connection);
					connection = null;
				}
			}
			else {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("returnSpaceToReservation("+pnfsId+"): file state=" +f.getState() );
                                }
				connection.commit();
				connection_pool.returnConnection(connection);
				connection = null;
			}

		}
		catch(SQLException sqle) {
			logger.error("returnSpaceToReservation failed with "+sqle.getMessage());
			if (connection!=null) {
				try {
					connection.rollback();
				}
				catch(SQLException sqle1) {}
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	private void  fileRemoved(PoolRemoveFilesMessage fileRemoved) throws Exception {
		if ( !spaceManagerEnabled) return;
                if (logger.isDebugEnabled()) {
                        logger.debug("fileRemoved()");
                }
		String[] pnfsIdStrings = fileRemoved.getFiles();
		if(pnfsIdStrings == null || pnfsIdStrings.length == 0) {
			return;
		}
		for(String pnfsIdString : pnfsIdStrings ) {
			PnfsId pnfsId ;
			try {
				pnfsId = new PnfsId(pnfsIdString);
			}
			catch(Exception e) {
				logger.error(e.getMessage());
				continue;
			}
                        if (logger.isDebugEnabled()) {
                                logger.debug("fileRemoved("+pnfsId+")");
                        }
			if(!returnRemovedSpaceToReservation) return;
			Connection connection = null;
			try {
				connection = connection_pool.getConnection();
				connection.setAutoCommit(false);
				File f = selectFileForUpdate(connection,pnfsId);
				if(f == null) {
					connection.rollback();
					connection_pool.returnConnection(connection);
					connection = null;
                                        if (logger.isDebugEnabled()) {
                                                logger.debug( "fileRemoved("+pnfsId+"): file not in a reservation, do nothing");
                                        }
					return;
				}
				removeFileFromSpace(connection,f);
				connection.commit();
				connection_pool.returnConnection(connection);
				connection = null;
			}
			catch(SQLException sqle) {
                                if (logger.isDebugEnabled()) {
                                        logger.debug(sqle.getMessage());
                                        logger.debug( "fileRemoved("+pnfsId+"): file not in a reservation, do nothing");
                                }
				if (connection!=null) {
					try {
						connection.rollback();
					}
					catch(SQLException sqle1) {}
					connection_pool.returnFailedConnection(connection);
					connection = null;
				}

			}
			finally {
				if(connection != null) {
					connection_pool.returnConnection(connection);
				}
			}
		}
	}

	private void cancelUseSpace(CancelUse cancelUse)
		throws SQLException,SpaceException {
		if(!spaceManagerEnabled) {
			throw new SpaceException("SpaceManager is disabled in configuration");
		}
                if (logger.isDebugEnabled()) {
                        logger.debug("cancelUseSpace("+cancelUse+")");
                }
		long reservationId = cancelUse.getSpaceToken();
		String pnfsPath    = cancelUse.getPnfsName();
		PnfsId pnfsId      = cancelUse.getPnfsId();
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
                        File f = null;
                        try {
                                f=selectFileFromSpaceForUpdate(connection,pnfsPath,reservationId);
                        }
                        catch(SQLException sqle) {
                                //
                                // this is not an error: we are here in two cases
                                //   1) no transient file found - OK
                                //   2) more than one transient file found, less OK, but
                                //      remaining transient files will be garbage colllected after timeout
                                //
                                if(connection != null) {
                                        connection_pool.returnConnection(connection);
                                        connection = null;
                                }
                                return;
                        }
                        if(f.getState() == FileState.RESERVED ||
                           f.getState() == FileState.TRANSFERRING) {
                                try {
                                        removeFileFromSpace(connection,f);
                                        connection_pool.returnConnection(connection);
                                        connection = null;
                                }
                                finally {
                                        if (connection!=null) {
                                                logger.warn("Failed to remove file "+pnfsPath);
                                                connection_pool.returnFailedConnection(connection);
                                                connection = null;
                                        }
                                }
                        }
                }
		finally {
			if(connection != null) {
				connection_pool.returnFailedConnection(connection);
                                connection = null;
			}
		}
        }

	private long reserveSpace(
		String voGroup,
		String voRole,
		long sizeInBytes,
		AccessLatency latency ,
		RetentionPolicy policy,
		long lifetime,
		String description)
		throws SQLException,
		java.io.IOException,
                SpaceException
    {
            if (logger.isDebugEnabled()) {
                    logger.debug("reserveSpace(group="+voGroup+", role="+voRole+", sz="+sizeInBytes+
                                 ", latency="+latency+", policy="+policy+", lifetime="+lifetime+
                                 ", description="+description);
            }
            boolean needHsmBackup = policy.equals(RetentionPolicy.CUSTODIAL);
            if (logger.isDebugEnabled()) {
                    logger.debug("policy is "+policy+", needHsmBackup is "+needHsmBackup);
            }
            Long[] linkGroups = findLinkGroupIds(sizeInBytes,
                                                 voGroup,
                                                 voRole,
                                                 latency,
                                                 policy);
            if(linkGroups.length == 0) {
                logger.warn("find LinkGroup Ids returned 0 linkGroups, no linkGroups found");
                throw new NoFreeSpaceException(" no space available");
            }
            Long linkGroupId = linkGroups[0];
            return reserveSpaceInLinkGroup(
                                           linkGroupId.longValue(),
                                           voGroup,
                                           voRole,
                                           sizeInBytes,
                                           latency,
                                           policy,
                                           lifetime,
                                           description);
    }

	private long reserveSpace(
		AuthorizationRecord authRecord,
		long sizeInBytes,
		AccessLatency latency ,
		RetentionPolicy policy,
		long lifetime,
		String description)
		throws SQLException,
		java.io.IOException,
                SpaceException
    {
            if (logger.isDebugEnabled()) {
                    logger.debug("reserveSpace( ar="+authRecord+", sz="+sizeInBytes+
                                 ", latency="+latency+", policy="+policy+", lifetime="+lifetime+
                                 ", description="+description);
            }
            boolean needHsmBackup = policy.equals(RetentionPolicy.CUSTODIAL);
            if (logger.isDebugEnabled()) {
                    logger.debug("policy is "+policy+", needHsmBackup is "+needHsmBackup);
            }
            Set<LinkGroup> linkGroups = findLinkGroupIds(sizeInBytes,
                                                 latency,
                                                 policy);
            if(linkGroups.isEmpty()) {
                logger.warn("find LinkGroups returned 0 linkGroups, no linkGroups found");
                throw new NoFreeSpaceException(" no space available");
            }
            SpaceAuthorizationException sae = null;
            for(LinkGroup linkGroup:linkGroups) {
                 try {
                     VOInfo voInfo =
                             checkReservePermission(
                             authRecord, linkGroup);
                        return reserveSpaceInLinkGroup(
                                                       linkGroup.getId(),
                                                       voInfo.getVoGroup(),
                                                       voInfo.getVoRole(),
                                                       sizeInBytes,
                                                       latency,
                                                       policy,
                                                       lifetime,
                                                       description);
                    } catch(SpaceAuthorizationException sae1) {
                        sae = sae1;
                    }
			}

            throw sae;
    }

	private long reserveSpaceInLinkGroup(
		long linkGroupId,
		String voGroup,
		String voRole,
		long sizeInBytes,
		AccessLatency latency ,
		RetentionPolicy policy,
		long lifetime,
		String description)
		throws SQLException,
		java.io.IOException,
		SpaceException {
                if (logger.isDebugEnabled()) {
                        logger.debug("reserveSpaceInLinkGroup(linkGroupId="+linkGroupId+
                                     "group="+voGroup+", role="+voRole+", sz="+sizeInBytes+
                                     ", latency="+latency+", policy="+policy+", lifetime="+lifetime+
                                     ", description="+description);
                }
		Connection connection =null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			long spaceReservationId = getNextToken(connection);
			insertSpaceReservation(
				connection,
				spaceReservationId,
				voGroup,
				voRole,
				policy,
				latency,
				linkGroupId,
				sizeInBytes,
				lifetime,
				description,
				0,
				0,
				0);
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;
			return spaceReservationId;
		}
		catch(SQLException sqle) {
                        logger.error("failed to reserve space "+sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}


	private long useSpace(
		long reservationId,
		String voGroup,
		String voRole,
		long sizeInBytes,
		long lifetime,
		String pnfsPath,
		PnfsId pnfsId) throws SQLException,SpaceException {
		Connection connection =null;
		pnfsPath =new FsPath(pnfsPath).toString();
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			long fileId = getNextToken(connection);
			insertFileInSpace(
				connection,
				fileId,
				voGroup,
				voRole,
				reservationId,
				sizeInBytes,
				lifetime,
				pnfsPath,
				pnfsId,
				SpaceState.RESERVED.getStateId());
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;
			return fileId;
		}
		catch(SQLException sqle) {
			logger.error("useSpace(): insertFileInSpace failed with "+sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		catch (SpaceException e) {
			logger.error("useSpace(): insertFileInSpace failed with "+e.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw e;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	public void selectPool(CellMessage cellMessage, boolean isReply ) throws Exception{
		PoolMgrSelectPoolMsg selectPool = (PoolMgrSelectPoolMsg)cellMessage.getMessageObject();
		if(!spaceManagerEnabled ) {
			if(!isReply) {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("just forwarding the message to "+ poolManager);
                                }
				cellMessage.getDestinationPath().add( new CellPath(poolManager) ) ;
				cellMessage.nextDestination() ;
				sendMessage(cellMessage) ;
			}
			return;
		}
                if (logger.isDebugEnabled()) {
                        logger.debug("selectPool("+selectPool +")");
                }
		String pnfsPath = selectPool.getPnfsPath();
		PnfsId pnfsId = selectPool.getPnfsId();
		if( !(selectPool instanceof PoolMgrSelectWritePoolMsg)||pnfsPath == null) {
                        if (logger.isDebugEnabled()) {
                                logger.debug("selectPool: pnfsPath is null");
                        }
			if(!isReply) {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("just forwarding the message to "+ poolManager);
                                }
				cellMessage.getDestinationPath().add( new CellPath(poolManager) ) ;
				cellMessage.nextDestination() ;
				sendMessage(cellMessage) ;
			}
			return;
		}
		File file = null;
		try {
                        if (logger.isDebugEnabled()) {
                                logger.debug("selectPool: getFiles("+pnfsPath+")");
                        }
                        Set<File> files = getFiles(pnfsPath);
                        for (File f: files) {
                                if (f.getPnfsId()==null) {
                                        file=f;
                                        break;
                                }
                        }
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
		if(file==null) {
                        StorageInfo storageInfo = selectPool.getStorageInfo();
                        AccessLatency al = null;
                        RetentionPolicy rp = null;
                        String defaultSpaceToken=null;
                        al  = storageInfo.getAccessLatency();
                        rp  = storageInfo.getRetentionPolicy();
                        defaultSpaceToken=storageInfo.getMap().get("writeToken");
                        ProtocolInfo protocolInfo = selectPool.getProtocolInfo();
                        VOInfo voinfo = null;
                        if(protocolInfo instanceof GridProtocolInfo) {
                                voinfo = ((GridProtocolInfo)protocolInfo).getVOInfo();
                                if (logger.isDebugEnabled()) {
                                        logger.debug("protocol info is GridProtocolInfo voinfo="+voinfo);
                                }
                        }
                        if (defaultSpaceToken==null) {
                                if(reserveSpaceForNonSRMTransfers) {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("selectPool: file is not found, no prior reservations for this file, calling reserveAndUseSpace()");
                                        }
                                        file = reserveAndUseSpace(pnfsPath,
                                                                  null,
                                                                  selectPool.getFileSize(),
                                                                  al,
                                                                  rp,
                                                                  voinfo);
                                }
                                else {
                                        if (logger.isDebugEnabled()) {
                                                logger.debug("selectPool: file is not found, no prior reservations for this file");
                                        }
                                        if(!isReply) {
                                                if (logger.isDebugEnabled()) {
                                                        logger.debug("just forwarding the message to "+ poolManager);
                                                }
                                                cellMessage.getDestinationPath().add( new CellPath(poolManager) ) ;
                                                cellMessage.nextDestination() ;
                                                sendMessage(cellMessage) ;
                                        }
                                        return;
                                }
                        }
                        else {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("selectPool: file is not found, found default space token, calling useSpace()");
                                }
                                String voGroup   = null;
                                String voRole    = null;
                                long lifetime    = 1000*60*60;
                                if(voinfo != null){
                                        voGroup = voinfo.getVoGroup();
                                        voRole = voinfo.getVoRole();
                                }
                                long spaceToken = Long.parseLong(defaultSpaceToken);
                                long fileId     = useSpace(spaceToken,
                                                           voGroup,
                                                           voRole,
                                                           selectPool.getFileSize(),
                                                           lifetime,
                                                           pnfsPath,
                                                           selectPool.getPnfsId());
                                file = getFile(fileId);
                        }
                }
		else {
                        if (isReply&&selectPool.getReturnCode()==0) {
                                if (logger.isDebugEnabled()) {
                                        logger.debug("selectPool: file is not null, calling updateSpaceFile()");
                                }
                                updateSpaceFile(file.getId(),null,null,pnfsId,null,null,null);
                        }
		}
                if (isReply&&selectPool.getReturnCode()!=0) {
                        Connection connection = null;
                        try {
                                connection = connection_pool.getConnection();
                                connection.setAutoCommit(false);
                                removePnfsIdOfFileInSpace(connection,file.getId(),null);
                                connection.commit();
                                connection_pool.returnConnection(connection);
                                connection = null;
                        }
                        catch(SQLException sqle) {
                                logger.error(sqle.getMessage());
                                if (connection!=null) {
                                        try {
                                                connection.rollback();
                                        }
                                        catch (SQLException e) {}
                                        connection_pool.returnFailedConnection(connection);
                                        connection = null;
                                }
                        }
                        finally {
                                if(connection != null) {
                                        connection_pool.returnConnection(connection);
                                }
                        }
                }
		if(!isReply) {
                        long spaceId     = file.getSpaceId();
                        Space space      = getSpace(spaceId);
                        long linkGroupid = space.getLinkGroupId();
                        LinkGroup linkGroup  = getLinkGroup(linkGroupid);
                        String linkGroupName = linkGroup.getName();
                        selectPool.setLinkGroup(linkGroupName);
                        StorageInfo storageInfo = selectPool.getStorageInfo();
                        storageInfo.setKey("SpaceToken",Long.toString(spaceId));
                        //
                        // add Space Token description
                        //
                        if (space.getDescription()!=null) {
                                storageInfo.setKey("SpaceTokenDescription",space.getDescription());
                        }
			cellMessage.getDestinationPath().add( new CellPath(poolManager) ) ;
			cellMessage.nextDestination() ;
                        if (logger.isDebugEnabled()) {
                                logger.debug("selectPool: found linkGroup = "+linkGroupName+", forwarding message");
                        }
			sendMessage(cellMessage) ;
		}
		return;
	}

	public void markFileDeleted(PnfsDeleteEntryNotificationMessage msg) throws Exception {
		if (msg.getReturnCode()!=0) return;
		if( msg.getPnfsId() == null ) {
                        return;
		}
		File file=null;
		try {
			HashSet files = manager.selectPrepared(new FileIO(),
							       FileIO.SELECT_BY_PNFSID,
							       msg.getPnfsId().toString());
			if (files.isEmpty()) return;
			if (files.size()>1) {
				throw new SQLException("found two records with pnfsId="+(msg.getPnfsId()!=null?msg.getPnfsId():"null"));
			}
			file=(File)files.toArray()[0];
		}
		catch (Exception e) {
			logger.error("Failed to retrieve file by pnfs id "+ (msg.getPnfsId()!=null?msg.getPnfsId():"null")+" "
			     +(msg.getPnfsPath()!=null?msg.getPnfsPath():"null"));
                        if (logger.isDebugEnabled()) {
                                logger.debug(e.getMessage(),e);
                        }
			return;
		}
                if (logger.isDebugEnabled()) {
                        logger.debug("Marking file as deleted "+file);
                }
		Connection connection = null;
		int rc = 0;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			File f = selectFileForUpdate(connection,file.getId());
			rc = manager.update(connection,
					    FileIO.UPDATE_DELETED_FLAG,
					    1,
					    f.getId());
			if (rc!=1) {
				throw new SQLException("Update failed, row count="+rc);
			}
			connection.commit();
			connection_pool.returnConnection(connection);
			connection=null;
		}
		catch (SQLException e) {
			logger.error(e.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection=null;
			}
			throw e;
		}
	}

	public String getPnfsManager() {
		return pnfsManager;
	}

	public String getPoolManager() {
		return poolManager;
	}

	public void getSpaceMetaData(GetSpaceMetaData gsmd) throws Exception{
		if(!spaceManagerEnabled) {
			throw new SpaceException("SpaceManager is disabled in configuration");
		}
		long[] tokens = gsmd.getSpaceTokens();
		if(tokens == null) {
			throw new IllegalArgumentException("null space tokens");
		}
		Space[] spaces = new Space[tokens.length];
		for(int i=0;i<spaces.length; ++i){
			try {
				spaces[i] = getSpace(tokens[i]);
			}
			catch(Exception e) {
				logger.error(e.getMessage());
				spaces[i]= null;
			}
		}
		gsmd.setSpaces(spaces);
	}

	public void getSpaceTokens(GetSpaceTokens gst) throws Exception{
		if(!spaceManagerEnabled) {
			throw new SpaceException("SpaceManager is disabled in configuration");
		}
		String description = gst.getDescription();

		long [] tokens = getSpaceTokens(gst.getAuthRecord(), description);
		gst.setSpaceToken(tokens);
	}

	public void getFileSpaceTokens(GetFileSpaceTokensMessage getFileTokens) throws Exception{
		if(!spaceManagerEnabled) {
			throw new SpaceException("SpaceManager is disabled in configuration");
		}
		PnfsId pnfsId = getFileTokens.getPnfsId();
		String pnfsPath = getFileTokens.getPnfsPath();
		if(pnfsId == null && pnfsPath == null) {
			throw new IllegalArgumentException("null voGroup");
		}
		long [] tokens = getFileSpaceTokens(pnfsId,pnfsPath);
		getFileTokens.setSpaceToken(tokens);
	}

	public void extendLifetime(ExtendLifetime extendLifetime) throws Exception{
		if(!spaceManagerEnabled) {
			throw new SpaceException("SpaceManager is disabled in configuration");
		}
		long token            = extendLifetime.getSpaceToken();
		long newLifetime      = extendLifetime.getNewLifetime();
		Connection connection = null;
		try {
			connection = connection_pool.getConnection();
			connection.setAutoCommit(false);
			Space space = selectSpaceForUpdate(connection,token);
			if(SpaceState.isFinalState(space.getState())) {
				connection.rollback();
				connection_pool.returnConnection(connection);
				connection = null;
				throw new Exception("Space Is already Released");
			}
			long creationTime = space.getCreationTime();
			long lifetime = space.getLifetime();
			if(lifetime == -1) {
				connection.rollback();
				connection_pool.returnConnection(connection);
				connection = null;
				return;
			}
			if(newLifetime == -1) {
				manager.update(connection,
					       SpaceReservationIO.UPDATE_LIFETIME,
					       newLifetime,
					       token);
				connection.commit();
				connection_pool.returnConnection(connection);
				connection = null;
				return;
			}
			long currentTime = System.currentTimeMillis();
			long remainingLifetime = creationTime + lifetime - currentTime;
			if(remainingLifetime > newLifetime) {
				connection.rollback();
				connection_pool.returnConnection(connection);
				connection = null;
				return;
			}
			long newLifetimeSinceCreation = currentTime - creationTime + newLifetime;
			manager.update(connection,
				       SpaceReservationIO.UPDATE_LIFETIME,
				       newLifetime,
				       token);
			connection.commit();
			connection_pool.returnConnection(connection);
			connection = null;

		}
		catch(SQLException sqle) {
			logger.error("Failed to extend lifetime "+sqle.getMessage());
			if (connection!=null) {
				connection.rollback();
				connection_pool.returnFailedConnection(connection);
				connection = null;
			}
			throw sqle;
		}
		finally {
			if(connection != null) {
				connection_pool.returnConnection(connection);
			}
		}
	}

	//
	// the crap below shoulda've been replaced with database triggers (litvinse@fnal.gov)
	//

	public void decrementReservedSpaceInLinkGroup(Connection connection,
						      long id,
						      long size) throws SQLException {
		manager.update(connection,LinkGroupIO.DECREMENT_RESERVED_SPACE,size,id);
	}

	public void incrementReservedSpaceInLinkGroup(Connection connection,
						      long id,
						      long size) throws SQLException {
		manager.update(connection,LinkGroupIO.INCREMENT_RESERVED_SPACE,size,id);
	}

	public void decrementFreeSpaceInLinkGroup(Connection connection,
						  long id,
						  long size) throws SQLException {
		manager.update(connection,LinkGroupIO.DECREMENT_FREE_SPACE,size,id);
	}

	public void incrementFreeSpaceInLinkGroup(Connection connection,
						  long id,
						  long size) throws SQLException {
		manager.update(connection,LinkGroupIO.INCREMENT_FREE_SPACE,size,id);
	}


	//
	// Very important: we DO NOT touch reservedspaceinbytes of srmlinkgroup table
        // when we change allocatedspaceinbytes in srmspace table (litvinse@fnal.gov)
	//

	public void decrementAllocatedSpaceInSpaceReservation(Connection connection,
							      Space space,
							      long size) throws SQLException {
		manager.update(connection,SpaceReservationIO.DECREMENT_ALLOCATED_SPACE,size,space.getId());
	}

	public void incrementAllocatedSpaceInSpaceReservation(Connection connection,
							      Space space,
							      long size) throws SQLException {
		manager.update(connection,SpaceReservationIO.INCREMENT_ALLOCATED_SPACE,size,space.getId());
	}

	//
	// Very important: we DO  touch reservedspaceinbytes of srmlinkgroup table
        // when we change usedspaceinbytes in srmspace table (litvinse@fnal.gov)
	//

	//
        // when the space becomes "unused" we "return" it to "reservedspaceinbytes" space in linkgroup
	//
	public void decrementUsedSpaceInSpaceReservation(Connection connection,
							 Space space,
							 long size) throws SQLException {
		LinkGroup group = selectLinkGroupForUpdate(connection, space.getLinkGroupId());
		manager.update(connection,SpaceReservationIO.DECREMENT_USED_SPACE,size,space.getId());
		if (space.getState() == SpaceState.RESERVED) {
			incrementReservedSpaceInLinkGroup(connection,group.getId(),size);
		}
	}

	//
	// when the space becomes "used" we subtract it from "reservedspaceinbytes" space in linkgroup
	//
	public void incrementUsedSpaceInSpaceReservation(Connection connection,
							 Space space,
							 long size) throws SQLException {
		LinkGroup group = selectLinkGroupForUpdate(connection, space.getLinkGroupId());
		manager.update(connection,SpaceReservationIO.INCREMENT_USED_SPACE,size,space.getId());
		if (space.getState() == SpaceState.RESERVED) {
			decrementReservedSpaceInLinkGroup(connection,group.getId(),size);
		}
	}

    public void checkReleasePermission(AuthorizationRecord authRecord, Space space)
        throws SpaceAuthorizationException {
        String spaceGroup = space.getVoGroup();
        String spaceRole = space.getVoRole();

        if((spaceGroup == null || spaceGroup.equals(authRecord.getVoGroup())) &&
            (spaceRole == null || spaceRole.equals(authRecord.getVoRole()))) {
            logger.debug("userGroup : "+authRecord.getVoGroup()+", userRole : "+
                    authRecord.getVoRole()+ " have permission to release ");
            return;
        }

        for(GroupList groupList: authRecord.getGroupLists()) {
            FQAN voAttribute = new FQAN(groupList.getAttribute());
            String userGroup = voAttribute.getGroup();
            String userRole = voAttribute.getRole();
            if((spaceGroup == null || spaceGroup.equals(userGroup)) &&
                (spaceRole == null || spaceRole.equals(userRole))) {
                logger.debug("userGroup : "+userGroup+", userRole : "+userRole+
                        " have permission to release ");
                return;
            }
        }

        throw new SpaceAuthorizationException("user with "+authRecord+
                " has no permission to release "+space);
    }

    public VOInfo checkReservePermission(AuthorizationRecord authRecord, LinkGroup linkGroup)
        throws SpaceAuthorizationException {
        VOInfo[] voInfos = linkGroup.getVOs();
        for(VOInfo voInfo:voInfos) {
            String lgGroup=voInfo.getVoGroup();
            String lgRole=voInfo.getVoRole();
            if( (lgGroup.equals("*") ||lgGroup.equals(authRecord.getVoGroup()))  &&
                (lgRole.equals("*") ||lgRole.equals(authRecord.getVoRole()))) {
                String userGroup = authRecord.getVoGroup();
                String userRole = authRecord.getVoRole();
                logger.debug("userGroup : "+userGroup+", userRole : "+userRole+
                        " have permission to reserve ");
                return new VOInfo(userGroup,userRole );
            }

            for(GroupList groupList: authRecord.getGroupLists()) {
                FQAN voAttribute = new FQAN(groupList.getAttribute());
                String userGroup = voAttribute.getGroup();
                String userRole = voAttribute.getRole();
                if((lgGroup.equals("*") ||lgGroup.equals(userGroup))  &&
                   (lgRole.equals("*")  ||lgRole.equals(userRole))) {
                    logger.debug("userGroup : "+userGroup+", userRole : "+userRole+
                            " have permission to reserve ");
                    return new VOInfo(userGroup,userRole );
                }
            }
        }
        throw new SpaceAuthorizationException("user with "+authRecord+
                " has no permission to reserve in "+linkGroup);

    }

}
