// $Id: AuthorizationService.java,v 1.11.4.10 2007-10-10 18:01:55 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.11.4.9  2007/02/28 23:10:02  tdh
// Added code for user-specified mapping for dcache.kpwd method.
//
// Revision 1.11.4.8  2006/11/30 21:49:57  tdh
// Backport of rationalization of logging.
//
// Revision 1.19  2006/11/29 19:11:35  tdh
// Added ability to set log level and propagate to plugins.
//
// Revision 1.18  2006/11/28 21:14:55  tdh
// Check that caller is not null.
//
// Revision 1.17  2006/11/28 21:13:20  tdh
// Set log4j log level in plugins according to printout level of calling cell.
//
// Revision 1.16  2006/11/13 16:33:41  tigran
// fixed CLOSE_WAIT:
// delegated socked was no closed
//
// Revision 1.15  2006/09/07 20:12:52  tdh
// Propagate fix of Revision 1.11.4.4 to development branch.
//
// Revision 1.14  2006/08/23 16:40:05  tdh
// Propagate authorization request ID to plugins so log entries can be tagged with it.
//
// Revision 1.13  2006/08/07 16:38:03  tdh
// Merger of changes from branch, exception handling and ignore blank config file lines.
//
// Revision 1.11.4.3  2006/08/07 16:23:41  tdh
// Added exception handling lines to other authorization methods.
//
// Revision 1.11.4.2  2006/08/07 15:55:23  tdh
// Checks that authorization is null before throwing AuthorizationServiceException.
//
// Revision 1.11.4.1  2006/07/26 18:41:59  tdh
// Backport of recent changes to development branch.
//
// Revision 1.12  2006/07/25 14:58:45  tdh
// Merged DN/Role authentication. Added logging and authRequestID code.
//
// Revision 1.11.2.2  2006/07/12 19:45:57  tdh
// Uncommented GPLAZMALiteVORoleAuthzPlugin and added CVS line.
//

package diskCacheV111.services.authorization;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import diskCacheV111.util.*;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.DNInfo;
import diskCacheV111.vehicles.AuthenticationMessage;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.auth.GSSAuthorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.gsi.GlobusCredential;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.srm.SRMAuthorizationException;
import org.gridforum.jgss.ExtendedGSSManager;

import dmg.cells.nucleus.*;

/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public class AuthorizationService {

	public static final String GPLAZMA_SRMDCACHE_RELEASE_VERSION="0.1-1";
	private List l = new LinkedList();
	AuthorizationServicePlugin kPlug;
	AuthorizationServicePlugin VOPlug;
	AuthorizationServicePlugin gPlug;
	AuthorizationServicePlugin liteVORolePlug;
	private String authConfigFilePath;
  private long authRequestID;
  CellAdapter caller;
  private String loglevel=null;
  private AuthorizationConfig authConfig;
	private Vector pluginPriorityConfig;
  private DateFormat _df   = new SimpleDateFormat("MM/dd HH:mm:ss" ) ;

  public AuthorizationService()
	throws AuthorizationServiceException {
    this(null, Math.abs(new Random().nextInt()), null);
	}

	public AuthorizationService(String authservConfigFilePath)
	throws AuthorizationServiceException {
    this(authservConfigFilePath, Math.abs(new Random().nextInt()), null);
	}

	public AuthorizationService(long authRequestID)
	throws AuthorizationServiceException {
    this(null, authRequestID, null);
	}

  public AuthorizationService(CellAdapter caller)
  throws AuthorizationServiceException {
    this(null, Math.abs(new Random().nextInt()), caller);
  }

	public AuthorizationService(String authservConfigFilePath, long authRequestID)
	throws AuthorizationServiceException {
    this(authservConfigFilePath, authRequestID, null);
	}

  public AuthorizationService(String authservConfigFilePath, CellAdapter caller)
	throws AuthorizationServiceException {
    this(authservConfigFilePath, Math.abs(new Random().nextInt()), caller);
	}

  public AuthorizationService(String authservConfigFilePath, long authRequestID, CellAdapter caller)
	throws AuthorizationServiceException {
		this.authConfigFilePath = authservConfigFilePath;
    this.authRequestID=authRequestID;
    this.caller = caller;
	}

  public long getAuthRequestID() {
    return authRequestID;
  }

  private void say(String s) {
    if(caller!=null) {
      caller.say("authRequestID " + authRequestID + " " + s);
    } else {
      System.out.println(_df.format(new Date()) + " authRequestID " + authRequestID + " " + s);
    }
  }

  private void esay(String s) {
    if(caller!=null) {
      caller.esay("authRequestID " + authRequestID + " " + s);
    } else {
      System.err.println(_df.format(new Date()) + " authRequestID " + authRequestID + " " + s);
    }
  }

	private void addPlugin(AuthorizationServicePlugin plugin)
	throws AuthorizationServiceException {
		try {
			if(plugin == null) {
				esay("Plugin " +plugin.toString()+ " is null and cannot be added.");
			}
			else {
        if(loglevel==null) {
          if(caller!=null && (caller.getNucleus().getPrintoutLevel() & CellNucleus.PRINT_CELL ) > 0 )
            plugin.setLogLevel("INFO");
          else
            plugin.setLogLevel("ERROR");
        } else {
          plugin.setLogLevel(loglevel);
        }
        l.add(plugin);
			}
		}
		catch (Exception e ) {
			throw new AuthorizationServiceException("authRequestID " + authRequestID + " Exception adding Plugin: " +e);
		}
	} 

	private void buildDCacheAuthzPolicy()
	throws AuthorizationServiceException {

		String kpwdPath;
		String VOMapUrl;
		String gridmapfilePath;
		String storageAuthzDbPath;
		String gPLAZMALiteVORoleMapPath;
		String gPLAZMALiteStorageAuthzDbPath;

		try {
			authConfig = new AuthorizationConfig(authConfigFilePath);
		}
		catch(java.io.IOException ioe) {
			esay("Exception in AuthorizationConfig instantiation :" + ioe);
			throw new AuthorizationServiceException (ioe.toString());
		}
		try {
			pluginPriorityConfig = authConfig.getpluginPriorityConfig();
			ListIterator iter = pluginPriorityConfig.listIterator(0);
			while (iter.hasNext()) {
				String thisSignal = (String)iter.next();
				if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getKpwdSignal())) ) {
					try {
						try {
						 	kpwdPath = authConfig.getKpwdPath();
						}
						catch(Exception e) {
							esay("Exception getting Kpwd Path from configuration :" +e);
							throw new AuthorizationServiceException (e.toString());
						}	
						if (kpwdPath != null && !kpwdPath.equals("")) {
							AuthorizationServicePlugin kPlug = new KPWDAuthorizationPlugin(kpwdPath, authRequestID);
							addPlugin(kPlug);
						}
						else {
							esay("Kpwd Path not well-formed in configuration.");
						}
					}
					catch (AuthorizationServiceException ae) {
						esay("Exception: " +ae);
					}	
				}//end of kpwd-if
				else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getVOMappingSignal())) ) {
					try {
						try {
							VOMapUrl = authConfig.getMappingServiceUrl();
						}
						catch(Exception e) {
							esay("Exception getting VO Map Url from configuration : " +e);
							throw new AuthorizationServiceException (e.toString());
						}
						if (VOMapUrl != null && !VOMapUrl.equals("")) {
							AuthorizationServicePlugin VOPlug = new VOAuthorizationPlugin(VOMapUrl, authRequestID);
                            ((VOAuthorizationPlugin) VOPlug).setCacheLifetime(authConfig.getMappingServiceCacheLifetime());
                            addPlugin(VOPlug);
						}
						else {
							esay("VO Map Url not well-formed in configuration.");
						}
					}
					catch (AuthorizationServiceException ae) {
						esay("Exception : " +ae);
					}	
				}//end of saml-based-vo-mapping-if
				else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getGPLiteVORoleMappingSignal())) ) {
					try {
						try {
							gPLAZMALiteVORoleMapPath = authConfig.getGridVORoleMapPath();
							gPLAZMALiteStorageAuthzDbPath = authConfig.getGridVORoleStorageAuthzPath();
						}
						catch(Exception e) {
							esay("Exception getting Grid VO Role Map or Storage Authzdb paths from configuration :" +e);
							throw new AuthorizationServiceException (e.toString());
						}
						if (gPLAZMALiteVORoleMapPath != null && gPLAZMALiteStorageAuthzDbPath != null && 
						!gPLAZMALiteVORoleMapPath.equals("") && !gPLAZMALiteStorageAuthzDbPath.equals("")) {
							AuthorizationServicePlugin liteVORolePlug = new GPLAZMALiteVORoleAuthzPlugin(gPLAZMALiteVORoleMapPath, gPLAZMALiteStorageAuthzDbPath, authRequestID);
							addPlugin(liteVORolePlug);
						}
						else {
							esay("Grid VO Role Map or Storage Authzdb paths not well-formed in configuration");
						}
					}
					catch (AuthorizationServiceException ae) {
						esay("Exception : " +ae);
					}	
				}//end of gplazmalite-vorole-mapping-if
				else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getGridMapFileSignal())) ) {
					try {
						try {
							gridmapfilePath = authConfig.getGridMapFilePath();
							storageAuthzDbPath = authConfig.getStorageAuthzPath();
						}
						catch(Exception e) {
							esay("Exception getting GridMap or Storage Authzdb path from configuration :" +e);
							throw new AuthorizationServiceException (e.toString());
						}
						if (gridmapfilePath != null && storageAuthzDbPath != null &&
						!gridmapfilePath.equals("") && !storageAuthzDbPath.equals("")) {
							AuthorizationServicePlugin gPlug  = new GridMapFileAuthzPlugin(gridmapfilePath, storageAuthzDbPath, authRequestID);
							addPlugin(gPlug);
						}
						else {
							esay("GridMap or Storage Authzdb paths not well-formed in configuration");
						}
					}
					catch (AuthorizationServiceException ae) {
						esay("Exception : " +ae);
					}	
				}//end of gplazmalite-gridmapfile-if
			}//end of while
		}
		catch(Exception cpe) {
			esay("Exception processing Choice|Priority Configuration :" + cpe);
			throw new AuthorizationServiceException (cpe.toString());
		}	

	}

	public UserAuthRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

		Iterator plugins = getPlugins();
    if (plugins==null) return null;

    UserAuthRecord r = null;

    AuthorizationServicePlugin p=null;
    AuthorizationServiceException authexceptions=null;
    int counter = 0;
		while (r==null && plugins.hasNext()) {
			try {
				p = (AuthorizationServicePlugin) plugins.next();
				r = p.authorize(context, desiredUserName, serviceUrl, socket);
        counter++;
			} catch(AuthorizationServiceException ae) {
				esay("AuthorizationService - Exception: " + ae);
        if(authexceptions==null)
        authexceptions = new  AuthorizationServiceException("\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
        else
        authexceptions = new  AuthorizationServiceException(authexceptions.getMessage() + "\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
			}
			//System.out.println("Plugins|services tried: " + counter);
		}
		//System.out.println("... Unloading...g P L A Z M A.....A u t h o r i z a t i o n.....S e r v i c e.....module ...");
    if(authexceptions!=null && r==null) throw authexceptions;
		return r;
		
	}

	public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

		Iterator plugins = getPlugins();
    if (plugins==null) return null;

    UserAuthRecord r = null;

    AuthorizationServicePlugin p=null;
    AuthorizationServiceException authexceptions=null;
    int counter = 0;
		while (r==null && plugins.hasNext()) {
			try {
				p = (AuthorizationServicePlugin) plugins.next();
				r = p.authorize(subjectDN, role, desiredUserName, serviceUrl, socket);
        counter++;
			} catch(AuthorizationServiceException ae) {
				esay("AuthorizationService - Exception: " + ae);
        if(authexceptions==null)
        authexceptions = new  AuthorizationServiceException("\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
        else
        authexceptions = new  AuthorizationServiceException(authexceptions.getMessage() + "\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
			}
		}
		if(authexceptions!=null && r==null) throw authexceptions;
		return r;

	}

	public UserAuthRecord authorize(String subjectDN, String role, GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

		Iterator plugins = getPlugins();
    if (plugins==null) return null;

    UserAuthRecord r = null;

    AuthorizationServicePlugin p=null;
    AuthorizationServiceException authexceptions=null;
    int counter = 0;
		while (r==null && plugins.hasNext()) {
			try {
				p = (AuthorizationServicePlugin) plugins.next();
				r = p.authorize(subjectDN, role, desiredUserName, serviceUrl, socket);
        if(r==null)
        r = p.authorize(context, desiredUserName, serviceUrl, socket);
        counter++;
			} catch(AuthorizationServiceException ae) {
				esay("AuthorizationService - Exception: " + ae);
        if(authexceptions==null)
        authexceptions = new  AuthorizationServiceException("\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
        else
        authexceptions = new  AuthorizationServiceException(authexceptions.getMessage() + "\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
			}
		}
		if(authexceptions!=null && r==null) throw authexceptions;
		return r;

	}

  public Iterator getPlugins() throws AuthorizationServiceException {

    try {
			buildDCacheAuthzPolicy();
			//System.out.println("- - -               Deactivating     g P L A Z M A    Policy   Loader                 - - - ");
		}
		catch(AuthorizationServiceException aue) {
			esay("Exception in building DCache Authz Policy: " + aue);
			throw new AuthorizationServiceException (aue.toString());
		}

		//System.out.println("Number of authorization mechanisms being used: " +maxm);
		if (l.size() == 0) {
			say("All Authorization OFF!  System Quasi-firewalled!");
			return null;
		}

		return l.listIterator(0);
  }

  // Called on requesting cel
  //
  public UserAuthBase authenticate(GSSContext serviceContext, String user, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {

        RemoteGsiftpTransferProtocolInfo protocolInfo =
          new RemoteGsiftpTransferProtocolInfo(
            "RemoteGsiftpTransfer",
            1,1,null,0,
            null,
            caller.getNucleus().getCellName(),
            caller.getNucleus().getCellDomainName(),
            authRequestID,
            0,
            0,
            0,
            new Long(0),
            user);
        //long authRequestID = protocolInfo.getId();

      return  authenticate(serviceContext, protocolInfo, cellpath, caller);
    }

    public UserAuthBase authenticate(GSSContext serviceContext, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {

        RemoteGsiftpTransferProtocolInfo protocolInfo =
          new RemoteGsiftpTransferProtocolInfo(
            "RemoteGsiftpTransfer",
            1,1,null,0,
            null,
            caller.getNucleus().getCellName(),
            caller.getNucleus().getCellDomainName(),
            authRequestID,
            0,
            0,
            0,
            new Long(0));
        //long authRequestID = protocolInfo.getId();

      return  authenticate(serviceContext, protocolInfo, cellpath, caller);
    }

    public UserAuthBase authenticate(GSSContext serviceContext, RemoteGsiftpTransferProtocolInfo protocolInfo, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {

      UserAuthBase PwdRecord = null;

      String authcellname = cellpath.getCellName();
        try {
          GSSName GSSIdentity = serviceContext.getSrcName();
          CellMessage m = new CellMessage(cellpath, protocolInfo);
          m = caller.getNucleus().sendAndWait(m, 40000L) ;
          if(m==null) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " Message to " + authcellname + " timed out for authentification of " + GSSIdentity);
          }


            Object obj = m.getMessageObject();
            if(obj instanceof RemoteGsiftpDelegateUserCredentialsMessage) {
              if(((Message) obj).getId()!=authRequestID) {
              throw new AuthorizationServiceException("authRequestID " + authRequestID + " delegation failed: mismatch with returned authRequestID " + ((Message) obj).getId());
              }
              GSIGssSocket authsock=null;

                try {

                  authsock = SslGsiSocketFactory.delegateCredential(
                InetAddress.getByName(((RemoteGsiftpDelegateUserCredentialsMessage) obj).getHost()),
                ((RemoteGsiftpDelegateUserCredentialsMessage) obj).getPort(),
                //ExtendedGSSManager.getInstance().createCredential(GSSCredential.INITIATE_ONLY),
                serviceContext.getDelegCred(),
                false);
              //say(this.toString() + " delegation appears to have succeeded");
              } catch ( UnknownHostException uhe ) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " unknown host exception in delegation " + uhe);
              } catch(Exception e) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " delegation failed for authentification of " + GSSIdentity + " " + e);
              }

            Object authobj=null;
            if(authsock!=null) {
              try {
                authsock.setSoTimeout(30000);
                InputStream authin = authsock.getInputStream();
                ObjectInputStream authstrm = new ObjectInputStream(authin);
                authobj = authstrm.readObject();
                if(authobj==null) {
                  throw new AuthorizationServiceException("authRequestID " + authRequestID + " authorization object was null for " + GSSIdentity);
                } else {
                  if( authobj instanceof Exception ) throw (Exception) authobj;
                  PwdRecord = (UserAuthBase) authobj;
                  caller.say("authRequestID " + authRequestID + " received " + PwdRecord.Username + " " + PwdRecord.UID + " " + PwdRecord.GID + " " + PwdRecord.Root);// + " for " + GSSIdentity);
                }
              } catch (IOException ioe) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " could not receive authorization object for " + GSSIdentity + " " + ioe);
              } catch (ClassCastException cce) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " incorrect class for authorization object for " + GSSIdentity + " " + cce);
              } finally {
            	  authsock.close();
              }
              
            } else {
              throw new AuthorizationServiceException("authRequestID " + authRequestID + " socket to receive authorization object was null for " + GSSIdentity);
            }

          } else {
            if( obj instanceof NoRouteToCellException )
            throw (NoRouteToCellException) obj;
            if( obj instanceof Throwable )
            throw (Throwable) obj;
          }
        } catch ( AuthorizationServiceException ase ) {
          throw ase;
        } catch ( GSSException gsse ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " error getting source from context " + gsse);
        } catch ( NotSerializableException nse ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " message to " + authcellname + " not serializable " + nse);
        } catch ( NoRouteToCellException nre ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + nre);
        } catch ( InterruptedException ie ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " message thread was interrupted " + ie);
        } catch ( ClassNotFoundException cnfe ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " class of object returned from " + authcellname + " not found " + cnfe);
        } catch ( Throwable t ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " recevied exception " + t.getMessage());
        }

      return PwdRecord;
    }

    public UserAuthBase authenticate(String subjectDN, String role, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {

        UserAuthBase PwdRecord = null;

        DNInfo dnInfo = new DNInfo(subjectDN, role, authRequestID);
        //long authRequestID = dnInfo.getId();

        String authcellname = cellpath.getCellName();
        try {
            CellMessage m = new CellMessage(cellpath, dnInfo);
            m = caller.getNucleus().sendAndWait(m, 40000L) ;
          if(m==null) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " Message to " + authcellname + " timed out for authentification of " + subjectDN + " and role " + role);
          }

          Object authobj = m.getMessageObject();
          if(authobj instanceof AuthenticationMessage) {
            if(((AuthenticationMessage) authobj).getAuthRequestID()!=authRequestID) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " delegation failed: mismatch with returned authRequestID " + ((Message) authobj).getId());
          }

            try {
              if(authobj==null) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " authorization object was null for " + subjectDN + " and role " + role);
              } else {
              if( authobj instanceof Exception ) throw (Exception) authobj;
              PwdRecord = ((AuthenticationMessage) authobj).getUserAuthBase();
              caller.say("authRequestID " + authRequestID + " received " + PwdRecord.Username + " " + PwdRecord.UID + " " + PwdRecord.GID + " " + PwdRecord.Root);// + " for " + GSSIdentity);
            }
            } catch (IOException ioe) {
              throw new AuthorizationServiceException("authRequestID " + authRequestID + " could not receive authorization object for " + subjectDN + " and role " + role + " " + ioe);
            } catch (ClassCastException cce) {
              throw new AuthorizationServiceException("authRequestID " + authRequestID + " incorrect class for authorization object for " + subjectDN + " and role " + role + " " + cce);
            }
          } else {
            if( authobj instanceof NoRouteToCellException )
            throw (NoRouteToCellException) authobj;
            if( authobj instanceof Throwable )
            throw (Throwable) authobj;
          }
        } catch ( AuthorizationServiceException ase ) {
          throw ase;
        } catch ( GSSException gsse ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " error getting source from context " + gsse);
        } catch ( NotSerializableException nse ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " message to " + authcellname + " not serializable " + nse);
        } catch ( NoRouteToCellException nre ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + nre);
        } catch ( InterruptedException ie ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " message thread was interrupted " + ie);
        } catch ( ClassNotFoundException cnfe ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " class of object returned from " + authcellname + " not found " + cnfe);
        } catch ( Throwable t ) {
          throw new AuthorizationServiceException("authRequestID " + authRequestID + " recevied exception " + t.getMessage());
        }

      return PwdRecord;
    }

  public void setLogLevel(String level) {
    if (level==null) {
      esay("setLogLevel called with null argument. Log level not changed.");
      return;
    }

    String newlevel = level.toUpperCase();
    if( newlevel.equals("DEBUG") ||
        newlevel.equals("INFO")  ||
        newlevel.equals("WARN")  ||
        newlevel.equals("ERROR")  )
      loglevel = newlevel;
    else
      esay("Log level not set. Allowed values are DEBUG, INFO, WARN, ERROR.");
  }

} //end of class AuthorizationService
