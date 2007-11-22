/*
 * $Id: UserMetaDataProvider_gPlazma.java,v 1.9 2007-07-24 09:58:39 tigran Exp $
 */
package diskCacheV111.admin ;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;

import diskCacheV111.services.authorization.AuthorizationService;
import diskCacheV111.services.authorization.AuthorizationServiceException;
import diskCacheV111.util.UserAuthBase;

/**
 * Author : Patrick Fuhrmann, Vladimir Podstavkov
 * Based on the UserMetaDataProviderExample
 *
 */
public class UserMetaDataProvider_gPlazma implements UserMetaDataProvider {

    private final CellAdapter _cell ;
    private final Args        _args    ;
    private final String      _ourName ;

    private int     _requestCount      = 0 ;
    private Map<String, Integer> _userStatistics    = new HashMap<String, Integer>();
    protected boolean _use_gplazmaAuthzCell=false;
    protected boolean _use_gplazmaAuthzModule=false;
    private AuthorizationService _authServ = null;


    /**
     * We are assumed to provide the following constructor signature.
     */
    public UserMetaDataProvider_gPlazma(CellAdapter cell) {

        _cell    =  cell ;
        _args    = _cell.getArgs() ;
        _ourName = this.getClass().getName() ;



        if( _args.getOpt("use-gplazma-authorization-module") != null) {
            _use_gplazmaAuthzModule=
            _args.getOpt("use-gplazma-authorization-module").equalsIgnoreCase("true");
        }

        if( _args.getOpt("use-gplazma-authorization-cell") != null) {
            _use_gplazmaAuthzCell=
            _args.getOpt("use-gplazma-authorization-cell").equalsIgnoreCase("true");
        }

        if( _use_gplazmaAuthzModule && _use_gplazmaAuthzCell  ) {
        	throw new
        		IllegalArgumentException(_ourName+" : use-gplazma-authorization-cell and  use-gplazma-authorization-module defined at the same time.");
        }

        if( !(_use_gplazmaAuthzModule || _use_gplazmaAuthzCell)  ) {
        	throw new
        		IllegalArgumentException(_ourName+" : use-gplazma-authorization-cell or use-gplazma-authorization-module have to be defined.");
        }


        try {

        	if( _use_gplazmaAuthzCell ) {
        		_authServ = new AuthorizationService(_cell);
        	}else{
                String gplazmaPolicyFilePath = _args.getOpt("gplazma-authorization-module-policy");
                if (gplazmaPolicyFilePath == null) {
                    throw new IllegalArgumentException(_ourName+" : -gplazma-authorization-module-policy not specified");
                }
        		_authServ = new AuthorizationService(gplazmaPolicyFilePath);
        	}

        }catch(AuthorizationServiceException ae) {
            _cell.esay(ae);
            _cell.esay(ae.getMessage());
        }

    }

    /**
     * just for the fun of it
     */
    public String hh_ls = "" ;

    public String ac_ls( Args args ) {
        StringBuilder sb = new StringBuilder() ;

        for ( Map.Entry<String, Integer> entry: _userStatistics.entrySet() ) {
            sb.append(entry.getKey()).
                append("  ->  ").
                append(entry.getValue().toString()).
                append("\n") ;
        }
        return sb.toString();
    }

    private void updateStatistics( String userName ) {
        Integer count = _userStatistics.get(userName);
        int c = count == null ? 0 : count.intValue() ;
        _userStatistics.put( userName , new Integer( c+1 ) ) ;
        _requestCount++ ;
    }

    /**
     * and of course the interface definition
     */
    public synchronized Map getUserMetaData( String userName, String userRole, List attributes )
        throws Exception {

        //
        // 'attributes' is a list of keys somebody (door)
        // needs from us. We are assumed to prepare
        // a map containing the 'key' and the
        // corresponding values.
        // we should at least be prepared to know the
        // 'uid','gid' of the user.
        // If we are not sure about the user, we should
        // throw an exception rather returning an empty
        // map.
        //
        updateStatistics( userName ) ;
        //
        // get the information for the user
        //
        Map<String, String> result = getUserMD(userName, userRole) ;
        //
        // check for minimum requirements
        //
        if ( ( result.get("uid") == null ) ||
             ( result.get("gid") == null ) ||
             ( result.get("home") == null )  ) {
            throw new IllegalArgumentException(_ourName+" : insufficient info for user : "+userName+"->"+userRole);
        }

        return result;

    }


    private Map<String, String> getUserMD(String userPrincipal, String userRole) throws Exception {

        UserAuthBase pwdRecord = null;
        Map<String, String> answer = new HashMap<String, String>() ;
        int uid, gid;
        String home;

        try {
        	// dcap hack
        	if( userRole != null && userRole.equals("UNSPECIFIED") ) {
        		userRole = "";
        	}


        	if(_use_gplazmaAuthzCell ) {
        		// cell
        		pwdRecord = _authServ.authenticate(userPrincipal, userRole, new CellPath("gPlazma"), _cell).getUserAuthBase();
        	}else{
        		// module
        		pwdRecord = _authServ.authorize(userPrincipal, userRole, null, null, null);
        	}

            if( pwdRecord == null ) {
                throw new AuthorizationServiceException("User not found");
            }

            uid  = pwdRecord.UID;
            gid  = pwdRecord.GID;
            home = pwdRecord.Home;

            answer.put("uid", String.valueOf(uid));
            answer.put("gid", String.valueOf(gid));
            answer.put("home", home);

            _cell.say("User "+userRole+" logged in");

        }catch(AuthorizationServiceException ae) {
            _cell.esay("Authorization " + userPrincipal + ":"+ userRole+ " failed: " + ae.getMessage() );
        }


        return answer;
    }


    /**
     * and of course the interface definition
     */
    public String toString() {
        return "rc="+_requestCount;
    }

}

/*
 * $Log: not supported by cvs2svn $
 * Revision 1.8  2007/05/24 13:51:07  tigran
 * merge of 1.7.1 and the head
 *
 * Revision 1.1.2.4.2.1  2007/03/20 19:19:11  tdh
 * Modified AuthorizationService to return AuthorizationMessage instead of UserAuthBase.
 * Changed UserMetaDataProvider_gPlazma, DCacheAuthorization, and GssFtpDoorV1 to receive AuthorizationMessage.
 * Modified GPLAZMA to operate on LinkedLists and to take requested usernames.
 * Modified AuthorizationService to operate on LinkedLists.
 * Changed methods in AbstractFtpDoorV1 to try again with the next UserAuthRecord in AuthorizationMessage if permission is denied.
 *
 * Revision 1.1.2.4  2006/11/13 19:39:59  tigran
 * changes from HEAD:
 * UserMetaDataProvider_gPlazma and GssFtpDoorV1 passing cell to gPlazma constructor to keep say and esay under control
 * AuthorizationService closes delegation socket (CLOSE_WAIT)
 * SocketAdapter code cleanup
 * EBlockReceiverNio fixed pool looping
 *
 * Revision 1.6  2006/11/12 15:53:18  tigran
 * pass cell to gPlazma
 * if cell is not given, then System.out.println is used
 *
 * Revision 1.5  2006/10/17 08:19:29  tigran
 * added gPlazma cell suport into dcap
 *
 * Revision 1.1.2.3  2006/10/16 07:26:56  tigran
 * added gPlazma cell suport into dcap
 *
 * Revision 1.1.2.2  2006/09/07 07:37:22  tigran
 * added VOMS support for dcap ( including extended proxy )
 *
 * Revision 1.1.2.1  2006/08/17 09:15:28  tigran
 * added gPlazma support for DCAP
 *
 * Revision 1.1  2006/08/16 15:04:37  tigran
 * added gPlazma support for DCAP
 *
 */
