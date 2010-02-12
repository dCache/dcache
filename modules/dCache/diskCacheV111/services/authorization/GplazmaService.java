package diskCacheV111.services.authorization;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.auth.*;
import org.dcache.vehicles.AuthorizationMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;
import gplazma.authz.AuthorizationController;
import gplazma.authz.AuthorizationException;
import gplazma.authz.util.NameRolePair;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import diskCacheV111.vehicles.AuthenticationMessage;
import dmg.cells.nucleus.CellEndpoint;

/*
* this is a temporary solution to let dcap to talk directly with gPlazma
*/

@Deprecated
public class GplazmaService {


    private static final Logger _logAuth = LoggerFactory.getLogger("logger.org.dcache.authorization." + GplazmaService.class.getName());


    private final boolean _use_gplazmaAuthzModule;
    private final boolean _use_gplazmaAuthzCell;
    private AuthzQueryHelper _authHelp;
    private AuthorizationController _authServ;
    private String gplazmaPolicyFilePath;

    public GplazmaService(CellEndpoint endpoint)
        throws AuthorizationException
    {
        Args args = endpoint.getArgs();
        if( args.getOpt("use-gplazma-authorization-module") != null) {
            _use_gplazmaAuthzModule=
            args.getOpt("use-gplazma-authorization-module").equalsIgnoreCase("true");
        }else{
            _use_gplazmaAuthzModule = false;
        }

        if( args.getOpt("use-gplazma-authorization-cell") != null) {
            _use_gplazmaAuthzCell=
            args.getOpt("use-gplazma-authorization-cell").equalsIgnoreCase("true");
        }else{
            _use_gplazmaAuthzCell = false;
        }

        if( _use_gplazmaAuthzModule && _use_gplazmaAuthzCell  ) {
            throw new
                IllegalArgumentException("use-gplazma-authorization-cell and  use-gplazma-authorization-module defined at the same time.");
        }

        if( !(_use_gplazmaAuthzModule || _use_gplazmaAuthzCell)  ) {
            throw new
                IllegalArgumentException("use-gplazma-authorization-cell or use-gplazma-authorization-module have to be defined.");
        }

        if( _use_gplazmaAuthzCell ) {
            _authHelp = new AuthzQueryHelper(endpoint);
        }else{
            gplazmaPolicyFilePath = args.getOpt("gplazma-authorization-module-policy");
        }
    }

   public UserAuthRecord getUserRecord(String userPrincipal ,String userRole)
       throws AuthorizationException
    {
        AuthorizationRecord authRecord;
        UserAuthRecord pwdRecord = null;

        /*
         * for now we request only one role and take only one record
         */

        if( _use_gplazmaAuthzCell ) {
            // cell
            AuthenticationMessage authmessage = _authHelp.authorize(userPrincipal, userRole);
            AuthorizationMessage authzmsg = new AuthorizationMessage(authmessage);
            authRecord = authzmsg.getAuthorizationRecord();
        }else{
            // module
            if (gplazmaPolicyFilePath == null) {
                throw new IllegalArgumentException("-gplazma-authorization-module-policy not specified");
            }
            _authServ = new AuthorizationController(gplazmaPolicyFilePath);
            gPlazmaAuthorizationRecord gauthrec = _authServ.authorize(userPrincipal, userRole, null, null, null, null);
            //LinkedList<gPlazmaAuthorizationRecord> gauthlist = new LinkedList<gPlazmaAuthorizationRecord>();
            //gauthlist.add(gauthrec);
            Map <NameRolePair, gPlazmaAuthorizationRecord> authzMappingrecords = new LinkedHashMap <NameRolePair, gPlazmaAuthorizationRecord>();
            authzMappingrecords.put(new NameRolePair(userPrincipal, userRole), gauthrec);
            authRecord = RecordConvert.gPlazmaToAuthorizationRecord(authzMappingrecords);
        }


        if (authRecord == null) return null;

        Set<GroupList> uniqueGroupListSet = new LinkedHashSet<GroupList>(authRecord.getGroupLists());
        Iterator<GroupList> _userAuthGroupLists = uniqueGroupListSet.iterator();

        if (_userAuthGroupLists == null || !_userAuthGroupLists.hasNext()) return null;

        GroupList grplist  = _userAuthGroupLists.next();
        String fqan = grplist.getAttribute();
        int i=0, glsize = grplist.getGroups().size();
        int GIDS[] = (glsize > 0) ? new int[glsize] : null;
        for(Group group : grplist.getGroups()) {
             GIDS[i++] = group.getGid();
        }
        pwdRecord = new UserAuthRecord(
                authRecord.getIdentity(),
                authRecord.getName(),
                fqan,
                authRecord.isReadOnly(),
                authRecord.getPriority(),
                authRecord.getUid(),
                GIDS,
                authRecord.getHome(),
                authRecord.getRoot(),
                "/",
                new HashSet<String>());

        if(_logAuth.isDebugEnabled() ) {
            _logAuth.debug("Mapped [ " + userPrincipal + " ]" + "[ " + userRole + " ] to : " + pwdRecord);
        }

        return pwdRecord;

    }

}
