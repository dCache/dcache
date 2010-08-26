package diskCacheV111.services.authorization;

import java.util.concurrent.atomic.AtomicReference;
import java.util.*;

import org.apache.log4j.Logger;

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


    private static final Logger _logAuth = Logger.getLogger("logger.org.dcache.authorization." + GplazmaService.class.getName());


    private static final AtomicReference<GplazmaService> INSTANCE = new AtomicReference<GplazmaService>();

    private final boolean _use_gplazmaAuthzModule;
    private final boolean _use_gplazmaAuthzCell;
    private AuthzQueryHelper _authHelp;
    private AuthorizationController _authServ;
    private final CellPath _gPlazmaCell;
    private String gplazmaPolicyFilePath;

    // Only factory method allowed
    private GplazmaService(CellEndpoint caller) throws AuthorizationException {

        Args args = caller.getArgs();
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
            _authHelp = new AuthzQueryHelper(caller);
        }else{
            gplazmaPolicyFilePath = args.getOpt("gplazma-authorization-module-policy");
        }


        /*
         * TODO: configurable in batch file
         */
        _gPlazmaCell = new CellPath("gPlazma");
    }


    public static GplazmaService getInstance(CellEndpoint caller) throws AuthorizationException {

        GplazmaService service = INSTANCE.get();

        if( service == null ) {

            /*
             * at this point some other thread may already have updated the reference,
             * but it's OK with us, while we do not require true singleton. In the worst
             * case on 'extra' instances will be created, but only one will be used.
             */

            INSTANCE.compareAndSet(null, new GplazmaService(caller));
            service = INSTANCE.get();
        }

        return service;

    }


   public UserAuthRecord getUserRecord( CellEndpoint cell, String userPrincipal ,String userRole , Args args ) throws AuthorizationException {

        AuthorizationRecord authRecord;
        UserAuthRecord pwdRecord = null;

        /*
         * for now we request only one role and take only one record
         */

        if( _use_gplazmaAuthzCell ) {
            // cell
            AuthenticationMessage authmessage = _authHelp.authorize(userPrincipal, userRole, _gPlazmaCell, cell);
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
