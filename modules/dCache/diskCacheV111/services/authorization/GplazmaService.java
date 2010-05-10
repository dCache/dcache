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

   public AuthorizationRecord getUserRecord(String userPrincipal ,List<String> userRoles)
       throws AuthorizationException
    {
        AuthorizationRecord authRecord;

        /*
         * for now we request only one role and take only one record
         */

        if( _use_gplazmaAuthzCell ) {
            // cell
            AuthenticationMessage authmessage = _authHelp.authorize(userPrincipal, userRoles);
            AuthorizationMessage authzmsg = new AuthorizationMessage(authmessage);
            authRecord = authzmsg.getAuthorizationRecord();
        }else{
            // module
            if (gplazmaPolicyFilePath == null) {
                throw new IllegalArgumentException("-gplazma-authorization-module-policy not specified");
            }
            Map <NameRolePair, gPlazmaAuthorizationRecord> authzMappingrecords =
                    _authServ.authorize(userPrincipal, userRoles, null, null, null, null);

            authRecord = RecordConvert.gPlazmaToAuthorizationRecord(authzMappingrecords);
        }

        if(_logAuth.isDebugEnabled() ) {
            _logAuth.debug("Mapped [ " + userPrincipal + " ]" + "[ " + userRoles + " ] to : " + authRecord);
        }

        return authRecord;

    }

}
