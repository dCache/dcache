package diskCacheV111.services.authorization;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import diskCacheV111.util.UserAuthRecord;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;


/*
 * this is a temporary solution to let dcap to talk directly with gPlazma
 */

@Deprecated
public class GplazmaService {


    private static final Logger _logAuth = Logger.getLogger("logger.org.dcache.authorization." + GplazmaService.class.getName());


    private static final AtomicReference<GplazmaService> INSTANCE = new AtomicReference<GplazmaService>();

    private final boolean _use_gplazmaAuthzModule;
    private final boolean _use_gplazmaAuthzCell;
    private final AuthorizationService _authServ;
    private final CellPath _gPlazmaCell;

    // Only factory method allowed
    private GplazmaService(Args args) throws AuthorizationServiceException {


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
            _authServ = new AuthorizationService();
        }else{
            String gplazmaPolicyFilePath = args.getOpt("gplazma-authorization-module-policy");
            if (gplazmaPolicyFilePath == null) {
                throw new IllegalArgumentException("-gplazma-authorization-module-policy not specified");
            }
            _authServ = new AuthorizationService(gplazmaPolicyFilePath);
        }


        /*
         * TODO: configurable in batch file
         */
        _gPlazmaCell = new CellPath("gPlazma");
    }


    public static GplazmaService getInstance( Args args ) throws AuthorizationServiceException {

        GplazmaService service = INSTANCE.get();

        if( service == null ) {

            /*
             * at this point some other thread may already have updated the reference,
             * but it's OK with us, while we do not require true singleton. In the worst
             * case on 'extra' instances will be created, but only one will be used.
             */

            INSTANCE.compareAndSet(null, new GplazmaService(args));
            service = INSTANCE.get();
        }

        return service;

    }


   public UserAuthRecord getUserRecord( CellAdapter cell, String userPrincipal ,String userRole , Args args ) throws AuthorizationServiceException {


        UserAuthRecord pwdRecord = null;

        /*
         * for now we request only one role and take only one record
         */

        if( _use_gplazmaAuthzCell ) {
            // cell
            pwdRecord = _authServ.authenticate(userPrincipal, userRole, _gPlazmaCell, cell).getUserAuthRecords().get(0);
        }else{
            // module
            pwdRecord = _authServ.authorize(userPrincipal, userRole, null, null, null);
        }


        if(_logAuth.isDebugEnabled() ) {
            _logAuth.debug("Mapped [ " + userPrincipal + " ]" + "[ " + userRole + " ] to : " + pwdRecord);
        }

        return pwdRecord;

    }

}
