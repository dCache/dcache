package diskCacheV111.admin ;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.CharArrayWriter;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SpreadAndWait;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolLinkInfo;
import diskCacheV111.vehicles.PoolMgrGetPoolByLink;
import diskCacheV111.vehicles.PoolMgrGetPoolLinks;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.QuotaMgrCheckQuotaMessage;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.cells.services.GetAllDomainsReply;
import dmg.cells.services.GetAllDomainsRequest;
import dmg.util.AclException;
import dmg.util.AuthorizedString;
import dmg.util.CommandAclException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;
import dmg.util.command.HelpFormat;
import dmg.util.command.Option;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Args;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.Glob;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.util.concurrent.Futures.*;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static org.dcache.util.Glob.parseGlobToPattern;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.RED;

public class UserAdminShell
    extends CommandInterpreter
    implements Completer
{
    private static final Logger _log =
        LoggerFactory.getLogger(UserAdminShell.class);

    private static final String ADMIN_COMMAND_NOOP = "xyzzy";
    private static final int CD_PROBE_MESSAGE_TIMEOUT_MS = 1000;
    public static final StringsCompleter SHELL_COMMAND_COMPLETER =
            new StringsCompleter("\\c", "\\l", "\\s", "\\sn", "\\sp", "\\q", "\\h", "\\?");
    private final Completer POOL_MANAGER_COMPLETER = createRemoteCompleter("PoolManager");
    private final Completer PNFS_MANAGER_COMPLETER = createRemoteCompleter("PnfsManager");

    private final CellEndpoint _cellEndpoint;
    private final CellStub _acmStub;
    private final CellStub _poolManager;
    private final CellStub _pnfsManager;
    private final CellStub _cellStub;
    private String      _user;
    private String      _authUser;
    private long        _timeout  = TimeUnit.MINUTES.toMillis(5);
    private boolean     _fullException;
    private final String      _instance ;
    private Position    _currentPosition = null;
    private Completer _completer;

    private static class Position
    {
        final String remoteName;
        final CellPath remote;

        Position(String name, CellPath path)
        {
            remoteName = name;
            remote = path;
        }
    }

    public UserAdminShell(String user, CellEndpoint cellEndpoint, Args args) {
       _cellEndpoint = cellEndpoint;
       _user     = user ;
       _authUser = user ;

        _acmStub = new CellStub(cellEndpoint, new CellPath("acm"));
        _poolManager = new CellStub(cellEndpoint, new CellPath("PoolManager"));
        _pnfsManager = new CellStub(cellEndpoint, new CellPath("PnfsManager"), 30000, MILLISECONDS);
        _cellStub = new CellStub(cellEndpoint);

       String prompt = args.getOpt("dCacheInstance");
       if( prompt == null || !prompt.equals("hide") ){
           if( prompt == null || prompt.length() == 0 ){
               try{
                   prompt = InetAddress.getLocalHost().getHostName() ;
               }catch(UnknownHostException ee){
                   prompt = null;
               }
           }
           _instance = prompt;
       }else{
           _instance = null;
       }
    }

    @Override
    protected Serializable doExecute(CommandEntry entry, Args args, String[] acls)
            throws CommandException
    {
        try {
            checkPermission(acls);
            return super.doExecute(entry, args, acls);
        } catch (AclException e) {
            throw new CommandAclException(e.getPrincipal(), e.getAcl());
        }
    }

    protected String getUser(){ return _user ; }

    protected void checkPermission(String [] acls) throws AclException
    {
        if (acls.length > 0) {
            AclException e = null;
            for (String acl : acls) {
                try {
                    checkPermission(acl);
                    return;
                } catch (AclException ce) {
                    e = ce;
                }
            }
            throw e;
        }
    }

    public void checkPermission( String aclName )
           throws AclException {

         Object [] request = new Object[5] ;
         request[0] = "request" ;
         request[1] = "<nobody>" ;
         request[2] = "check-permission" ;
         request[3] = getUser() ;
         request[4] = aclName ;
         Object[] r;
         try{
            r = _acmStub.sendAndWait(request, Object[].class, _timeout);
         } catch (TimeoutCacheException e) {
             throw new AclException(e.getMessage());
         } catch (CacheException | InterruptedException e) {
             throw new AclException("Problem: " + e.getMessage());
         }
         if (r.length < 6 | !(r[5] instanceof Boolean)) {
             throw new AclException("Protocol violation 4456");
         }

         if (!((Boolean) r[5])) {
             throw new AclException(getUser(), aclName);
         }
    }

    /**
     * Asynchronously queries the cells of {@code domain} matching {@code cellPredicate}.
     *
     * The resulting list is sorted and fully qualified. Errors are logged and otherwise ignored.
     */
    private ListenableFuture<List<String>> getCells(String domain, Predicate<String> cellPredicate)
    {
        /* Query System cell and split, filter, sort and expand the answer. */
        ListenableFuture<List<String>> future = transform(
                _cellStub.send(new CellPath("System", domain), "ps", String.class),
                (String s) ->
                        Arrays.stream(s.split("\n"))
                                .filter(cellPredicate)
                                .sorted(CASE_INSENSITIVE_ORDER)
                                .map(cell -> cell + "@" + domain)
                                .collect(toList()));
        /* Log and ignore any errors. */
        return withFallback(future,
                            t -> {
                                _log.debug("Failed to query the System cell of domain {}: {}", domain, t);
                                return immediateFuture(Collections.emptyList());
                            });
    }

    private List<String> expandCellPatterns(List<String> patterns)
            throws CacheException, InterruptedException, ExecutionException
    {
        Map<String, Collection<String>> domains =
                _cellStub
                        .sendAndWait(new CellPath("RoutingMgr"), new GetAllDomainsRequest(),
                                     GetAllDomainsReply.class)
                        .getDomains();

        List<ListenableFuture<List<String>>> futures = new ArrayList<>();
        for (String pattern : patterns) {
            String[] s = pattern.split("@", 2);
            Predicate<String> matchesCellName = toGlobPredicate(s[0]);
            if (s.length == 1) {
                /* Add matching well-known cells. */
                domains.values().stream()
                        .flatMap(Collection::stream)
                        .filter(matchesCellName)
                        .sorted(CASE_INSENSITIVE_ORDER)
                        .map(Collections::singletonList)
                        .map(Futures::immediateFuture)
                        .forEach(futures::add);
            } else {
                /* Query the cells of each matching domain.
                 */
                Predicate<String> matchesDomainName = toGlobPredicate(s[1]);
                domains.keySet().stream()
                        .filter(matchesDomainName)
                        .sorted(CASE_INSENSITIVE_ORDER)
                        .map(domain -> getCells(domain, matchesCellName))
                        .forEach(futures::add);
            }
        }

        /* Collect and flatten the result. */
        return allAsList(futures).get().stream().flatMap(Collection::stream).collect(toList());
    }

    private Predicate<String> toGlobPredicate(String s)
    {
        return s.isEmpty() ? (String) -> true : parseGlobToPattern(s).asPredicate();
    }


    public String getHello(){
      return "dCache (" + Version.of(UserAdminShell.class).getVersion() + ")\n" + "Type \"\\?\" for help.\n";
    }
    public String getPrompt(){
        return  ( _instance == null ? "" : ( "[" + _instance + "] " ) ) +
                ( _currentPosition == null ? "(local) " : ( "(" + _currentPosition.remoteName +") " ) ) +
                getUser()+" > " ;
    }
    public static final String hh_set_exception = "message|detail" ;
    public String ac_set_exception_$_0_1( Args args ) throws CommandException {
       if( args.argc() > 0 ){
          if( args.argv(0).equals( "message" ) ){
             _fullException = false ;
          }else if ( args.argv(0).equals("detail") ){
             _fullException = true ;
          }else {
              throw new
                      CommandSyntaxException("set exception message|detail");
          }
       }
       return "Exception = " +( _fullException ? "detail" : "message" ) ;
    }
    public static final String hh_set_timeout = "<timeout/sec> # command timeout in seconds";
    public String ac_set_timeout_$_0_1( Args args ){
        if( args.argc() > 0 ){
           long timeout = Integer.parseInt(args.argv(0)) * 1000L ;
           if( timeout < 1000L ) {
               throw new
                       IllegalArgumentException("<timeout> >= 1");
           }
           _timeout = timeout;
        }
        return "Timeout = "+(_timeout/1000L) ;

    }

    public static final String hh_getpoolbylink = "<linkName> [-size=<filesize>] [-service=<serviceCellName]" ;
    public String ac_getpoolbylink_$_1( Args args ) throws Exception {

       String linkName   = args.argv(0) ;
       String service    = args.getOpt("service");
       String sizeString = args.getOpt("size") ;

       PoolMgrGetPoolByLink msg = new PoolMgrGetPoolByLink(  linkName ) ;
       if( sizeString != null ) {
           msg.setFilesize(Long.parseLong(sizeString));
       }
       service = service == null ? "PoolManager" : service ;

       Object result = sendObject(  service , msg ) ;
       if( result == null ) {
           throw new
                   Exception("QuotaRequest timed out");
       }

       if( result instanceof PoolMgrGetPoolByLink ){
          PoolMgrGetPoolByLink link = (PoolMgrGetPoolByLink)result ;
          int rc = link.getReturnCode() ;
          if( rc != 0 ){
              return "Problem "+rc+" <"+link.getErrorObject()+"> reported for link "+linkName ;
          }else{
              return "Pool <"+link.getPoolName()+"> selected for link "+linkName ;
          }
       }
       return "Unexpected class "+result.getClass().getName()+
                  " arrived with message "+result.toString();
    }
    public static final String hh_quota_query  = "<storageClassName>|* [-l] [-service=<serviceCellName>]" ;
    public Object ac_quota_query_$_1( Args args ) throws Exception {

       String storageClassName  = args.argv(0) ;
       String service           = args.getOpt("service");
       service = service == null ? "QuotaManager" : service ;
       boolean extended         = args.hasOption("l") ;

       Message msg;

       if( storageClassName.equals("*" ) ){
           msg = new PoolMgrGetPoolLinks() ;
       }else{
           msg = new QuotaMgrCheckQuotaMessage( storageClassName ) ;
       }

       Object result = sendObject(  service , msg ) ;
       if( result == null ) {
           throw new
                   Exception("QuotaRequest timed out");
       }

       if(  result instanceof QuotaMgrCheckQuotaMessage  ){
          return result.toString() ;
       }else if( result instanceof PoolMgrGetPoolLinks ){
          if(extended){
            PoolMgrGetPoolLinks info  = (PoolMgrGetPoolLinks)result ;
            PoolLinkInfo   []   links = info.getPoolLinkInfos() ;
            StringBuilder sb    = new StringBuilder() ;
            if( links == null ) {
                return "Object doesn't contain a Links list";
            }

              for (PoolLinkInfo link : links) {
                  sb.append(" Link ").append(link.getName()).append(" : ")
                          .append(link.getAvailableSpaceInBytes()).append("\n");
                  String[] storageGroups = link.getStorageGroups();
                  if (storageGroups == null) {
                      continue;
                  }
                  for (String storageGroup : storageGroups) {
                      sb.append("    ").append(storageGroup).append("\n");
                  }
              }
            return sb.toString();
          }
          return result.toString() ;
       }
       return "Unexpected class "+result.getClass().getName()+
                  " arrived with message "+result.toString();

    }

    public static final String hh_set_sticky = "<pnfsId>|<globalPath> [-target=<target>] [-silent]" ;
    public Object ac_set_sticky_$_1( Args args ) throws Exception {
       return setSticky(
             args.argv(0) ,
             args.getOpt("target") ,
             true ,
             !args.hasOption("silent") ? new StringBuffer() : null ) ;
    }
    public static final String hh_set_unsticky = "<pnfsId>|<globalPath> [-target=<target>] [-silent]" ;
    public Object ac_set_unsticky_$_1( Args args ) throws Exception  {
       return setSticky(
             args.argv(0) ,
             args.getOpt("target") ,
             false ,
             !args.hasOption("silent") ? new StringBuffer() : null ) ;
    }
    public static final String hh_uncache = "<pnfsId>|<globalPath> [-target=<target>] [-silent]" ;
    public Object ac_uncache_$_1( Args args ) throws Exception {
      try{
       return uncache(
             args.argv(0) ,
             args.getOpt("target") ,
             !args.hasOption("silent") ? new StringBuffer() : null ) ;
      }catch(Exception ee ){
         ee.printStackTrace();
         throw ee ;
      }
    }


    public static final String fh_repinfoof =
            "repinfoof <pnfsId> | <globalPath> # lists info the status of a file by pnfsid or by path.\n" +
            "The information includes pools on which the file has been stored (info provided by \"cacheinfoof\" in the PnfsManager cell)\n" +
            "and the repository info of the file (info provided by \"rep ls\" in the pool cell).\n";

    public static final String hh_repinfoof = "<pnfsId> | <globalPath>";

    public String ac_repinfoof_$_1(Args args)
            throws CacheException, SerializationException, NoRouteToCellException, InterruptedException, CommandException
    {
        StringBuilder sb = new StringBuilder();
        String fileIdentifier = args.argv(0);

        FileAttributes fileAttributes = getFileLocations(fileIdentifier);

        PnfsId pnfsId = fileAttributes.getPnfsId();

        if (fileAttributes.getLocations().isEmpty()) { // nothing to do
            return "No file locations found";
        }

        Map<CellPath, String> replies = askPoolsForRepLs(fileAttributes, pnfsId);
        for (Map.Entry<CellPath, String> reply : replies.entrySet()) {
            sb.append(reply.getKey().getCellName()).append(" : ");
            sb.append(reply.getValue());
        }

        return sb.toString();
    }

    private FileAttributes getFileLocations(String fileIdentifier)
            throws CacheException, SerializationException, NoRouteToCellException, InterruptedException, CommandException
    {
        Set<FileAttribute> request = EnumSet.of(FileAttribute.LOCATIONS, FileAttribute.PNFSID);
        PnfsGetFileAttributes msg;

        if (PnfsId.isValid(fileIdentifier)) {
            PnfsId pnfsId = new PnfsId(fileIdentifier);
            msg = new PnfsGetFileAttributes(pnfsId, request);
        } else {
            msg = new PnfsGetFileAttributes(fileIdentifier, request);
        }

        PnfsGetFileAttributes replyFileLocations = (PnfsGetFileAttributes) sendObject("PnfsManager", msg);

        if (replyFileLocations == null) {
            throw new CacheException("Request to the PnfsManager timed out");
        }

        if (replyFileLocations.getReturnCode() != 0) {
            throw CacheExceptionFactory.exceptionOf(replyFileLocations);
        }

        return replyFileLocations.getFileAttributes();
    }

    private Map<CellPath,String> askPoolsForRepLs(FileAttributes fileAttributes, PnfsId pnfsId) {

        SpreadAndWait<String> spreader = new SpreadAndWait<>(new CellStub(_cellEndpoint, null, _timeout));

        for (String poolName : fileAttributes.getLocations()) {
            spreader.send(new CellPath(poolName), String.class, "rep ls " + pnfsId);
        }

        try {
            spreader.waitForReplies();
        } catch (InterruptedException ex) {
            _log.info("InterruptedException while waiting for a reply from pools " + ex);
        }

        return spreader.getReplies();
    }

    private String setSticky(
               String destination ,
               String target ,
               boolean mode ,
               StringBuffer sb )
            throws Exception {

       if (Strings.isNullOrEmpty(target)) {
           target = "*";
       }

       boolean verbose = sb != null ;

       PnfsFlagReply reply = setPnfsFlag( destination , "s" , target, mode ) ;

       PnfsId pnfsId = reply.getPnfsId() ;

       PnfsGetCacheLocationsMessage pnfsMessage =
                    new PnfsGetCacheLocationsMessage(pnfsId) ;

       pnfsMessage = (PnfsGetCacheLocationsMessage)sendObject("PnfsManager",pnfsMessage) ;
       if( pnfsMessage.getReturnCode() != 0 ) {
           throw new
                   FileNotFoundException(destination);
       }

       List<String> list = pnfsMessage.getCacheLocations() ;
       if( verbose ){
          sb.append("Location(s) : ") ;
          for( String location : list ){
             sb.append(location).append(",") ;
          }
          sb.append("\n");
       }
       if( target.equals("*") ){
          if( verbose ) {
              sb.append("Selection : <all>\n");
          }
       }else if( list.contains(target) ){
          if( verbose ) {
              sb.append("Selection : ").append(target).append("\n");
          }
          list = new ArrayList<>();
          list.add(target);
       }else{
          if( verbose ) {
              sb.append("Selection : <nothing>\n");
          }
          return sb == null ? "" : sb.toString() ;
       }
       PoolSetStickyMessage sticky;

       for( String poolName: list ){
           if( verbose ) {
               sb.append(poolName).append(" : ");
           }
           try{
              sticky = new PoolSetStickyMessage( poolName , pnfsId , mode ) ;
              sticky = (PoolSetStickyMessage)sendObject(poolName,sticky) ;
              if( verbose ){
                 int rc = sticky.getReturnCode() ;
                 if( rc != 0 ) {
                     sb.append("[").append(rc).append("] ").
                             append(sticky.getErrorObject().toString());
                 } else {
                     sb.append("ok");
                 }
              }
           }catch(Exception ee ){
              if(verbose) {
                  sb.append(ee.getMessage());
              }
           }
           if(verbose) {
               sb.append("\n");
           }
       }

       return sb == null ? "" : sb.toString() ;
    }
    private String uncache(  String destination ,  String target , StringBuffer sb )
            throws Exception {

       if( ( target == null ) || ( target.equals("") ) ) {
           target = "*";
       }

       boolean verbose = sb != null ;

       PnfsId pnfsId;
       if( destination.startsWith( "/pnfs" ) ){

          PnfsMapPathMessage map = new PnfsMapPathMessage( destination ) ;

          map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

          if( map.getReturnCode() != 0 ){
             Object o = map.getErrorObject() ;
             if( o instanceof Exception ) {
                 throw (Exception) o;
             } else {
                 throw new Exception(o.toString());
             }
          }

          if( ( pnfsId = map.getPnfsId() ) == null ) {
              throw new
                      FileNotFoundException(destination);
          }

       }else{
          pnfsId = new PnfsId( destination ) ;
       }

       int dbId = pnfsId.getDatabaseId() ;

       try{
          checkPermission( "pool.*.uncache" ) ;
       }catch( AclException ee ){
          checkPermission( "pool."+dbId+".uncache" ) ;
       }

       PnfsGetCacheLocationsMessage pnfsMessage =
                    new PnfsGetCacheLocationsMessage(pnfsId) ;

       pnfsMessage = (PnfsGetCacheLocationsMessage)sendObject("PnfsManager",pnfsMessage) ;

       if( pnfsMessage.getReturnCode() != 0 ) {
           throw new
                   FileNotFoundException(destination);
       }

       List<String> locations = pnfsMessage.getCacheLocations() ;
       if( verbose ){
          sb.append("Location(s) : ") ;
           for (Object location : locations) {
               sb.append(location.toString()).append(",");
           }
          sb.append("\n");
       }
       if( target.equals("*") ){
          if( verbose ) {
              sb.append("Selection : <all>\n");
          }
       }else if( locations.contains(target) ){
          if( verbose ) {
              sb.append("Selection : ").append(target).append("\n");
          }
          locations = new ArrayList<>();
          locations.add(target);
       }else{
          if( verbose ) {
              sb.append("Selection : <nothing>\n");
          }
          return sb == null ? "" : sb.toString() ;
       }
       PoolRemoveFilesMessage remove;
        for (Object location : locations) {
            String poolName = location.toString();
            if (verbose) {
                sb.append(poolName).append(" : ");
            }
            try {
                remove = new PoolRemoveFilesMessage(poolName, pnfsId.toString());
                remove = (PoolRemoveFilesMessage) sendObject(poolName, remove);
                if (verbose) {
                    int rc = remove.getReturnCode();
                    if (rc != 0) {
                        Object obj = remove.getErrorObject();
                        if ((obj != null) && (obj instanceof Object[])) {
                            Object o = ((Object[]) obj)[0];
                            if (o != null) {
                                sb.append("[").append(rc).append("] Failed ").
                                        append(o.toString());
                            }
                        } else if (obj != null) {
                            sb.append("[").append(rc).append("] Failed ").
                                    append(obj.toString());
                        }

                    } else {
                        sb.append("ok");
                    }

                }
            } catch (Exception ee) {
                if (verbose) {
                    sb.append(ee.getMessage());
                }
            }
            if (verbose) {
                sb.append("\n");
            }
        }

       return sb == null ? "" : sb.toString() ;
    }
    private class PnfsFlagReply {
       private PnfsId          _pnfsId;
       private PnfsFlagMessage _message;
       public PnfsFlagReply( PnfsId pnfsId , PnfsFlagMessage message ){
          _pnfsId  = pnfsId ;
          _message = message ;
       }
       public PnfsId getPnfsId(){ return _pnfsId ; }
       public PnfsFlagMessage getPnfsFlagMessage(){ return _message ; }
    }
    public static final String hh_flags_set = "<pnfsId>|<globalPath> <key> <value>";
    public Object ac_flags_set_$_3( Args args ) throws Exception {

       String destination   = args.argv(0) ;
       String key    = args.argv(1) ;
       String value  = args.argv(2) ;

       PnfsFlagMessage result =
           setPnfsFlag( destination , key , value, true ).getPnfsFlagMessage() ;

       return result.getReturnCode() == 0 ? "" : result.getErrorObject().toString() ;

    }
    private PnfsFlagReply setPnfsFlag(
        String destination ,
        String key ,
        String value ,
        boolean mode)
            throws Exception {

       PnfsId pnfsId;
       if( destination.startsWith("/pnfs") ){

          PnfsMapPathMessage map = new PnfsMapPathMessage( destination ) ;

          map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

          if( map.getReturnCode() != 0 ){
             Object o = map.getErrorObject() ;
             if( o instanceof Exception ) {
                 throw (Exception) o;
             } else {
                 throw new Exception(o.toString());
             }
          }

          pnfsId = map.getPnfsId() ;
          if (pnfsId == null) {
              throw new FileNotFoundException(destination);
          }


       }else{
          pnfsId = new PnfsId( destination ) ;
       }

       int dbId = pnfsId.getDatabaseId() ;

       try{
          checkPermission( "pnfs.*.update" ) ;
       }catch( AclException ee ){
          checkPermission("pnfs." + key + "." + dbId + ".update") ;
       }


       PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , key,
           mode?PnfsFlagMessage.FlagOperation.SET:PnfsFlagMessage.FlagOperation.REMOVE ) ;
       pfm.setValue( value ) ;

       PnfsFlagMessage result = (PnfsFlagMessage)sendObject( "PnfsManager" , pfm ) ;
       if( result.getReturnCode() != 0 ){
          Object o = result.getErrorObject() ;
          if( o instanceof Exception ) {
              throw (Exception) o;
          } else {
              throw new Exception(o.toString());
          }
       }

       return new PnfsFlagReply( pnfsId , result ) ;
    }
    public static final String hh_flags_remove = "<pnfsId> <key>";
    public Object ac_flags_remove_$_2( Args args ) throws Exception {
       PnfsId pnfsId;
       if( args.argv(0).startsWith( "/pnfs" ) ){

          PnfsMapPathMessage map = new PnfsMapPathMessage( args.argv(0) ) ;

          map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

          if( map.getReturnCode() != 0 ){
             Object o = map.getErrorObject() ;
             if( o instanceof Exception ) {
                 throw (Exception) o;
             } else {
                 throw new Exception(o.toString());
             }
          }

          pnfsId = map.getPnfsId() ;


       }else{
          pnfsId = new PnfsId( args.argv(0) ) ;
       }

       int dbId = pnfsId.getDatabaseId() ;

       String key    = args.argv(1) ;

       try{
          checkPermission( "pnfs.*.update" ) ;
       }catch( AclException ee ){
          checkPermission( "pnfs."+key+"."+dbId+".update" ) ;
       }


       PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , key, PnfsFlagMessage.FlagOperation.REMOVE ) ;

       PnfsFlagMessage result = (PnfsFlagMessage)sendObject( "PnfsManager" , pfm ) ;
       if( result.getReturnCode() != 0 ){
          Object o = result.getErrorObject() ;
          if( o instanceof Exception ) {
              throw (Exception) o;
          } else {
              throw new Exception(o.toString());
          }
       }
       return result.getReturnCode() == 0 ? "" : result.getErrorObject().toString() ;
    }
    public static final String hh_p2p = "<pnfsId> [<sourcePool> <destinationPool>] [-ip=<address]" ;
    public String ac_p2p_$_1_3( Args args )throws Exception {

       if( args.argc() >= 3 ){
           String source = args.argv(1) ;
           String dest   = args.argv(2) ;
           PnfsId pnfsId = new PnfsId( args.argv(0) ) ;

           FileAttributes fileAttributes = new FileAttributes();
           fileAttributes.setPnfsId(pnfsId);
           Pool2PoolTransferMsg p2p =
                new Pool2PoolTransferMsg( source , dest , fileAttributes ) ;


            _cellEndpoint.sendMessage(
                    new CellMessage(new CellPath(dest), p2p)
            ) ;

           return "P2p of "+pnfsId+" initiated from "+source+" to "+dest ;
       }else{
           PnfsId pnfsId = new PnfsId(args.argv(0) ) ;
           String ip     = args.getOpt("ip");

           PnfsGetFileAttributes fileAttributesMsg =
               new PnfsGetFileAttributes(pnfsId, PoolMgrReplicateFileMsg.getRequiredAttributes());

           fileAttributesMsg = _pnfsManager.sendAndWait(fileAttributesMsg);

           DCapProtocolInfo pinfo =
            new DCapProtocolInfo("DCap",0,0, new InetSocketAddress("localhost",0));


          String timeoutString = args.getOpt("timeout");
          long timeout = timeoutString != null ?
                         Long.parseLong(timeoutString)*1000L :
                         60000L ;

           PoolMgrReplicateFileMsg select =
                   new PoolMgrReplicateFileMsg(fileAttributesMsg.getFileAttributes(), pinfo);
           select = _poolManager.sendAndWait(select, timeout);
          return "p2p -> "+select.getPoolName() ;
       }
    }
    public String ac_modify_poolmode =
        " a) modify poolmode enable <poolname>[,<poolname>...]\n"+
        " b) modify poolmode [OPTIONS] disable <poolname>[,<poolname>...] [<code> [<message>]]\n"+
        "      OPTIONS :\n"+
        "        -fetch    #  disallows fetch (transfer to client)\n"+
        "        -stage    #  disallows staging (from HSM)\n"+
        "        -store    #  disallows store (transfer from client)\n"+
        "        -p2p-client\n"+
        "        -rdonly   #  := store,stage,p2p-client\n"+
        "        -strict   #  := disallows everything\n" ;
    public static final String hh_modify_poolmode =
        "enable|disable <poolname>[,<poolname>...] [<code> [<message>]] [-strict|-stage|-rdonly|-fetch|-store]" ;
    public String ac_modify_poolmode_$_2_4( Args args ) throws Exception {

       checkPermission( "*.*.*" ) ;

       String enable   = args.argv(0) ;
       String poolList = args.argv(1) ;
       String message  = args.argc() > 3 ? args.argv(3) : null ;
       int    code     = args.argc() > 2 ? Integer.parseInt(args.argv(2)) : 0 ;

       PoolV2Mode mode = new PoolV2Mode() ;

        switch (enable) {
        case "disable":

            int modeBits = PoolV2Mode.DISABLED;
            if (args.hasOption("strict")) {
                modeBits |= PoolV2Mode.DISABLED_STRICT;
            }
            if (args.hasOption("stage")) {
                modeBits |= PoolV2Mode.DISABLED_STAGE;
            }
            if (args.hasOption("fetch")) {
                modeBits |= PoolV2Mode.DISABLED_FETCH;
            }
            if (args.hasOption("store")) {
                modeBits |= PoolV2Mode.DISABLED_STORE;
            }
            if (args.hasOption("p2p-client")) {
                modeBits |= PoolV2Mode.DISABLED_P2P_CLIENT;
            }
            if (args.hasOption("p2p-server")) {
                modeBits |= PoolV2Mode.DISABLED_P2P_SERVER;
            }
            if (args.hasOption("rdonly")) {
                modeBits |= PoolV2Mode.DISABLED_RDONLY;
            }

            mode.setMode(modeBits);

            break;
        case "enable":

            break;
        default:
            throw new
                    CommandSyntaxException("Invalid keyword : " + enable);
        }

       StringTokenizer       st     = new StringTokenizer(poolList,",");
       PoolModifyModeMessage modify;
       StringBuilder sb     = new StringBuilder() ;
       sb.append("Sending new pool mode : ").append(mode).append("\n");
       while( st.hasMoreTokens() ){
          String poolName = st.nextToken() ;
          modify = new PoolModifyModeMessage(poolName,mode);
          modify.setStatusInfo(code,message);
          sb.append("  ").append(poolName).append(" -> ") ;
          try{

             modify = (PoolModifyModeMessage)sendObject( poolName , modify ) ;
          }catch(Exception ee ){
             sb.append(ee.getMessage()).append("\n");
             continue ;
          }
          if( modify.getReturnCode() != 0 ){
             sb.append(modify.getErrorObject().toString()).append("\n") ;
             continue ;
          }
          sb.append("OK\n");
       }
       return sb.toString() ;
    }
    public static final String hh_set_deletable = "<pnfsId> # DEBUG for advisory delete (srm)" ;
    public String ac_set_deletable_$_1( Args args ) throws Exception {

       checkPermission( "*.*.*" ) ;

       PnfsId       pnfsId = new PnfsId(args.argv(0));
       StringBuilder sb     = new StringBuilder() ;

       PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , "d", PnfsFlagMessage.FlagOperation.SET ) ;
       pfm.setValue("true");

       try{
          pfm = (PnfsFlagMessage)sendObject( "PnfsManager" , pfm ) ;
       }catch(Exception ee ){
          sb.append("Attempt to set 'd' flag reported an Exception : ")
                  .append(ee);
          sb.append("\n");
          sb.append("Operation aborted\n");
          return sb.toString();
       }
       if( pfm.getReturnCode() != 0 ){
          sb.append("set 'd' flag reported  : ").append(pfm.getErrorObject());
          return sb.toString() ;
       }

       sb.append("Setting 'd' succeeded\n");

       PnfsGetCacheLocationsMessage locations = new PnfsGetCacheLocationsMessage(pnfsId) ;
       try{
          locations = (PnfsGetCacheLocationsMessage)sendObject( "PnfsManager" , locations ) ;
       }catch(Exception ee ){
          sb.append("Attempt to get cache locations reported an Exception : ")
                  .append(ee);
          sb.append("\n");
          sb.append("Operation aborted\n");
          return sb.toString() ;
       }
       if( locations.getReturnCode() != 0 ){
          sb.append("Problem in getting cache location(s) : ")
                  .append(locations.getErrorObject());
          return sb.toString() ;
       }
       List<String> assumedLocations = locations.getCacheLocations() ;
       sb.append("Assumed cache locations : ").append(assumedLocations.toString()).append("\n");

        for (Object assumedLocation : assumedLocations) {
            String poolName = assumedLocation.toString();
            PoolModifyPersistencyMessage p =
                    new PoolModifyPersistencyMessage(poolName, pnfsId, false);

            try {
                p = (PoolModifyPersistencyMessage) sendObject(poolName, p);
            } catch (Exception ee) {
                sb.append("Attempt to contact ").
                        append(poolName).
                        append(" reported an Exception : ").
                        append(ee.toString()).
                        append("\n").
                        append("  Operation continues\n");
                continue;
            }
            if (locations.getReturnCode() != 0) {
                sb.append("Set 'cached' reply from ").
                        append(poolName).
                        append(" : ").
                        append(p.getErrorObject()).
                        append("\n");
            } else {
                sb.append("Set 'cached' OK for ").
                        append(poolName).
                        append("\n");
            }
        }
       return sb.toString() ;

    }
    public static final String hh_flags_ls = "<pnfsId> <key>";
    public Object ac_flags_ls_$_2( Args args ) throws Exception {
       PnfsId pnfsId;
       if( args.argv(0).startsWith( "/pnfs" ) ){

          PnfsMapPathMessage map = new PnfsMapPathMessage( args.argv(0) ) ;

          map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

          if( map.getReturnCode() != 0 ){
             Object o = map.getErrorObject() ;
             if( o instanceof Exception ) {
                 throw (Exception) o;
             } else {
                 throw new Exception(o.toString());
             }
          }

          pnfsId = map.getPnfsId() ;


       }else{
          pnfsId = new PnfsId( args.argv(0) ) ;
       }
       String key    = args.argv(1) ;

       PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , key, PnfsFlagMessage.FlagOperation.GET ) ;

       PnfsFlagMessage result = (PnfsFlagMessage)sendObject( "PnfsManager" , pfm ) ;

       return result.getReturnCode() == 0 ?
              ( key+" -> "+result.getValue()) :
              result.getErrorObject().toString() ;
    }
    public static final String hh_pnfs_map = "<globalPath>" ;
    public String ac_pnfs_map_$_1( Args args )throws Exception {

       if( ! args.argv(0).startsWith( "/pnfs" ) ) {
           throw new
                   IllegalArgumentException("not a global dCache path (/pnfs...)");
       }

       PnfsMapPathMessage map = new PnfsMapPathMessage( args.argv(0) ) ;

       map = (PnfsMapPathMessage)sendObject("PnfsManager" , map ) ;

       if( map.getReturnCode() != 0 ){
          Object o = map.getErrorObject() ;
          if( o instanceof Exception ) {
              throw (Exception) o;
          } else {
              throw new Exception(o.toString());
          }
       }

       return map.getPnfsId().toString() ;

    }

    @Command(name = "\\l", hint = "list cells",
            description = "Lists all matching cells. The argument is interpreted as a glob. If no " +
                          "domain suffix is provided, only well known cells are listed. Otherwise " +
                          "all matching cells in all matching domains are listed.")
    class ListCommand implements Callable<String>
    {
        @Argument(required = false, valueSpec = "CELL[@DOMAIN]",
                usage = "A glob pattern. An empty CELL or DOMAIN string matches any name.")
        String[] pattern = { "*" };

        @Override
        public String call() throws Exception
        {
            return String.join("\n", expandCellPatterns(asList(pattern)));
        }
    }

    @Command(name = "\\c", hint = "connect to cell",
            description = "Connect to new cell. May optionally switch to another user.")
    class ConnectCommand implements Callable<String>
    {
        @Argument(index = 0, valueSpec = "CELL[@DOMAIN]",
                usage = "Well known or fully qualified cell name.")
        String name;

        @Argument(required = false, index = 1,
                usage = "Account to connect with.")
        String user;

        @Override
        public String call() throws Exception
        {
            String oldUser = _user;
            try {
                if (user != null) {
                    if (!user.equals(_authUser) && !user.equals(_user)) {
                        try {
                            checkPermission("system.*.newuser");
                        } catch (AclException acle) {
                            checkPermission("system." + user + ".newuser");
                        }
                    }
                    _user = user;
                }
                checkCdPermission(name);
                _currentPosition = resolve(name);
            } catch (Throwable e) {
                _user = oldUser;
                throw e;
            }
            return "";
        }

        private Position resolve(String cell) throws InterruptedException
        {
            CellPath address = new CellPath(cell);
            try {
                SettableFuture<CellPath> future = SettableFuture.create();
                _cellEndpoint.sendMessage(new CellMessage(address, ADMIN_COMMAND_NOOP),
                                          new CellMessageAnswerable()
                                          {
                                              @Override
                                              public void answerArrived(CellMessage request, CellMessage answer)
                                              {
                                                  future.set(answer.getSourcePath());
                                              }

                                              @Override
                                              public void exceptionArrived(CellMessage request, Exception exception)
                                              {
                                                  future.setException(exception);
                                              }

                                              @Override
                                              public void answerTimedOut(CellMessage request)
                                              {
                                                  future.setException(new NoRouteToCellException("No reply"));
                                              }
                                          }, MoreExecutors.directExecutor(), CD_PROBE_MESSAGE_TIMEOUT_MS);
                CellPath returnPath = future.get();
                if (address.hops() == 1 && address.getCellDomainName().equals("local")) {
                    return new Position(returnPath.getSourceAddress().toString(), returnPath.revert());
                } else {
                    return new Position(cell, returnPath.revert());
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof NoRouteToCellException) {
                    throw new IllegalArgumentException("Cell does not exist.");
                }
                // Some other failure, but apparently the cell exists
                _log.info("Cell probe failed: {}", e.getCause().toString());
                return new Position(cell, address);
            }
        }
    }

    @Command(name = "\\q", hint = "quit")
    class QuitCommand implements Callable<Serializable>
    {
        @Override
        public Serializable call() throws Exception
        {
            throw new CommandExitException("Done", 0);
        }
    }

    @Command(name = "\\?", hint = "display help for shell commands",
            description = "Shows help for shell commands. Commands that begin with a backslash are always " +
                          "accessible, while other commands are only available when not connected to a cell." +
                          "\n\n" +
                          "When invoked with a specific command, detailed help for that " +
                          "command is displayed. When invoked with a partial command or without " +
                          "an argument, a summary of all matching commands is shown.")
    class ShellHelpCommand implements Callable<String>
    {
        @Option(name = "format", usage = "Output format.")
        HelpFormat format = Ansi.isEnabled() ? HelpFormat.ANSI : HelpFormat.PLAIN;

        @Argument(valueSpec = "COMMAND", required = false,
                usage = "Partial or full command for which to show help.")
        String[] command = {};

        @Override
        public String call()
        {
            return getHelp(format, command);
        }
    }

    @Command(name = "\\h", hint = "display help for cell commands",
            description = "Shows help for cell commands." +
                          "\n\n" +
                          "When invoked with a specific command, detailed help for that " +
                          "command is displayed. When invoked with a partial command or without " +
                          "an argument, a summary of all matching commands is shown.")
    class HelpCommand implements Callable<Serializable>
    {
        @Option(name = "format", usage = "Output format.")
        HelpFormat format = Ansi.isEnabled() ? HelpFormat.ANSI : HelpFormat.PLAIN;

        @Argument(valueSpec = "COMMAND", required = false,
                usage = "Partial or full command for which to show help.")
        String[] command = {};

        @Override
        public Serializable call() throws InterruptedException, CommandException, NoRouteToCellException
        {
            if ( _currentPosition == null) {
                return "You are not connected to any cell. Use \\? to display shell commands.";
            } else {
                return sendObject(_currentPosition.remote,
                                  new AuthorizedString(_user, "help -format=" + format + " " + String.join(" ", command)));
            }
        }
    }

    @Command(name = "\\sn", hint = "execute pnfsmanager command", allowAnyOption = true,
            acl = { "cell.*.execute", "cell.PnfsManager.execute" },
            description = "Sends COMMAND to the pnfsmanager service. Use \\sn help for a list of supported commands.")
    class NameSpaceCommand implements Callable<Serializable>
    {
        @Argument
        String[] command;

        @CommandLine
        Args args;

        @Override
        public Serializable call() throws InterruptedException, CommandException, NoRouteToCellException
        {
            return sendObject(_pnfsManager.getDestinationPath(), args.toString());
        }
    }

    @Command(name = "\\sp", hint = "execute poolmanager command", allowAnyOption = true,
            acl = { "cell.*.execute", "cell.PoolManager.execute" },
            description = "Sends COMMAND to the poolmanager service. Use \\sp help for a list of supported commands.")
    class PoolManagerCommand implements Callable<Serializable>
    {
        @Argument
        String[] command;

        @CommandLine
        Args args;

        @Override
        public Serializable call() throws InterruptedException, NoRouteToCellException, CommandException
        {
            return sendObject(_poolManager.getDestinationPath(), args.toString());
        }
    }

    @Command(name = "\\s", hint = "execute command", allowAnyOption = true,
            description = "Sends COMMAND to one or more cells.")
    class SendCommand implements Callable<Serializable>
    {
        @Argument(index = 0, valueSpec = "CELL[@DOMAIN][,CELL[@DOMAIN]]...",
                usage = "List of cell addresses. Wildcards are expanded. An empty CELL or DOMAIN string matches any name.")
        String destination;

        @Argument(index = 1)
        String[] command;

        @CommandLine
        Args args;

        @Override
        public Serializable call()
                throws InterruptedException, ExecutionException, CacheException, AclException,
                CommandException, NoRouteToCellException
        {
            args.shift();
            AuthorizedString command = new AuthorizedString(_user, args.toString());

            /* Special case non-wildcard single cell destinations to avoid the indentation and
             * addition of a cell name header. Makes the command nicer to use in scripts.
             */
            if (!destination.contains(",") && !isExpandable(destination)) {
                return sendObject(destination,  command);
            }

            /* Expand wildcards.
             */
            Map<Boolean, List<String>> expandable =
                    Arrays.stream(destination.split(",")).collect(partitioningBy(UserAdminShell::isExpandable));
            Iterable<String> destinations = concat(expandable.get(false), expandCellPatterns(expandable.get(true)));

            /* Check permissions.
             */
            try {
                checkPermission("cell.*.execute");
            } catch (AclException e) {
                for (String cell : destinations) {
                    checkPermission("cell." + cell + ".execute");
                }
            }

            /* Submit commands. */
            List<Map.Entry<String,ListenableFuture<Serializable>>> futures = new ArrayList<>();
            for (String cell : destinations) {
                futures.add(immutableEntry(cell, _cellStub.send(new CellPath(cell), command, Serializable.class)));
            }

            /* Collect results. */
            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, ListenableFuture<Serializable>> entry : futures) {
                result.append(Ansi.ansi().bold().a(entry.getKey()).boldOff()).append(":");
                try {
                    String reply = Objects.toString(entry.getValue().get(), "");
                    if (reply.isEmpty()) {
                        result.append(Ansi.ansi().fg(GREEN).a(" OK").reset()).append("\n");
                    } else {
                        result.append("\n");
                        for (String s : reply.split("\n")) {
                            result.append("    ").append(s).append("\n");
                        }
                    }
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof NoRouteToCellException) {
                        result.append(Ansi.ansi().fg(RED).a(" Cell is unreachable.").reset()).append("\n");
                    } else {
                        result.append(" ").append(Ansi.ansi().fg(RED).a(cause.getMessage()).reset()).append("\n");
                    }
                } catch (InterruptedException e) {
                    result.append(" ^C\n");

                    /* Cancel all uncompleted tasks. Doesn't actually cancel any requests, but will cause
                     * the remaining uncompleted futures to throw a CancellationException.
                     */
                    for (Map.Entry<String, ListenableFuture<Serializable>> entry2 : futures) {
                        entry2.getValue().cancel(true);
                    }
                } catch (CancellationException e) {
                    result.append(" ^C\n");
                }
            }

            return result.toString();
        }
    }

    private void checkCdPermission( String remoteName ) throws AclException {
       int pos = remoteName.indexOf('-') ;
       String prefix = null ;
       if( pos > 0 ) {
           prefix = remoteName.substring(0, pos);
       }
       try{
           checkPermission("cell.*.execute") ;
       }catch( AclException acle ){
          try{
             checkPermission( "cell."+remoteName+".execute" ) ;
          }catch( AclException acle2 ){
             if( prefix == null ) {
                 throw acle2;
             }
              try {
                  checkPermission("cell." + prefix + "-pools.execute");
              } catch (AclException acle3) {
                  throw new AclException(getUser(), remoteName);
              }
          }
       }
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (buffer.startsWith("\\") || _currentPosition == null) {
            return completeShell(buffer, cursor, candidates);
        }
        return completeRemote(buffer, cursor, candidates);
    }

    private int completeRemote(String buffer, int cursor, List<CharSequence> candidates)
    {
        try {
            if (_completer == null) {
                Object help = executeCommand("help");
                if (help == null) {
                    return -1;
                }
                _completer = new HelpCompleter(String.valueOf(help));
            }
            return _completer.complete(buffer, cursor, candidates);
        } catch (CommandException | NoRouteToCellException e) {
            _log.info("Completion failed: {}", e.toString());
            return -1;
        } catch (InterruptedException e) {
            return -1;
        }
    }

    private int completeConnectCommand(String buffer, int cursor, List<CharSequence> candidates)
    {
        try {
            if (CharMatcher.WHITESPACE.matchesAnyOf(buffer)) {
                return -1;
            }
            candidates.addAll(expandCellPatterns(Collections.singletonList(buffer + "*")));
            if (!buffer.contains("@") && _currentPosition != null) {
                /* Add local cells in the connected domain too. */
                candidates.addAll(
                        getCells(_currentPosition.remote.getCellDomainName(),
                                 toGlobPredicate(buffer + "*")).get());
            }
            return 0;
        } catch (CacheException | ExecutionException e) {
            _log.info("Completion failed: {}", e.toString());
            return -1;
        } catch (InterruptedException e) {
            return -1;
        }
    }

    private int completeSendCommand(String buffer, int cursor, List<CharSequence> candidates)
    {
        Completable destination = new Completable(buffer, cursor, candidates);

        if (!destination.hasArguments()) {
            int lastDestinationStart = destination.value.lastIndexOf(',') + 1;
            String lastDestination = destination.value.substring(lastDestinationStart);

            if (lastDestination.contains(":")) {
                return -1;
            }

            try {
                int i = lastDestination.indexOf('@');
                if (i > -1) {
                    expandCellPatterns(asList(lastDestination + "*")).stream()
                            .map(s -> s.substring(s.indexOf("@") + 1))
                            .forEach(candidates::add);
                    return lastDestinationStart + i + 1;
                } else {
                    /* Complete on well-known and cells local to current domain. */
                    candidates.addAll(expandCellPatterns(asList(lastDestination + "*")));
                    if (!buffer.contains("@") && _currentPosition != null) {
                        candidates.addAll(
                                getCells(_currentPosition.remote.getCellDomainName(),
                                         toGlobPredicate(buffer + "*")).get());
                    }
                    return lastDestinationStart;
                }
            } catch (CacheException | ExecutionException e) {
                _log.info("Completion failed: {}", e.toString());
                return -1;
            } catch (InterruptedException e) {
                return -1;
            }
        } else if (!destination.value.contains(",") && !isExpandable(destination.value)) {
            return destination.completeArguments(createRemoteCompleter(destination.value));
        }
        return -1;
    }

    private int completeShell(String buffer, int cursor, List<CharSequence> candidates)
    {
        Completable command = new Completable(buffer, cursor, candidates);
        if (!command.hasArguments()) {
            return command.complete(SHELL_COMMAND_COMPLETER);
        }

        switch (command.value) {
        case "\\?":
            return command.completeArguments(SHELL_COMMAND_COMPLETER);
        case "\\h":
            if (_currentPosition != null) {
                return command.completeArguments(this::completeRemote);
            }
            break;
        case "\\c":
            return command.completeArguments(this::completeConnectCommand);
        case "\\s":
            return command.completeArguments(this::completeSendCommand);
        case "\\sp":
            return command.completeArguments(POOL_MANAGER_COMPLETER);
        case "\\sn":
            return command.completeArguments(PNFS_MANAGER_COMPLETER);
        }
        return -1;
    }

    private Completer createRemoteCompleter(String cell)
    {
        return (buffer, cursor, candidates) -> completeRemote(cell, buffer, cursor, candidates);
    }

    private int completeRemote(String cell, String buffer, int cursor, List<CharSequence> candidates)
    {
        try {
            Serializable help = sendObject(cell, "help");
            if (help == null) {
                return -1;
            }
            HelpCompleter completer = new HelpCompleter(String.valueOf(help));
            return completer.complete(buffer, cursor, candidates);
        } catch (NoRouteToCellException | CommandException e) {
            _log.info("Completion failed: {}", e.toString());
            return -1;
        } catch (InterruptedException e) {
            return -1;
        }
    }

    public Object executeCommand(String str) throws CommandException, InterruptedException, NoRouteToCellException
    {
       _log.info( "String command (super) "+str ) ;

       if( str.trim().equals("") ) {
           return "";
       }

       Args args = new Args( str ) ;

       if( _currentPosition == null || str.startsWith("\\")) {
           return localCommand( args ) ;
       }else{
           return sendObject( _currentPosition.remote ,  new AuthorizedString(_user,str) ) ;
       }
    }

    private Serializable localCommand( Args args ) throws CommandException
    {
           _log.info("Local command {}", args);
           Object or = command(args);
           if( or == null ) {
               return "";
           }
           String r = or.toString() ;
           if(  r.length() < 1) {
               return "";
           }
           if( r.substring(r.length()-1).equals("\n" ) ) {
               return r;
           } else {
               return r + "\n";
           }


    }

    private Serializable sendObject(String cellPath, Serializable object)
            throws NoRouteToCellException, InterruptedException, CommandException
    {
        return sendObject(new CellPath(cellPath), object);
    }

    private Serializable sendObject(CellPath cellPath, Serializable object)
            throws NoRouteToCellException, InterruptedException, CommandException
    {
       try {
           return _cellStub.send(cellPath, object, Serializable.class).get();
       } catch (ExecutionException e) {
           Throwable cause = e.getCause();
           if (_fullException) {
               return getStackTrace(cause);
           }
           Throwables.propagateIfInstanceOf(cause, Error.class);
           Throwables.propagateIfInstanceOf(cause, NoRouteToCellException.class);
           Throwables.propagateIfInstanceOf(cause, CommandException.class);
           throw new CommandThrowableException(cause.toString(), cause);
       }
    }

    private String getStackTrace(Throwable obj)
    {
        CharArrayWriter ca = new CharArrayWriter();
        obj.printStackTrace(new PrintWriter(ca));
        return ca.toString();
    }

    private static boolean isExpandable(String s)
    {
        return !s.contains(":") && (s.startsWith("@") || s.endsWith("@") || Glob.isGlob(s));
    }

    /**
     * Utility class for completing an input buffer.
     */
    private static class Completable
    {
        final String buffer;
        final String value;
        final String arguments;
        final int argumentPosition;
        final int cursor;
        final List<CharSequence> candidates;

        Completable(String buffer, int cursor, List<CharSequence> candidates)
        {
            int offset = CharMatcher.WHITESPACE.indexIn(buffer);
            if (offset > -1) {
                value = buffer.substring(0, offset);
                int i = CharMatcher.WHITESPACE.negate().indexIn(buffer, offset);
                offset = (i > -1) ? i : buffer.length();
                arguments = buffer.substring(offset);
            } else {
                value = buffer;
                arguments = null;
            }
            this.buffer = buffer;
            this.argumentPosition = offset;
            this.cursor = cursor;
            this.candidates = candidates;
        }

        boolean hasArguments()
        {
            return arguments != null;
        }

        int complete(Completer completer)
        {
            return completer.complete(buffer, cursor, candidates);
        }

        int completeArguments(Completer completer)
        {
            int i = completer.complete(arguments, cursor - argumentPosition, candidates);
            return (i == -1) ? -1 : i + argumentPosition;
        }
    }
}
