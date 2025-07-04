package diskCacheV111.admin;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SpreadAndWait;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.AclException;
import dmg.util.AuthorizedString;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import java.io.CharArrayWriter;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import jline.console.completer.Completer;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Args;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Christian Bernardt, Patrick Fuhrmann
 * @version 0.2, 10 December 2010
 */
public class LegacyAdminShell
      extends CommandInterpreter
      implements Completer {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(LegacyAdminShell.class);

    private static final String ADMIN_COMMAND_NOOP = "xyzzy";
    private static final int CD_PROBE_MESSAGE_TIMEOUT_MS = 1000;

    private final CellEndpoint cellEndPoint;
    private final CellStub _acmStub;
    private final CellStub _poolManager;
    private final CellStub _pnfsManager;
    private final CellStub _cellStub;
    private String _user;
    private final String _authUser;
    private long _timeout = TimeUnit.SECONDS.toMillis(10);
    private boolean _fullException;
    private final String _instance;
    private Position _currentPosition = new Position();
    private final boolean _debug = false;
    private Completer _completer;

    private static class Position {

        private CellPath remote;
        private String remoteName;
        private boolean hyperMode;
        private List<String> hyperPath = new ArrayList<>();
        private String moduleName;

        private Position() {
        }

        private Position(Position position) {
            remote = position.remote;
            remoteName = position.remoteName;
            hyperMode = position.hyperMode;
            hyperPath = new ArrayList<>(position.hyperPath);
            moduleName = position.moduleName;
        }

        private Position(String removeCell) {
            hyperMode = false;
            remoteName = removeCell;
            remote = remoteName == null ? null : new CellPath(remoteName);
        }

        private void clearHyperMode() {
            hyperMode = false;
            remoteName = null;
            remote = null;
            hyperPath = new ArrayList<>();
            moduleName = null;
        }

        private void gotoLocal() {
            remoteName = null;
            remote = null;
            hyperPath = new ArrayList<>();
            moduleName = null;
        }

        private String getPrefix() {
            if ((hyperPath == null) || (hyperPath.size() < 3)) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 2, n = hyperPath.size(); i < n; i++) {
                sb.append(hyperPath.get(i)).append(" ");
            }
            return sb.toString();
        }

        private void finish() {
            if (!hyperMode) {
                return;
            }

            int size = hyperPath.size();

            String domainName = size > 0 ? hyperPath.get(0) : null;
            String cellName = size > 1 ? hyperPath.get(1) : null;

            moduleName = size > 2 ? hyperPath.get(2) : null;

            if (domainName == null) {

                remoteName = null;

            } else if (cellName == null) {

                remoteName = domainName.equals("*") ?
                      "topo" : "System@" + domainName;

            } else {
                remoteName = domainName.equals("*") ?
                      cellName : cellName + "@" + domainName;

            }

            remote = remoteName == null ? null : new CellPath(remoteName);

        }

        private void mergePath(Path path) {
            hyperMode = true;
            String[] pathString = path.getPath();

            if (path.isAbsolutePath()) {
                hyperPath = new ArrayList<>();
                Collections.addAll(hyperPath, pathString);
            } else {

                for (String pathElement : pathString) {
                    switch (pathElement) {
                        case ".":
                            break;
                        case "..":
                            int currentSize = hyperPath.size();
                            if (currentSize == 0) {
                                continue;
                            }
                            hyperPath.remove(currentSize - 1);
                            break;
                        default:
                            hyperPath.add(pathElement);
                            break;
                    }
                }
            }

            finish();

        }
    }

    private class Path {

        private final String _pathString;
        private boolean _isAbsolutePath;
        private boolean _isPath;
        private boolean _isDomain;

        private final String[] _path;

        private Path(String pathString) {
            _pathString = pathString;
            if (_pathString.indexOf('@') > -1) {
                _isDomain = true;
                StringTokenizer st = new StringTokenizer(pathString, "@");
                _path = new String[2];
                _path[0] = st.nextToken();
                _path[1] = st.nextToken();
            } else if (_pathString.indexOf('/') > -1) {
                _isPath = true;
                _isAbsolutePath = _pathString.startsWith("/");
                StringTokenizer st = new StringTokenizer(_pathString, "/");
                int count = st.countTokens();
                _path = new String[count];
                for (int i = 0; i < _path.length; i++) {
                    _path[i] = st.nextToken();
                }
            } else {
                _path = new String[1];
                _path[0] = _pathString;
            }
        }

        private boolean isAbsolutePath() {
            return _isAbsolutePath;
        }

        private boolean isDomain() {
            return _isDomain;
        }

        private boolean isPath() {
            return _isPath;
        }

        private String[] getPath() {
            return _path;
        }

        @Override
        public String toString() {
            return _pathString;
        }
    }

    public LegacyAdminShell(String user, CellEndpoint cellEndpoint, String prompt) {
        cellEndPoint = cellEndpoint;
        _user = user;
        _authUser = user;

        _acmStub = new CellStub(cellEndpoint, new CellPath("acm"));
        _poolManager = new CellStub(cellEndpoint, new CellPath("PoolManager"));
        _pnfsManager = new CellStub(cellEndpoint, new CellPath("PnfsManager"), 30000, MILLISECONDS);
        _cellStub = new CellStub(cellEndpoint);

        _instance = prompt;

        addCommandListener(new HelpCommands());
    }

    protected String getUser() {
        return _user;
    }

    public void checkPermission(String aclName)
          throws AclException {

        Object[] request = new Object[5];
        request[0] = "request";
        request[1] = "<nobody>";
        request[2] = "check-permission";
        request[3] = getUser();
        request[4] = aclName;
        Object[] r;
        try {
            r = _acmStub.sendAndWait(request, Object[].class, _timeout);
        } catch (NoRouteToCellException | TimeoutCacheException e) {
            throw new AclException(e.getMessage());
        } catch (CacheException | InterruptedException e) {
            throw new AclException("Problem: " + e.getMessage());
        }
        if (r.length < 6 || !(r[5] instanceof Boolean)) {
            throw new AclException("Protocol violation 4456");
        }

        if (!((Boolean) r[5])) {
            throw new AclException(getUser(), aclName);
        }
    }

    public String getHello() {
        return "\n    dCache Admin (VII) (user=" + getUser() + ")\n\n";
    }

    public String getPrompt() {
        if (_currentPosition.hyperMode) {
            StringBuilder sb = new StringBuilder();

            sb.append("(").append(getUser()).append(") ");
            if (_debug) {
                String remote =
                      _currentPosition.remoteName == null ? "local" : _currentPosition.remoteName;
                sb.append("[").append(remote).append("] ");
            }
            sb.append(_instance == null ? "/" : ("/" + _instance));
            for (Object pathElement : _currentPosition.hyperPath) {
                sb.append("/").append(pathElement.toString());
            }
            sb.append(" > ");
            return sb.toString();
        } else {
            return (_instance == null ? "" : ("[" + _instance + "] ")) +
                  (_currentPosition.remote == null ? "(local) "
                        : ("(" + _currentPosition.remoteName + ") ")) +
                  getUser() + " > ";
        }
    }

    public Object ac_logoff(Args args) throws CommandException {
        throw new CommandExitException("Done", 0);
    }

    public static final String hh_su = "<userName>";

    public String ac_su_$_1(Args args) throws Exception {
        String user = args.argv(0);
        if (user.equals(_authUser)) {
            _user = _authUser;
            return "User changed BACK to " + _user;
        } else if (user.equals(_user)) {
            return "User not changed, still " + _user;
        }
        try {
            checkPermission("system.*.newuser");
        } catch (AclException acle) {
            checkPermission("system." + user + ".newuser");
        }
        _user = user;
        return "User changed to " + _user;
    }

    public static final String hh_set_exception = "message|detail";

    public String ac_set_exception_$_0_1(Args args) throws CommandException {
        if (args.argc() > 0) {
            if (args.argv(0).equals("message")) {
                _fullException = false;
            } else if (args.argv(0).equals("detail")) {
                _fullException = true;
            } else {
                throw new
                      CommandSyntaxException("set exception message|detail");
            }
        }
        return "Exception = " + (_fullException ? "detail" : "message");
    }

    public static final String hh_set_timeout = "<timeout/sec> # command timeout in seconds";

    public String ac_set_timeout_$_0_1(Args args) {
        if (args.argc() > 0) {
            long timeout = Integer.parseInt(args.argv(0)) * 1000L;
            if (timeout < 1000L) {
                throw new
                      IllegalArgumentException("<timeout> >= 1");
            }
            _timeout = timeout;
        }
        return "Timeout = " + (_timeout / 1000L);

    }

    public static final String hh_set_sticky = "<pnfsId>|<globalPath> [-target=<target>] [-silent]";

    public Object ac_set_sticky_$_1(Args args) throws Exception {
        return setSticky(
              args.argv(0),
              args.getOpt("target"),
              true,
              !args.hasOption("silent") ? new StringBuffer() : null);
    }

    public static final String hh_set_unsticky = "<pnfsId>|<globalPath> [-target=<target>] [-silent]";

    public Object ac_set_unsticky_$_1(Args args) throws Exception {
        return setSticky(
              args.argv(0),
              args.getOpt("target"),
              false,
              !args.hasOption("silent") ? new StringBuffer() : null);
    }

    public static final String hh_uncache = "<pnfsId>|<globalPath> [-target=<target>] [-silent]";

    public Object ac_uncache_$_1(Args args) throws Exception {
        try {
            return uncache(
                  args.argv(0),
                  args.getOpt("target"),
                  !args.hasOption("silent") ? new StringBuffer() : null);
        } catch (Exception ee) {
            ee.printStackTrace();
            throw ee;
        }
    }


    public static final String fh_repinfoof =
          "repinfoof <pnfsId> | <globalPath> # lists info the status of a file by pnfsid or by path.\n"
                +
                "The information includes pools on which the file has been stored (info provided by \"cacheinfoof\" in the PnfsManager cell)\n"
                +
                "and the repository info of the file (info provided by \"rep ls\" in the pool cell).\n";

    public static final String hh_repinfoof = "<pnfsId> | <globalPath>";

    public String ac_repinfoof_$_1(Args args)
          throws CacheException, SerializationException, NoRouteToCellException, InterruptedException, CommandException {
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
          throws CacheException, SerializationException, NoRouteToCellException, InterruptedException, CommandException {
        Set<FileAttribute> request = EnumSet.of(FileAttribute.LOCATIONS, FileAttribute.PNFSID);
        PnfsGetFileAttributes msg;

        if (PnfsId.isValid(fileIdentifier)) {
            PnfsId pnfsId = new PnfsId(fileIdentifier);
            msg = new PnfsGetFileAttributes(pnfsId, request);
        } else {
            msg = new PnfsGetFileAttributes(fileIdentifier, request);
        }

        PnfsGetFileAttributes replyFileLocations = (PnfsGetFileAttributes) sendObject("PnfsManager",
              msg);

        if (replyFileLocations == null) {
            throw new CacheException("Request to the PnfsManager timed out");
        }

        if (replyFileLocations.getReturnCode() != 0) {
            throw CacheExceptionFactory.exceptionOf(replyFileLocations);
        }

        return replyFileLocations.getFileAttributes();
    }

    private Map<CellPath, String> askPoolsForRepLs(FileAttributes fileAttributes, PnfsId pnfsId) {

        SpreadAndWait<String> spreader = new SpreadAndWait<>(
              new CellStub(cellEndPoint, null, _timeout));

        for (String poolName : fileAttributes.getLocations()) {
            spreader.send(new CellPath(poolName), String.class, "rep ls " + pnfsId);
        }

        try {
            spreader.waitForReplies();
        } catch (InterruptedException ex) {
            LOGGER.info("InterruptedException while waiting for a reply from pools {}",
                  ex.toString());
        }

        return spreader.getReplies();
    }

    private String setSticky(
          String destination,
          String target,
          boolean mode,
          StringBuffer sb)
          throws Exception {

        if (Strings.isNullOrEmpty(target)) {
            target = "*";
        }

        boolean verbose = sb != null;

        PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage();
        if (PnfsId.isValid(destination)) {
            pnfsMessage.setPnfsId( new PnfsId(destination));
        } else {
            pnfsMessage.setPnfsPath(destination);
        }

        pnfsMessage = (PnfsGetCacheLocationsMessage) sendObject("PnfsManager", pnfsMessage);
        if (pnfsMessage.getReturnCode() != 0) {
            throw new
                  FileNotFoundException(destination);
        }

        List<String> list = pnfsMessage.getCacheLocations();
        if (verbose) {
            sb.append("Location(s) : ");
            for (String location : list) {
                sb.append(location).append(",");
            }
            sb.append("\n");
        }
        if (target.equals("*")) {
            if (verbose) {
                sb.append("Selection : <all>\n");
            }
        } else if (list.contains(target)) {
            if (verbose) {
                sb.append("Selection : ").append(target).append("\n");
            }
            list = new ArrayList<>();
            list.add(target);
        } else {
            if (verbose) {
                sb.append("Selection : <nothing>\n");
            }
            return sb == null ? "" : sb.toString();
        }
        PoolSetStickyMessage sticky;

        for (String poolName : list) {
            if (verbose) {
                sb.append(poolName).append(" : ");
            }
            try {
                sticky = new PoolSetStickyMessage(poolName, pnfsMessage.getPnfsId(), mode);
                sticky = (PoolSetStickyMessage) sendObject(poolName, sticky);
                if (verbose) {
                    int rc = sticky.getReturnCode();
                    if (rc != 0) {
                        sb.append("[").append(rc).append("] ").
                              append(sticky.getErrorObject().toString());
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

        return sb == null ? "" : sb.toString();
    }

    private String uncache(String destination, String target, StringBuffer sb)
          throws Exception {

        if ((target == null) || (target.isEmpty())) {
            target = "*";
        }

        boolean verbose = sb != null;

        PnfsId pnfsId;
        if (destination.startsWith("/pnfs")) {

            PnfsGetFileAttributes map = new PnfsGetFileAttributes(destination,
                  EnumSet.of(FileAttribute.PNFSID));
            map.setFollowSymlink(false);

            map = (PnfsGetFileAttributes) sendObject("PnfsManager", map);

            if (map.getReturnCode() != 0) {
                Object o = map.getErrorObject();
                if (o instanceof Exception) {
                    throw (Exception) o;
                } else {
                    throw new Exception(o.toString());
                }
            }

            pnfsId = map.getFileAttributes().getPnfsId();
        } else {
            pnfsId = new PnfsId(destination);
        }

        checkPermission("pool.*.uncache");

        PnfsGetCacheLocationsMessage pnfsMessage =
              new PnfsGetCacheLocationsMessage(pnfsId);

        pnfsMessage = (PnfsGetCacheLocationsMessage) sendObject("PnfsManager", pnfsMessage);

        if (pnfsMessage.getReturnCode() != 0) {
            throw new
                  FileNotFoundException(destination);
        }

        List<String> locations = pnfsMessage.getCacheLocations();
        if (verbose) {
            sb.append("Location(s) : ");
            for (Object location : locations) {
                sb.append(location.toString()).append(",");
            }
            sb.append("\n");
        }
        if (target.equals("*")) {
            if (verbose) {
                sb.append("Selection : <all>\n");
            }
        } else if (locations.contains(target)) {
            if (verbose) {
                sb.append("Selection : ").append(target).append("\n");
            }
            locations = new ArrayList<>();
            locations.add(target);
        } else {
            if (verbose) {
                sb.append("Selection : <nothing>\n");
            }
            return sb == null ? "" : sb.toString();
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

        return sb == null ? "" : sb.toString();
    }

    public static final String hh_p2p = "<pnfsId> [<sourcePool> <destinationPool>] [-ip=<address]";

    public String ac_p2p_$_1_3(Args args)
          throws CacheException, InterruptedException, NoRouteToCellException {

        if (args.argc() >= 3) {
            String source = args.argv(1);
            String dest = args.argv(2);
            PnfsId pnfsId = new PnfsId(args.argv(0));

            Pool2PoolTransferMsg p2p = new Pool2PoolTransferMsg(source, dest,
                  FileAttributes.ofPnfsId(pnfsId));

            cellEndPoint.sendMessage(
                  new CellMessage(new CellPath(dest), p2p)
            );

            return "P2p of " + pnfsId + " initiated from " + source + " to " + dest;
        } else {
            PnfsId pnfsId = new PnfsId(args.argv(0));
            String ip = args.getOpt("ip");

            PnfsGetFileAttributes fileAttributesMsg =
                  new PnfsGetFileAttributes(pnfsId,
                        PoolMgrReplicateFileMsg.getRequiredAttributes());

            fileAttributesMsg = _pnfsManager.sendAndWait(fileAttributesMsg);

            DCapProtocolInfo pinfo =
                  new DCapProtocolInfo("DCap", 0, 0, new InetSocketAddress("localhost", 0));

            String timeoutString = args.getOpt("timeout");
            long timeout = timeoutString != null ?
                  Long.parseLong(timeoutString) * 1000L :
                  60000L;

            PoolMgrReplicateFileMsg select =
                  new PoolMgrReplicateFileMsg(fileAttributesMsg.getFileAttributes(), pinfo);
            select = _poolManager.sendAndWait(select, timeout);
            return "p2p -> " + select.getPool().getName();
        }
    }

    public String fh_modify_poolmode =
          " a) modify poolmode enable <poolname>[,<poolname>...]\n" +
                " b) modify poolmode [OPTIONS] disable <poolname>[,<poolname>...] [<code> [<message>]]\n"
                +
                "      OPTIONS :\n" +
                "        -fetch    #  disallows fetch (transfer to client)\n" +
                "        -stage    #  disallows staging (from HSM)\n" +
                "        -store    #  disallows store (transfer from client)\n" +
                "        -p2p-client\n" +
                "        -rdonly   #  := store,stage,p2p-client\n" +
                "        -strict   #  := disallows everything\n";
    public static final String hh_modify_poolmode =
          "enable|disable <poolname>[,<poolname>...] [<code> [<message>]] [-strict|-stage|-rdonly|-fetch|-store]";

    public String ac_modify_poolmode_$_2_4(Args args) throws Exception {

        checkPermission("*.*.*");

        String enable = args.argv(0);
        String poolList = args.argv(1);
        String message = args.argc() > 3 ? args.argv(3) : null;
        int code = args.argc() > 2 ? Integer.parseInt(args.argv(2)) : 0;

        PoolV2Mode mode = new PoolV2Mode();

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

        StringTokenizer st = new StringTokenizer(poolList, ",");
        PoolModifyModeMessage modify;
        StringBuilder sb = new StringBuilder();
        sb.append("Sending new pool mode : ").append(mode).append("\n");
        while (st.hasMoreTokens()) {
            String poolName = st.nextToken();
            modify = new PoolModifyModeMessage(poolName, mode);
            modify.setStatusInfo(code, message);
            sb.append("  ").append(poolName).append(" -> ");
            try {

                modify = (PoolModifyModeMessage) sendObject(poolName, modify);
            } catch (Exception ee) {
                sb.append(ee.getMessage()).append("\n");
                continue;
            }
            if (modify.getReturnCode() != 0) {
                sb.append(modify.getErrorObject().toString()).append("\n");
                continue;
            }
            sb.append("OK\n");
        }
        return sb.toString();
    }

    public static final String hh_set_deletable = "<pnfsId> # DEBUG for advisory delete (srm)";

    public String ac_set_deletable_$_1(Args args) throws Exception {

        checkPermission("*.*.*");

        PnfsId pnfsId = new PnfsId(args.argv(0));
        StringBuilder sb = new StringBuilder();


        PnfsGetCacheLocationsMessage locations = new PnfsGetCacheLocationsMessage(pnfsId);
        try {
            locations = (PnfsGetCacheLocationsMessage) sendObject("PnfsManager", locations);
        } catch (Exception ee) {
            sb.append("Attempt to get cache locations reported an Exception : ")
                  .append(ee);
            sb.append("\n");
            sb.append("Operation aborted\n");
            return sb.toString();
        }
        if (locations.getReturnCode() != 0) {
            sb.append("Problem in getting cache location(s) : ")
                  .append(locations.getErrorObject());
            return sb.toString();
        }
        List<String> assumedLocations = locations.getCacheLocations();
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
        return sb.toString();

    }

    public static final String hh_pnfs_map = "<globalPath>";

    public String ac_pnfs_map_$_1(Args args) throws Exception {

        if (!args.argv(0).startsWith("/pnfs")) {
            throw new
                  IllegalArgumentException("not a global dCache path (/pnfs...)");
        }

        PnfsGetFileAttributes map = new PnfsGetFileAttributes(args.argv(0),
              EnumSet.of(FileAttribute.PNFSID));
        map.setFollowSymlink(false);

        map = (PnfsGetFileAttributes) sendObject("PnfsManager", map);

        if (map.getReturnCode() != 0) {
            Object o = map.getErrorObject();
            if (o instanceof Exception) {
                throw (Exception) o;
            } else {
                throw new Exception(o.toString());
            }
        }

        return map.getFileAttributes().getPnfsId().toString();

    }

    //
    //   input                        remoteName
    // ----------------------------------------------------------------
    //   /                              null
    //   /*                             null [no really defined]
    //   /<domain>                      System@<domain>
    //   /<domain>/<cell>               <cell>@<domain>
    //   /*/<cells>                     <cell>
    //   /<domain>|*/<cells>/<module>  see above
    //
    public static final String fh_cd =
          "  SYNTAX I :\n" +
                "     cd <cellPath>\n" +
                "          <cellPath> : <cellName>[@<domainName>]\n" +
                "     The cd command will send all subsequent commands to the specified cell.\n" +
                "     Use '..' to get back to local mode\n" +
                "" +
                "  SYNTAX II : \n" +
                "     cd <cellDirectoryPath>\n" +
                "           <cellDirecotryPath> : /<domainName>[/<cellName>[/<moduleName[...]]]" +
                "     The cd command will send all subsequent commands to the specified cell resp. module.\n"
                +
                "     Use the standard unix directory syntax to navigate though the cell/domain realm.\n"
                +
                "     Simple '..' will bring you back to 'SYNTAX I'\n" +
                "       Special paths : " +
                "            /*/<cellName>     : use the * directory for wellknown cells.\n" +
                "            /local            : use the local directory for the local domain\n" +
                "            /<domain>         : if no cell is given, the system cell is selected\n"
                +
                "                                except for the /* director where the topo cell is\n"
                +
                "                                chosen, if available\n" +
                "\n";
    public static final String hh_cd = "<cellPath> | <cellDirectoryPath> # see 'help cd'";

    public String ac_cd_$_1(Args args) throws Exception {

        String remoteCell = args.argv(0);
        Path path = new Path(remoteCell);

        Position newPosition;

        if (path.isDomain()) {
            //
            // switch back do domain mode (hyper mode or not)
            //
            newPosition = new Position(remoteCell);

        } else if (_currentPosition.hyperMode) {
            //
            // we are and stay in hyper mode
            //
            newPosition = new Position(_currentPosition);
            newPosition.mergePath(path);

        } else if (path.isPath()) {

            if (path.isAbsolutePath()) {

                newPosition = new Position(_currentPosition);
                newPosition.mergePath(path);

            } else {
                //
                // not hyper mode, got a path but not an absolute one.
                // so we wouldn't know what to do.
                //
                throw new
                      IllegalArgumentException("Need absolute path to switch to directory mode");
            }

        } else {
            //
            //
            //
            newPosition = new Position(remoteCell);

        }

        if (newPosition.remoteName != null) {
            checkCellExists(newPosition.remote);
            checkCdPermission(newPosition.remoteName);
        }

        synchronized (this) {
            _currentPosition = newPosition;
        }

        return "";
    }

    private void checkCdPermission(String remoteName) throws AclException {
        int pos = remoteName.indexOf('-');
        String prefix = null;
        if (pos > 0) {
            prefix = remoteName.substring(0, pos);
        }
        try {
            checkPermission("cell.*.execute");
        } catch (AclException acle) {
            try {
                checkPermission("cell." + remoteName + ".execute");
            } catch (AclException acle2) {
                if (prefix == null) {
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

    private void checkCellExists(CellPath remoteCell) {
        try {
            _cellStub.sendAndWait(remoteCell, ADMIN_COMMAND_NOOP, Object.class,
                  CD_PROBE_MESSAGE_TIMEOUT_MS);
        } catch (NoRouteToCellException e) {
            throw new IllegalArgumentException("Cannot cd to this cell as it doesn't exist.");
        } catch (CacheException e) {
            // Some other failure, but apparently the cell exists
            LOGGER.info("Cell probe failed: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        try {
            if (_completer == null) {
                Object help = executeCommand("help");
                if (help == null) {
                    return -1;
                }
                _completer = new HelpCompleter(String.valueOf(help));
            }
            return _completer.complete(buffer, cursor, candidates);
        } catch (Exception e) {
            LOGGER.info("Completion failed: {}", e.toString());
            return -1;
        }
    }

    public Object executeCommand(String str)
          throws CommandException, InterruptedException, NoRouteToCellException {
        LOGGER.info("String command (super) {}", str);

        if (str.trim().isEmpty()) {
            return "";
        }

        if (str.equals("..")) {
            _currentPosition.clearHyperMode();
            _currentPosition.gotoLocal();
            return "";
        }

        Args args = new Args(str);

        if (_currentPosition.remote == null) {
            return localCommand(args);
        } else {
            if (_currentPosition.hyperMode) {
                if ((args.argc() == 1) && (args.argv(0).equals("cd"))) {
                    _currentPosition.gotoLocal();
                    return "";
                } else if ((args.argc() > 1) && (args.argv(0).equals("cd"))) {
                    return localCommand(args);
                }
                String prefix = _currentPosition.getPrefix();
                if (!prefix.isEmpty()) {
                    if ((args.argc() >= 1) && (args.argv(0).equals("help"))) {
                        if (args.argc() == 1) {
                            str = "help " + prefix;
                        }
                    } else {
                        str = prefix + " " + str;
                    }
                }
            }
            return sendObject(_currentPosition.remote, new AuthorizedString(_user, str));
        }
    }

    private Object localCommand(Args args) throws CommandException {
        LOGGER.info("Local command {}", args);
        Object or = command(args);
        if (or == null) {
            return "";
        }
        String r = or.toString();
        if (r.length() < 1) {
            return "";
        }
        if (r.substring(r.length() - 1).equals("\n")) {
            return r;
        } else {
            return r + "\n";
        }


    }

    private Object sendObject(String cellPath, Serializable object)
          throws NoRouteToCellException, InterruptedException, CommandException {
        return sendObject(new CellPath(cellPath), object);
    }

    private Object sendObject(CellPath cellPath, Serializable object)
          throws NoRouteToCellException, InterruptedException, CommandException {
        try {
            return _cellStub.send(cellPath, object, Object.class, _timeout).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (_fullException) {
                return getStackTrace(cause);
            }
            Throwables.throwIfInstanceOf(cause, Error.class);
            Throwables.throwIfInstanceOf(cause, NoRouteToCellException.class);
            Throwables.throwIfInstanceOf(cause, CommandException.class);
            throw new CommandThrowableException(cause.toString(), cause);
        }
    }

    protected Object sendCommand(CellPath destination, String command)
          throws InterruptedException, NoRouteToCellException, TimeoutCacheException {

        try {
            Object obj = _cellStub.sendAndWait(destination,
                  new AuthorizedString(_user, command),
                  Object.class,
                  _timeout);
            if (obj instanceof Throwable && _fullException) {
                return getStackTrace((Throwable) obj);
            }
            return obj;
        } catch (TimeoutCacheException e) {
            throw new TimeoutCacheException("Request timed out");
        } catch (CacheException e) {
            if (_fullException) {
                return getStackTrace(e);
            }
            return e;
        }
    }

    private Object getStackTrace(Throwable obj) {
        CharArrayWriter ca = new CharArrayWriter();
        obj.printStackTrace(new PrintWriter(ca));
        return ca.toString();
    }

    public Object executeCommand(CellPath destination, Object str)
          throws InterruptedException, TimeoutCacheException, NoRouteToCellException {
        LOGGER.info("Object command ({}) {}", destination, str);

        return sendCommand(destination, str.toString());
    }
}
