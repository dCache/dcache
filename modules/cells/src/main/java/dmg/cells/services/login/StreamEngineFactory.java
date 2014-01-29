package dmg.cells.services.login;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

import dmg.cells.nucleus.CellNucleus;
import dmg.protocols.ssh.SshServerAuthentication;
import dmg.protocols.ssh.SshStreamEngine;
import dmg.protocols.telnet.TelnetServerAuthentication;
import dmg.protocols.telnet.TelnetStreamEngine;
import dmg.util.DummyStreamEngine;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

/**
 * StreamEngineFactory instantiates the appropriate Streamengine for the given
 * protocol/auth combination
 * @author jans
 */
public abstract class StreamEngineFactory {

    private static Logger _log = LoggerFactory.getLogger(StreamEngineFactory.class);

    public static StreamEngine newStreamEngine(Socket socket, String protocol,
            CellNucleus nucleusForAuth, Args argsForAuth) throws Exception {

        StreamEngine engine = null;

        switch (protocol) {
        case "ssh": {
            SshServerAuthentication auth = new SshSAuth_A(
                    nucleusForAuth, argsForAuth);
            _log.info("Using authentication Module : " + SshSAuth_A.class);
            engine = new SshStreamEngine(socket, auth);
            break;
        }
        case "raw":
            _log.info("No authentication used");
            engine = new DummyStreamEngine(socket);
            break;
        case "telnet": {
            TelnetServerAuthentication auth =
                    new TelnetSAuth_A(
                            nucleusForAuth, argsForAuth);
            _log.info("Using authentication Module : " + TelnetSAuth_A.class);
            engine = new TelnetStreamEngine(socket, auth);
            break;
        }
        default:
            _log.error("can't instantiate corresponding streamengine {}", protocol);
            break;
        }

        return engine;
    }

    public static StreamEngine newStreamEngineWithoutAuth(Socket socket,
            String protocol) throws Exception {

        StreamEngine engine = null;

        switch (protocol) {
        case "ssh": {
            SshServerAuthentication auth = null;
            _log.info("No authentication used");
            engine = new SshStreamEngine(socket, auth);
            break;
        }
        case "raw":
            _log.info("No authentication used");
            engine = new DummyStreamEngine(socket);
            break;
        case "telnet": {
            TelnetServerAuthentication auth = null;
            _log.info("No authentication used");
            engine = new TelnetStreamEngine(socket, auth);
            break;
        }
        default:
            _log.error("can't instantiate corresponding streamengine {}", protocol);
            break;
        }

        return engine;
    }
}
