package gplazma.authz.plugins;

import org.apache.log4j.*;

import java.util.Enumeration;

import gplazma.authz.AuthorizationController;

/**
 * LoggingPlugin.java
 * User: tdh
 * Date: Sep 16, 2008
 * Time: 3:46:33 PM
 */
public abstract class LoggingPlugin extends AuthorizationPlugin {

    static Logger log = Logger.getLogger(LoggingPlugin.class.getName());

    public LoggingPlugin(long authRequestID) {
        super(authRequestID);
    }

    public Logger getLogger() {
        return log;
    }

    public void setLogLevel	(Level level) {
        log.setLevel(level);
    }

    public void setLogLevel	(String level) {
        log.setLevel(Level.toLevel(level));
    }
}
