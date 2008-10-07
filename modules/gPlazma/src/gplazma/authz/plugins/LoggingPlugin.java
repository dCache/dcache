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

    static Logger log = Logger.getLogger(LoggingPlugin.class.getSimpleName());
    private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %C{1} authRequestID";
    private PatternLayout loglayout = new PatternLayout(logpattern);

    public LoggingPlugin(long authRequestID) {
        super(authRequestID);
        String authRequestID_str = AuthorizationController.getFormattedAuthRequestID(authRequestID);
        loglayout = new PatternLayout(logpattern + authRequestID_str + " %m%n");
        boolean found_console_appender=false;
        Enumeration appenders = log.getParent().getAllAppenders();//log.getParent().getAllAppenders();
        while(appenders.hasMoreElements()) {
            Appender apnd = (Appender) appenders.nextElement();
            if(apnd instanceof ConsoleAppender) {
                found_console_appender = true;
                apnd.setLayout(loglayout);
                ((ConsoleAppender)apnd).setThreshold(Level.DEBUG);
            }
        }
        if(!found_console_appender) {
            ConsoleAppender apnd = new ConsoleAppender(loglayout);
            apnd.setThreshold(Level.DEBUG);
            log.getParent().addAppender(apnd);
        }
    }

    public Logger getLogger() {
        return log;
    }

    public PatternLayout getLogLayout() {
        return loglayout;
    }

    public void setLogLevel	(Level level) {
        log.setLevel(level);
    }

    public void setLogLevel	(String level) {
        log.setLevel(Level.toLevel(level));
    }

    //public void trace(String s) {
    //    log.trace(authRequestID + " " + s);
    //}
   /*
    public void debug(String s) {
        log.debug(authRequestID + " " + s);
    }

    public void info(String s) {
        log.info(authRequestID + " " + s);
    }

    public void warn(String s) {
        log.warn(authRequestID + " " + s);
    }

    public void error(String s) {
        log.error(authRequestID + " " + s);
    }

    public void fatal(String s) {
        log.fatal(authRequestID + " " + s);
    }
     */
}
