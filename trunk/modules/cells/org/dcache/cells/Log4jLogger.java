/*
 * $Id: Log4jLogger.java,v 1.5 2006-07-11 08:40:23 tigran Exp $
 */
package org.dcache.cells;

import java.util.Dictionary;

import org.apache.log4j.*;

import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;


/**
 * 
 * @author tigran
 *
 * siple interface to use log4j as logging facility
 *
 */

public class Log4jLogger implements dmg.cells.nucleus.CellPrinter {

    
    Logger log = null;
    
    private static boolean _isInitialized = false;
    private static Object _configLock = new Object();
    
    public Log4jLogger( Args args , Dictionary dict ){
        
        synchronized(_configLock) {
            if( ! _isInitialized ) {
                String log4jConfigFile = System.getProperty("log4j.configuration");
                // check for new configuration every minute
                PropertyConfigurator.configureAndWatch(log4jConfigFile, 60*1000);
                _isInitialized = true;
            }
        }
    }
            
    
    public void say(String cellName, String domainName, String cellType, int level, String msg) {
        
        log = Logger.getLogger(cellType);
        
        
        String type = ( level & ( CellNucleus.PRINT_NUCLEUS | CellNucleus.PRINT_ERROR_NUCLEUS ) ) == 0 ?
                "Cell" : "CellNucleus" ;
                              
  		StringBuffer sb = new StringBuffer();
  		sb.append(type).append("(").append(cellName).append("@").append(domainName).append(") : ").append(msg);
        
        
        switch( level & CellNucleus.PRINT_EVERYTHING ) {
            case CellNucleus.PRINT_ERROR_CELL:
            case CellNucleus.PRINT_ERROR_NUCLEUS:
                log.error(sb);
                break;
            case CellNucleus.PRINT_CELL:
            case CellNucleus.PRINT_NUCLEUS:
                log.debug(sb);
                break;
            case CellNucleus.PRINT_FATAL:
                log.fatal(sb);
                break;
        }
                
    }

}
