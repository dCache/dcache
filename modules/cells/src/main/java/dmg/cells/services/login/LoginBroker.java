// $Id: LoginBroker.java,v 1.6 2007-10-18 04:28:58 behrmann Exp $

package dmg.cells.services.login ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellVersion;

import org.dcache.util.Args;

import static java.util.Arrays.asList;

public class LoginBroker
       extends  CellAdapter
       implements Runnable
{
    private final static Logger _log =
        LoggerFactory.getLogger(LoginBroker.class);

    private int _delay = 3;

    /**
     * Map from identifiers to entries.
     */
    private Map<String, LoginEntry> _hash =
        new HashMap<>();

    /**
     * Set of identifiers of disabled entries.
     */
    private Set<String> _disabled =
        new HashSet<>();

  private class LoginEntry {
     private long _time ;
     private LoginBrokerInfo _info ;
     private LoginEntry( LoginBrokerInfo info ){
        _time = System.currentTimeMillis() ;
        _info = info ;
     }
     private boolean isValid(){
        return System.currentTimeMillis() <
               ( _time + (long)_delay * _info.getUpdateTime() ) ;
     }
     public LoginBrokerInfo getInfo(){ return _info ; }
     public boolean equals( Object obj ){
        return (obj instanceof LoginEntry) && _info.equals( ((LoginEntry)obj).getInfo() ) ;
     }
     public int hashCode(){ return _info.hashCode() ; }
     public String toString(){
        return _info.toString()+
              (_time+_info.getUpdateTime()-System.currentTimeMillis())+
              ";"+
              (isValid()?"VALID":"INVALID")+";" ;
     }
     public String getIdentifier(){ return _info.getIdentifier() ; }
  }
    public LoginBroker(String name, String argString)
    {
        super(name, argString, false);

        getNucleus().newThread(this,"Cleaner").start();
        start();
    }

    public static final String hh_ls = "[-binary] [-protocol=<protocol_1,...,protocol_n>] [-time] [-all]";
    public Object ac_ls(Args args)
    {
        boolean   binary   = args.hasOption("binary");
        String    protocols= args.getOpt("protocol");
        Collection<LoginBrokerInfo> list     = new ArrayList<>();
        StringBuilder sb    = new StringBuilder();
        boolean   showTime = args.hasOption("l");
        boolean   showAll = args.hasOption("all");

        Set<String> protocolSet = null;

        if(protocols != null){
            protocolSet = new HashSet<>(asList(protocols.split(",")));
        }

        synchronized (this) {
            for (LoginEntry entry : _hash.values()) {
                LoginBrokerInfo info = entry.getInfo();
                boolean disabled =
                    _disabled.contains(info.getIdentifier())
                    || _disabled.contains(info.getCellName());

                if (protocols != null &&
                    !protocolSet.contains(info.getProtocolFamily())) {
                    continue;
                }

                if (binary) {
                    if (showAll || !disabled) {
                        list.add(info);
                    }
                } else {
                    sb.append(showTime ? entry.toString() : info.toString());
                    if (disabled) {
                        sb.append("disabled;");
                    }
                    sb.append("\n");
                }
            }
        }
        if (binary) {
            return list.toArray(new LoginBrokerInfo[list.size()]);
        } else {
            return sb.toString();
        }
    }

    public static final String hh_disable = "<door> ...";
    public synchronized String ac_disable_$_1_99(Args args)
    {
        for (int i = 0; i < args.argc(); i++) {
            _disabled.add(args.argv(i));
        }
        return "";
    }

    public static final String hh_enable = "<door> ...";
    public synchronized String ac_enable_$_1_99(Args args)
    {
        for (int i = 0; i < args.argc(); i++) {
            _disabled.remove(args.argv(i));
        }
        return "";
    }

  @Override
  public void run(){
     try{
        while( ! Thread.interrupted() ){
          synchronized(this){
             Map<String, LoginEntry> set = new HashMap<>() ;

              for (LoginEntry entry : _hash.values()) {
                  if (entry.isValid()) {
                      set.put(entry.getIdentifier(), entry);
                  }
              }
             _hash = set ;
          }
          Thread.sleep(60000);
        }
     }catch(Exception ee ){
        _log.info("Worker interrupted due to : "+ee ) ;
     }
  }
  @Override
  public void messageArrived( CellMessage msg ){
     Object obj = msg.getMessageObject() ;

     if( obj instanceof LoginBrokerInfo ){
        synchronized(this){
           LoginEntry entry = new LoginEntry((LoginBrokerInfo)obj) ;
           _hash.put( entry.getIdentifier() , entry ) ;
        }
     }
  }


}
