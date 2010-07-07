// $Id: LoginBroker.java,v 1.6 2007-10-18 04:28:58 behrmann Exp $

package dmg.cells.services.login ;

import java.util.* ;
import dmg.cells.nucleus.*;
import dmg.util.* ;

public class LoginBroker
       extends  CellAdapter
       implements Runnable
{

    private int _delay = 3;

    /**
     * Map from identifiers to entries.
     */
    private Map<String, LoginEntry> _hash =
        new HashMap<String,LoginEntry>();

    /**
     * Set of identifiers of disabled entries.
     */
    private Set<String> _disabled =
        new HashSet<String>();

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
    public LoginBroker(String name, String argString) throws Exception
    {
        super(name, argString, false);

        getNucleus().newThread(this,"Cleaner").start();
        start();
    }

  public CellVersion getCellVersion(){
      return new CellVersion(super.getCellVersion().getRelease(),"$Revision: 1.6 $" );
  }
    public String hh_ls = "[-binary] [-protocol=<protocol_1,...,protocol_n>] [-time]";
    public Object ac_ls(Args args)
    {
        boolean   binary   = args.getOpt("binary") != null;
        String    protocols= args.getOpt("protocol");
        ArrayList list     = new ArrayList();
        StringBuffer sb    = new StringBuffer();
        boolean   showTime = args.getOpt("l") != null;
        Set<String> protocolSet = null;

        if(protocols != null){
            protocolSet = new HashSet();
            for(String protocol: protocols.split(","))
                protocolSet.add(protocol);
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
                    if (!disabled) {
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

    public String hh_disable = "<door> ...";
    public synchronized String ac_disable_$_1_99(Args args)
    {
        for (int i = 0; i < args.argc(); i++) {
            _disabled.add(args.argv(i));
        }
        return "";
    }

    public String hh_enable = "<door> ...";
    public synchronized String ac_enable_$_1_99(Args args)
    {
        for (int i = 0; i < args.argc(); i++) {
            _disabled.remove(args.argv(i));
        }
        return "";
    }

  public void run(){
     try{
        while( ! Thread.interrupted() ){
          synchronized(this){
             HashMap set = new HashMap() ;

             Iterator i = _hash.values().iterator() ;
             while( i.hasNext() ){
                LoginEntry entry = (LoginEntry)i.next() ;
                if( entry.isValid() )set.put(entry.getIdentifier(),entry) ;
             }
             _hash = set ;
          }
          Thread.sleep(60000);
        }
     }catch(Exception ee ){
        say("Worker interrupted due to : "+ee ) ;
     }
  }
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
