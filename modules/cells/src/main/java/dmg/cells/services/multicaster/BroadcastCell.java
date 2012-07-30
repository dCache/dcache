/*
 * BroadcastCell.java
 *
 * Created on January 31, 2005, 8:32 AM
 */

package dmg.cells.services.multicaster;
import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  java.util.* ;
import  java.io.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author  patrick
 */
public class BroadcastCell extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(BroadcastCell.class);

   private class Entry {

      private static final int STATIC             =  1 ;
      private static final int CANCEL_ON_FAILURE  =  2 ;
      private static final int EXPIRES            =  4 ;

      private CellPath _destination;
      private String   _trigger;
      private int      _mode        = STATIC ;
      private long     _created     = System.currentTimeMillis() ;
      private long     _expires;
      private long     _used;
      private long     _failed;

      private Entry( CellPath destination , String trigger ){
          _destination = destination ;
          _trigger     = trigger ;
      }
      private void setCancelOnFailure( boolean cancel ){
          if(cancel){
              _mode |= CANCEL_ON_FAILURE ;
              _mode &= ~ STATIC ;
          }
          else{
              _mode &=  ~ CANCEL_ON_FAILURE ;
              if( ( _mode & EXPIRES ) == 0 ) {
                  _mode |= STATIC;
              }
          }
      }
      private void setExpires( long expires ){
          if( expires <= 0L ){
             _expires = 0L ;
             _mode &= ~ EXPIRES ;
             if( ( _mode & CANCEL_ON_FAILURE ) == 0 ) {
                 _mode |= STATIC;
             }
          }else{
             _expires = expires ;
             _mode |= EXPIRES ;
             _mode &= ~ STATIC ;
          }
      }
      private String getTrigger(){ return _trigger ; }
      private CellPath getPath(){ return _destination ; }
      public String toString(){
          StringBuilder sb = new StringBuilder() ;
          sb.append( "[").append(_trigger).
             append(";").append(_destination.toString() ) ;
          sb.append(";(").append(_used).append(",").append(_failed).append(")");
          sb.append(";mode=");
          sb.append( isValid() ? "V" : "X" ) ;
          if( ( _mode & STATIC ) != 0 ) {
              sb.append("S");
          }
          if( ( _mode & CANCEL_ON_FAILURE ) != 0 ) {
              sb.append("C");
          }
          if( ( _mode & EXPIRES ) != 0 ){
                long rest = _expires - System.currentTimeMillis() ;
                rest = ( rest <= 0L ) ? 0L : ( rest / 1000L ) ;
                sb.append("E;ex=").append(rest) ;
          }else{
                sb.append(";");
          }
          sb.append("]");
          return sb.toString() ;
      }
      public boolean isValid(){
         if( ( _mode & STATIC  ) != 0 ) {
             return true;
         }
         if( ( ( _mode & EXPIRES ) != 0 ) &&
             ( _expires < System.currentTimeMillis() ) ) {
             return false;
         }
         return true ;
      }
      public boolean isCancelOnFailure(){ return ( _mode & CANCEL_ON_FAILURE) != 0 ; }
      public boolean equals( Object obj ){

    	  if( ! (obj instanceof Entry) ) {
                  return false;
              }
          Entry other = (Entry)obj ;
          return other._destination.equals(this._destination) &&
                 other._trigger.equals( this._trigger ) ;
      }
      public int hashCode(){
         return (_destination.toString()+_trigger).hashCode();
      }
    }
    private CellNucleus      _nucleus;
    private Args             _args;

    private HashMap _eventClassMap  = new HashMap() ;
    private HashMap _destinationMap = new HashMap() ;
    private boolean _debug;
    private String  _debugMode;
    private long    _received;
    private long    _forwarded;
    private long    _sent;
    private Debugging _debugging    = new Debugging() ;


    /** Creates a new instance of BroadcastCell */
    public BroadcastCell(String name , String args ) {
        super( name , args , false ) ;
        _args    = getArgs() ;
        _nucleus = getNucleus() ;

        _debugMode = _args.getOpt("debug") ;
        if( _debugMode != null ){
            _debug = true ;
            addCommandListener(_debugging);
        }
        export() ;
        start() ;
    }
    public String hh_ls = "" ;
    public String ac_ls( Args args ){
        synchronized( this ){
            StringBuilder sb = new StringBuilder() ;
            Iterator i = _eventClassMap.entrySet().iterator() ;
            while( i.hasNext() ){

                Map.Entry entry = (Map.Entry) i.next() ;
                String key = (String)entry.getKey() ;
                sb.append( key ).append("\n");
                Map map    = (Map)entry.getValue() ;
                Iterator j = map.entrySet().iterator() ;
                while( j.hasNext() ){
                    Map.Entry me = (Map.Entry) j.next() ;
                    CellAddressCore path = (CellAddressCore)me.getKey() ;
                    Entry e = (Entry)me.getValue() ;
                    sb.append("   ").append(path.toString()).
                       append("   ").append(e.toString()).append("\n");
                }
            }
            return sb.toString();
        }
    }
     private class OptionClass {

       private long     expires     = -1 ;
       private boolean  failures;
       private  String  eventClass;
       private  String  destination;

       private OptionClass( Args args ){

            eventClass  = args.argv(0);
            destination = args.argc() > 1 ? args.argv(1) : null ;

            String tmp = args.getOpt("expires") ;
            if( tmp != null ) {
                expires = Long.parseLong(tmp) * 1000L + System
                        .currentTimeMillis();
            }

            tmp = args.getOpt("cancelonfailure") ;
            if( tmp != null ){
                 if( tmp.equals("") ){
                     failures = true ;
                 }else if( tmp.equals("on" ) ){
                     failures = true ;
                 }else if( tmp.equals("off") ){
                     failures = false ;
                 }else{
                     throw new
                     IllegalArgumentException("-cancelonfailure=[on|off]");
                 }
            }
       }

    }
    public String hh_register =
      "<classEvent> <cellPath> [-send] [-expires=<seconds>] [-cancelonfailure=[on|off]]" ;
    public String ac_register_$_2( Args args ) throws Exception {
        try{
        OptionClass options = new OptionClass( args ) ;

        synchronized( this ){
            Entry entry = register( new CellPath( options.destination ) , options.eventClass ) ;
            entry.setCancelOnFailure(options.failures) ;
            if( options.expires  > 0L ) {
                entry.setExpires(options.expires);
            }
        }
        }catch(Exception ee ){
            _log.warn(ee.toString(), ee);
            throw ee ;
        }
        return "" ;
    }
    public String hh_modify =
      "<classEvent> <cellPath> [-expires=<seconds>] [-cancelonfailure=[on|off]]" ;
    public String ac_modify_$_2( Args args ){

        OptionClass options = new OptionClass( args ) ;

        Entry entry;
        synchronized( this ){
            entry = get( new CellPath( options.destination ) , options.eventClass ) ;
            if( entry == null ) {
                throw new
                        IllegalArgumentException("Entry not found");
            }
            entry.setCancelOnFailure(options.failures) ;
            if( options.expires  > 0L ) {
                entry.setExpires(options.expires);
            }
        }
        return entry.toString() ;
    }
    public String hh_unregister = "<classEvent> <cellPath> [-send]" ;
    public String ac_unregister_$_2( Args args )
    {

        OptionClass options = new OptionClass( args ) ;
        Entry e = unregister( new CellPath(options.destination) , options.eventClass ) ;
        return "" ;
    }
    private synchronized Entry get( CellPath destination , String eventClass ){

        CellAddressCore core = destination.getDestinationAddress() ;

        Map map = (Map)_eventClassMap.get( eventClass ) ;
        if(  map == null ) {
            return null;
        }
        return (Entry) map.get( core ) ;
    }
    private synchronized Entry register( CellPath destination , String eventClass ){

        CellAddressCore core = destination.getDestinationAddress() ;

        Entry e = new Entry( destination , eventClass ) ;

        Map map = (Map)_eventClassMap.get( eventClass ) ;
        if(  map == null ){
           _eventClassMap.put( eventClass , map = new HashMap() ) ;
        }else{
           if( map.get( core ) != null ) {
               throw new
                       IllegalArgumentException("Duplicated entry : " + e);
           }
        }
        map.put( core , e ) ;

        map = (Map)_destinationMap.get( core ) ;
        if( map == null ) {
            _destinationMap.put(core, map = new HashMap());
        }
        map.put( eventClass , e ) ;

        return e ;
    }
    private synchronized Entry unregister( CellPath destination , String eventClass ){

        CellAddressCore core = destination.getDestinationAddress() ;

        Map map = (Map)_eventClassMap.get( eventClass ) ;
        if(  map == null ) {
            throw new
                    NoSuchElementException("Not an entry " + core + "/" + eventClass);
        }

        Entry e = (Entry) map.remove( core ) ;
        if( e == null ) {
            throw new
                    NoSuchElementException("Not an entry " + core + "/" + eventClass);
        }

        if( map.size() == 0 ) {
            _eventClassMap.remove(eventClass);
        }


        map = (Map)_destinationMap.get( core ) ;
        if( map == null ) {
            throw new
                    NoSuchElementException("PANIC : inconsitent db : " + core + "/" + eventClass);
        }

        e = (Entry)map.remove( eventClass ) ;
        if( map.size() == 0 ) {
            _destinationMap.remove(core);
        }

        return e ;

    }
    public String hh_send = "[<class>]";
    public String ac_send_$_0_1(Args args ) throws Exception {
        Object obj;
        if( args.argc() == 0 ){
            obj = new ArrayList() ;
        }else{
            Class c = Class.forName( args.argv(0) ) ;
            obj = c.newInstance() ;
        }
        CellMessage msg = new CellMessage(
                            new CellPath("broadcast"),
                            obj   );
        sendMessage(msg);
        return "" ;
    }

    private void handleBroadcastCommandMessage( CellMessage msg , BroadcastCommandMessage command ){
        if( ! ( command instanceof BroadcastEventCommandMessage ) ) {
            return;
        }
        BroadcastEventCommandMessage event = (BroadcastEventCommandMessage)command ;
        try{
            String eventClass = event.getEventClass() ;
            CellPath target   = event.getTarget() ;
            if( target == null ){
                target = (CellPath)msg.getSourcePath().clone() ;
                target.revert() ;
            }
            if( event instanceof BroadcastRegisterMessage ){
                BroadcastRegisterMessage reg = (BroadcastRegisterMessage)event ;
                _log.info("Message register : "+reg);
                synchronized( this ){
                    Entry entry = get( target , eventClass ) ;
                    if( entry == null ) {
                        entry = register(target, eventClass);
                    }

                    if( reg.isCancelOnFailure() ) {
                        entry.setCancelOnFailure(true);
                    }
                    long  expires = reg.getExpires() ;
                    if( expires > 0 ) {
                        entry.setExpires(expires);
                    }
                }
            }else if( event instanceof BroadcastUnregisterMessage ){
                BroadcastUnregisterMessage unreg = (BroadcastUnregisterMessage)event ;
                _log.info("Message unregister : "+unreg);

                unregister( target , eventClass ) ;

            }else{
                throw new
                IllegalArgumentException("Not a valid Broadcast command " +event.getClass());
            }
        }catch(Exception ee ){
            _log.warn("Problem with {"+command+"}"+ee, ee);
            event.setReturnValues(1,ee);
        }
        msg.revertDirection() ;
        try{
            sendMessage(msg);
        }catch(Exception ee ){
            _log.warn("Couldn't reply : "+ee);
        }
    }
    @Override
    public void getInfo(  PrintWriter pw ){
        pw.println( "        CellName : "+getCellName());
        pw.println( "       CellClass : "+this.getClass().getName()) ;
        pw.println( "         Version : $Id: BroadcastCell.java,v 1.8 2006-12-15 11:09:37 tigran Exp $");
        pw.println("     Destinations : "+_destinationMap.size() ) ;
        pw.println("    Event Classes : "+_eventClassMap.size() );
        pw.println(" Packets received : "+_received);
        pw.println("     Packets sent : "+ _sent ) ;
        pw.println("Packets forwarded : "+_forwarded ) ;

    }
    @Override
    public void messageArrived( CellMessage message ){
        _log.info("messageArrived : "+message);
        _received ++ ;
        if( _debug ){
            _debugging.messageArrived( message ) ;
            return ;
        }

        Object obj = message.getMessageObject() ;
        if( obj instanceof BroadcastCommandMessage ){
            handleBroadcastCommandMessage( message , (BroadcastCommandMessage)obj ) ;
            return ;
        }else if( obj instanceof NoRouteToCellException ){
            NoRouteToCellException nrtc = (NoRouteToCellException)obj ;
            handleNoRouteException( nrtc ) ;
            return ;
        }
        //
        // slit incoming object (classes) into subclasses and interfaces.
        //
        ArrayList classList = new ArrayList() ;
        for( Class o = obj.getClass() ; o != null ; ){
            classList.add(o.getName());
            Class [] il = o.getInterfaces() ;
            for( int i = 0 ; i < il.length ; i++){
                classList.add( il[i].getName() ) ;
            }
            o = o.getSuperclass() ;
        }
        _log.info("Message arrived "+obj.getClass().getName());
        Iterator i = classList.iterator() ;
        while( i.hasNext() ){
            String eventClass = i.next().toString() ;
            //_log.info("Checking :  "+eventClass);
            forwardMessage( message , eventClass )  ;
        }
    }
    @Override
    public void messageToForward( CellMessage message ){
        _log.info("FORWARD: "+message);
        _forwarded ++ ;
        Object obj = message.getMessageObject() ;
        if( ( obj != null ) && ( obj instanceof NoRouteToCellException ) ){
            NoRouteToCellException nrtc = (NoRouteToCellException)obj ;
            handleNoRouteException( nrtc ) ;
            return ;
        }
        super.messageToForward(message);
    }
    private synchronized void forwardMessage( CellMessage message , String classEvent ){
        Map map = (Map)_eventClassMap.get(classEvent);
        if( map == null ){
//            _log.info("forwardMessage : Not found in eventClassMap : "+classEvent);
            return ;
        }
        ArrayList list = new ArrayList() ;
        CellPath  dest = message.getDestinationPath() ;
        for( Iterator i = map.entrySet().iterator() ; i.hasNext() ; ){

            Map.Entry mapentry   = (Map.Entry)i.next() ;
            CellPath origin      = (CellPath)dest.clone() ;
            Entry    entry       = (Entry)mapentry.getValue() ;
            if( ! entry.isValid() ){
                list.add(entry);
                continue ;
            }
            entry._used++ ;
            //
            // add the (entry) path to our destination and
            // skip ourself.
            //
            origin.add(entry.getPath());
            origin.next();

            CellMessage msg = new CellMessage( origin , message.getMessageObject() ) ;
            msg.setUOID( message.getUOID() ) ;
            //
            //  make sure a reply will find its way back.
            //
            msg.getSourcePath().add( message.getSourcePath() );
            try{
                _log.info("forwardMessage : "+classEvent+" forwarding to "+origin);
                sendMessage(msg);
                _sent++ ;
            }catch(Exception ee ){
                _log.warn("forwardMessage : FAILED "+classEvent+" forwarding to "+origin+" "+ee);
                if( entry.isCancelOnFailure() ) {
                    list.add(entry);
                }
                entry._failed ++ ;
            }
        }
        unregister(list);

    }
    private void handleNoRouteException( NoRouteToCellException nrtc ){
        CellPath destination = nrtc.getDestinationPath() ;
        _log.warn("NoRouteToCell : "+nrtc);
        //
        // find matching destinations
        //
        ArrayList list = new ArrayList() ;
        synchronized( this ){
            Map map = (Map)_destinationMap.get(destination.getDestinationAddress());
            if( map == null ){
                _log.warn("Exception path not found in map : "+destination);
                return ;
            }
            for( Iterator i = map.values().iterator() ; i.hasNext() ; ){
                Entry e = (Entry)i.next() ;
                if( e.isCancelOnFailure() ){
                    _log.info("Scheduling for cancelation : "+e);
                    list.add(e);
                }
            }
            unregister( list ) ;

        }

    }
    private void unregister( List list ){
        for( Iterator i = list.iterator() ; i.hasNext() ; ){
            Entry e = (Entry)i.next() ;
            try{
                unregister( e.getPath() , e.getTrigger() ) ;
            }catch(NoSuchElementException nse){
                _log.warn("PANIC : Couldn't unregister "+e);
            }
        }
    }

    /*
     *
     **     DEBUG PART
     */
    private class Debugging {
        private void messageArrived( CellMessage message ){
            Object obj = message.getMessageObject() ;
            if( _debugMode.equals("source") ){
                _log.info("MessageObject : "+obj ) ;
            }else if( _debugMode.equals("destination" ) ){
                if( obj instanceof BroadcastCommandMessage ){
                    _log.info("Broadcast Message answer : "+obj ) ;
                    return ;
                }
                _log.info("Replying MessageObject : "+obj ) ;
                message.revertDirection() ;
                try{
                    sendMessage(message);
                }catch(Exception ee){
                    _log.warn("Problems sending : "+message+"("+ee+")");
                }
            }
        }
    }
        public String hh_d_reg   = "<eventClass> [<destination>] [-cancelonfailure] [-expires=<time>]" ;
        public String hh_d_unreg = "<eventClass> [<destination>]" ;
        public String hh_d_send  = "<javaClass> [-destination=<cellName>] [-wait]";

        public String ac_d_reg_$_1_2( Args args ) throws Exception {

            OptionClass options = new OptionClass(args) ;

            CellPath path = options.destination == null ? null : new CellPath(options.destination);
            BroadcastRegisterMessage cmd = new BroadcastRegisterMessage(options.eventClass,path);
            cmd.setCancelOnFailure(options.failures);
            cmd.setExpires(options.expires);

            CellMessage msg = new CellMessage( new CellPath("broadcast"), cmd ) ;

            sendMessage(msg);

            return "" ;
        }
        public String ac_d_unreg_$_1_2( Args args ) throws Exception {

            OptionClass options = new OptionClass(args) ;

            CellPath path = options.destination == null ? null : new CellPath(options.destination);
            BroadcastUnregisterMessage cmd = new BroadcastUnregisterMessage(options.eventClass,path);

            CellMessage msg = new CellMessage( new CellPath("broadcast"), cmd ) ;

            sendMessage(msg);

            return "" ;
        }
        public String ac_d_send_$_0_1( Args args ) throws Exception {

             Object obj = args.argc() == 0 ?
                          new ArrayList()  :
                          Class.forName( args.argv(0) ).newInstance() ;

             String dest = args.getOpt("destination") ;

             CellMessage msg = new CellMessage(
                               new CellPath(dest==null?"broadcast":dest),
                               obj   );
              sendMessage(msg);
              return "" ;

        }


}
