// $Id: MessageEventTimer.java,v 1.2 2002-02-03 23:27:27 cvs Exp $
package diskCacheV111.util ;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.UOID;
/**
  * This class handles the sending and receiving of
  * messages including a 'timeout' mechnism.
  */
public class MessageEventTimer {

   private TreeMap<Long, Object> _scheduledEvents = new TreeMap<>() ;
   private HashMap<Object, EventEntry> _hash            = new HashMap<>() ;
   private final Object      _lock            = new Object() ;
   private MessageEntry _currentTimeout;
   private CellNucleus  _nucleus;
   private Thread       _loopThread;

   private class MessageEntry extends EventEntry {

      private MessageEntry( MessageTimerEvent eventClass ,
                            CellMessage       cellMessage ,
                            int               eventType ){

         super( eventClass , cellMessage , eventType ) ;
      }
      public void   setUOID( UOID uoid ){ setPrivateKey( uoid ) ; }
      public UOID   getUOID(){ return (UOID) getPrivateKey() ; }
      public CellMessage  getCellMessage(){
          return (CellMessage)getEventObject() ;
      }

   }
   private static class EventEntry {

      protected final MessageTimerEvent _eventClass ;
      protected final Object            _eventObject ;
      protected final int               _eventType ;

      protected Object            _privateKey;
      protected Long              _timerKey;

      private EventEntry( MessageTimerEvent eventClass ,
                          Object            eventObject ,
                          int               eventType ){

          _eventClass  = eventClass ;
          _eventObject = eventObject ;
          _eventType   = eventType ;

      }
      public MessageTimerEvent getEventClass(){  return _eventClass ; }
      public Object            getEventObject(){ return _eventObject ; }
      public int               getEventType(){   return _eventType ; }

      public void   setTimerKey( Long timerKey ){ _timerKey = timerKey ; }
      public Long   getTimerKey(){ return _timerKey ; }

      public void setPrivateKey( Object key ){ _privateKey = key ; }
      public Object getPrivateKey(){ return _privateKey ; }
      //
      // we are only equals if we are the same.
      //
      public boolean equals( Object in ){ return in == this ; }
      public int hashCode() {
          return _eventType;
      }
   }
   /**
     *  Initiates the MessageEventTimer. The CellNucleus
     *  is needed to actually sending the message.
     */
   public MessageEventTimer( CellNucleus nucleus ){
      _nucleus = nucleus ;
   }
   public static final int MESSAGE_ARRIVED   = 0 ;
   public static final int TIMEOUT_EXPIRED   = 1 ;
   public static final int EXCEPTION_ARRIVED = 2 ;
   public static final int TIMER             = 3 ;
   /**
     *  Feeds the timer with new messages.
     */
   public void messageArrived( CellMessage message ){
      synchronized( _lock ){
         scheduleEvent( null , message , MESSAGE_ARRIVED ) ;
      }
   }
   public void send( CellMessage message , long timeout , MessageTimerEvent event ){

      synchronized( _lock ){
         try{

             _nucleus.sendMessage(message, true, true);
             scheduleEvent( event , message , TIMEOUT_EXPIRED , timeout ) ;

         }catch(Exception ee ){
             System.err.println( "Exception in sending : "+ee ) ;
             scheduleEvent( event ,
                            new CellMessage(null,ee) ,
                            MESSAGE_ARRIVED  ) ;
         }
      }
   }
   public void addTimer( Object privateKey ,
                         Object eventObject ,
                         MessageTimerEvent eventClass ,
                         long timeout ){

       EventEntry entry = new EventEntry(eventClass,eventObject,TIMER) ;
       if( privateKey != null ) {
           add(privateKey,
                   timeout + System.currentTimeMillis(), entry);
       } else {
           add(timeout + System.currentTimeMillis(), entry);
       }
   }
   public void reschedule( long timeOffset ) throws IllegalMonitorStateException{
     //
     // it shouldn't ne necessary to lock the stuff here, because
     // 'reschedule' can only be called withing the callback which
     // itself runs in the lock.
     //
     if( Thread.currentThread() != _loopThread ) {
         throw new
                 IllegalMonitorStateException("Not called in callback (loopThread)");
     }

     synchronized( _lock ){
        if( _currentTimeout == null ) {
            throw new
                    IllegalMonitorStateException("Nothing to reschedule");
        }

        _currentTimeout.setTimerKey(timeOffset) ;
        System.out.println("Rescheduling : "+_currentTimeout.getUOID() ) ;
        add( _currentTimeout.getUOID() ,
                timeOffset + System.currentTimeMillis(),
             _currentTimeout ) ;
     }
   }
   public void interrupt(){ _loopThread.interrupt() ; }
   public void loop() throws InterruptedException {
      while( ! Thread.interrupted()  ){
         EventEntry      entry  = null ;
	 CellMessage  [] array  = null ;
	 _loopThread = Thread.currentThread() ;

         synchronized( _lock ){
            if( _scheduledEvents.size()  == 0 ){
                _lock.wait() ;
            }else{
                Long timerValue  = _scheduledEvents.firstKey();
                long timer       = timerValue;
                long now         = System.currentTimeMillis() ;
                Object x         = _scheduledEvents.get(timerValue) ;

                if( ( timer == 0L ) || ( ( timer - now ) <= 0L ) ){
                   //
                   // simple 'direct event' of event time expired
                   //
                   if( x instanceof ArrayList ){
                      //
                      // take the first in the row and check the rest
                      // for the same lastUOID.
                      //
                      ArrayList<EventEntry> a   = (ArrayList<EventEntry>) x ;
		      List<CellMessage> tmp = new ArrayList<>() ;
                      entry = a.remove(0) ;
                      if( entry instanceof MessageEntry ){
                         MessageEntry messageEntry = (MessageEntry)entry ;
		         CellMessage message = messageEntry.getCellMessage() ;
                         tmp.add( message ) ;
                         Iterator<EventEntry> it = a.iterator() ;
		         while( it.hasNext() ){
			    EventEntry  scan = it.next() ;
                            if( scan instanceof MessageEntry ){
                               MessageEntry mscan = (MessageEntry)scan ;
			       CellMessage  cm    = mscan.getCellMessage() ;
			       if( cm.getLastUOID().equals(
                                      message.getLastUOID() ) ){
			          tmp.add( cm ) ;
			          it.remove() ;
			       }
                            }
		         }
		         array = new CellMessage[tmp.size()] ;
                         tmp.toArray( array ) ;
                      }
                      if( a.size() == 0 ) {
                          _scheduledEvents.remove(timerValue);
                      }

                   }else{
                      _scheduledEvents.remove(timerValue) ;
                      entry = (EventEntry) x ;
                      if( entry instanceof MessageEntry ){
                         array = new CellMessage[1] ;
                         array[0] = ((MessageEntry)entry).getCellMessage() ;
                      }
                   }

                }else{
                   //
                   // time not yet expired
                   //
                   _lock.wait( timer - now ) ;
                }
            }
//            System.out.println("Loop found : "+entry ) ;
            if( entry == null ) {
                continue;
            }
            _currentTimeout = null ;

            if( entry instanceof MessageEntry ){
               handleMessageEntries( (MessageEntry)entry , array ) ;
            }else{
               _hash.remove( entry._privateKey ) ;
               entry._eventClass.event( this ,
		                        entry._eventObject ,
			                entry._eventType ) ;
            }


         } // end of synchronized
      }
   }
   private void handleMessageEntries( MessageEntry entry, CellMessage [] array ){
      if( entry.getUOID() == null ){
         //
         // this is a message and not a timeout, so we have
         // to find the corresponding timeout.
         //
         _currentTimeout = (MessageEntry)remove( entry.getCellMessage().getLastUOID() ) ;

         if( _currentTimeout != null ){
            //
            // this is as it should be.
            // we found the timeout and can take
            // the event receiver class from it.
            //
	    _currentTimeout._eventClass.event(
                       this ,
                       array ,
                       entry._eventType ) ;

         }else if( entry._eventClass != null ){
            //
            // uups , no timeout entry found.
            // so this should be one of the faked
            // exception from the send call. If so, it
            // contains an event class (which a real
            // receiving event doesn't).
            //
	    entry._eventClass.event( this ,
		                     array ,
				     entry._eventType ) ;
         }else{
            //
            // A message arrived which has already expired
            //
            System.err.println(
                  "WARNING : MessageEventTimer lost event 1 -> UOID : "+
                   entry.getCellMessage().getLastUOID() ) ;
         }
      }else{
         //
         // this is an original timeout
         // we have to remove it from the hash as well.
         //
         _hash.remove( entry.getUOID() ) ;
         entry._eventClass.event( this ,
		                  array ,
			          entry._eventType ) ;
      }
   }

   public String toString(){ return ""+_hash.size()+"/"+_scheduledEvents.size() ; }
   public boolean removeTimer( Object privateKey ){
      return remove(privateKey) != null ;
   }
   private EventEntry remove( Object privateKey ){
     synchronized( _lock ){
        EventEntry entry = _hash.remove( privateKey );
        if( entry == null ) {
            return null;
        }
        Long       key   = entry.getTimerKey() ;
        Object y = _scheduledEvents.get( key ) ;
        if( y instanceof ArrayList ){
           Collection<?> alist = (ArrayList<?>) y ;
           alist.remove( entry ) ;
           if( alist.size() == 0 ) {
               _scheduledEvents.remove(key);
           }
        }else{
           _scheduledEvents.remove(key) ;
        }
        return entry ;
     }

   }
   private void add( Object privateKey , Long key , EventEntry entry ){
      synchronized( _lock ){
         entry.setPrivateKey( privateKey ) ;
         _hash.put( privateKey , entry ) ;
         add( key , entry ) ;
      }
   }
   private void add( Long key , EventEntry entry ){

       synchronized( _lock ){
          entry.setTimerKey( key ) ;
          Object x = _scheduledEvents.get( key ) ;
          if( x == null ){
              _scheduledEvents.put( key , entry ) ;
          }else if( x instanceof ArrayList ){
              ((ArrayList<Object>)x).add( entry ) ;
          }else{
              _scheduledEvents.remove( key ) ;
              Collection<Object> list = new ArrayList<>() ;
              list.add( x ) ;
              list.add( entry ) ;
              _scheduledEvents.put( key , list ) ;
          }
          _lock.notifyAll() ;
       }
   }
   private void scheduleEvent(
          MessageTimerEvent eventClass ,
          CellMessage       cellMessage ,
          int               eventType ,
          long              timeout ){

       MessageEntry entry = new MessageEntry(eventClass,cellMessage,eventType) ;
       add( cellMessage.getUOID() ,
               timeout + System.currentTimeMillis(), entry ) ;
   }
   private void scheduleEvent(
          MessageTimerEvent eventClass ,
          CellMessage       cellMessage ,
          int               eventType  ){

       MessageEntry entry = new MessageEntry(eventClass,cellMessage,eventType) ;
       add(0L, entry ) ;
       System.out.println("Event added ... type = "+
                          eventType+" UOID="+cellMessage.getUOID()+"/"+
                                             cellMessage.getLastUOID() ) ;
   }
}
