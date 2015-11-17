// $Id: CellCron.java,v 1.8 2006-12-15 10:58:15 tigran Exp $
package dmg.util ;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.TreeSet;

/**
  *  Author : Patrick Fuhrmann
  *  (C) DESY
  */
public class CellCron implements Runnable {

   public static final int NEXT = -1 ;

   private final TreeSet<TimerTask> _list = new TreeSet<>() ;

   public  class TimerTask implements Comparable<TimerTask>  {

      private Long     _time;
      private Calendar _calendar;
      private String   _name;
      private TaskRunnable _runner;

      private TimerTask( int hour , int minute , TaskRunnable runner , String name ){

          _runner   = runner ;
	  _name     = name ;
          _calendar = new GregorianCalendar() ;

	  long start = _calendar.getTime().getTime() ;

          if( hour != NEXT ) {
              _calendar.set(Calendar.HOUR_OF_DAY, hour);
          }
          _calendar.set(Calendar.MINUTE,minute);
          _calendar.set(Calendar.SECOND,0);

	  _time = _calendar.getTime().getTime();
	  if( (_time - start ) < 0L ){
             if( hour == NEXT ){
                _calendar.set(Calendar.HOUR_OF_DAY,_calendar.get(Calendar.HOUR_OF_DAY)+1);
             }else{
                _calendar.set(Calendar.DAY_OF_YEAR,_calendar.get(Calendar.DAY_OF_YEAR)+1);
             }

          }
	  _time = _calendar.getTime().getTime();

      }
      public Calendar getCalendar(){ return _calendar ; }
      private void nextTick(){
         _time = _time + 1;
      }
      @Override
      public int compareTo( TimerTask other ){
         return _time.compareTo( other._time);
      }

      public int hashCode() {
          return 17;
      }
      public boolean equals( Object other ){
          if (other == this) {
              return true;
          }
         return (other instanceof TimerTask) && _time.equals(((TimerTask)other)._time);
      }
      public void repeatTomorrow(){
         tomorrow() ;
	 add(this);
      }
      public void repeatNextHour(){
	 _calendar.set(Calendar.MINUTE,_calendar.get(Calendar.MINUTE)+60);
	 _time = _calendar.getTime().getTime();
	 add(this);
      }
      private void tomorrow(){
	 _calendar.set(Calendar.DAY_OF_YEAR,_calendar.get(Calendar.DAY_OF_YEAR)+1);
	 _time = _calendar.getTime().getTime();

      }
      private long getTime(){
         return _time;
      }
      public String toString(){
         return _name+" "+new Date(_time) ;
      }
   }
   public interface TaskRunnable {
      void run(TimerTask task);
   }
   public TimerTask add( int minutes , TaskRunnable runner , String name ){
       return add( NEXT , minutes , runner , name );
   }
   public TimerTask add( int hour , int minutes , TaskRunnable runner , String name ){
       TimerTask task = new TimerTask( hour , minutes , runner , name ) ;
       add( task ) ;
       return task ;
   }
   public void add( TimerTask task ){
       synchronized( _list ){
           while( _list.contains(task) ) {
               task.nextTick();
           }
	   _list.add(task) ;
	   _list.notifyAll();
       }
   }
   public Iterator<TimerTask> iterator(){
      return new ArrayList<>( _list ).iterator() ;

   }
   @Override
   public void run(){
      try{
         runLoop() ;
      }catch(InterruptedException ie){
      }
   }
   private void runLoop() throws InterruptedException {

      while( ! Thread.interrupted() ){

         synchronized( _list ){


	    if( _list.size() == 0 ){
//                 System.out.println("wakeup : nothing" ) ;
	        _list.wait((5*60*1000));
		continue ;
	    }

            TimerTask task = _list.first() ;
//          System.out.println("wakeup : "+task.toString()  ) ;
	    long diff =  task.getTime() - System.currentTimeMillis() ;

	    if( diff < 15000L ){
	       _list.remove(task);
	       try{
                   task._runner.run(task) ;
	       }catch(Exception ee ){
	           ee.printStackTrace();
	       }
	    }else if( diff > (5*60*1000) ){
	       _list.wait(diff-60000L);
	    }else{
	       _list.wait(diff);
	    }

	 }
      }


   }
}
