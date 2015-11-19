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

   public CellCron(){ this(true);}
   public CellCron(boolean autostart){

       if( autostart ) {
           new Thread(this).start();
       }

   }
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


   public static void main( String [] x ) throws Exception {

        if( x.length < 2 ){
	  System.out.println("Usage : <hour> <minute> [<hour> <minute> [..]] ");
	  System.exit(4);
	}
      CellCron timer = new CellCron() ;


      for( int i = 0 ; i < x.length ; i+=2 ){

         timer.add( Integer.parseInt(x[i]) , Integer.parseInt(x[i+1]) ,
                    new TaskRunnable(){
                       @Override
                       public void run( TimerTask task ){
                           System.out.println("!!! Cron fired "+task);
                           task.repeatNextHour();
                       }
                    } ,
		    "Task-"+x[i]+"-"+x[i+1]
                   ) ;

      }
       while(true){
	   Iterator<TimerTask> i = timer.iterator() ;
	   System.out.println("Now = "+new Date());
	   while( i.hasNext() ){
               System.out.println("    "+i.next().toString());
	   }

           Thread.sleep(10000L);

       }

/*	Calendar calendar = new GregorianCalendar();
	Date trialTime = new Date();
	calendar.setTime(trialTime);
        calendar.set( Calendar.MINUTE , 59 ) ;
        System.out.println("Data : "+calendar.getTime());
	// print out a bunch of interesting things
	System.out.println("ERA: " + calendar.get(Calendar.ERA));
	System.out.println("YEAR: " + calendar.get(Calendar.YEAR));
	System.out.println("MONTH: " + calendar.get(Calendar.MONTH));
	System.out.println("WEEK_OF_YEAR: " + calendar.get(Calendar.WEEK_OF_YEAR));
	System.out.println("WEEK_OF_MONTH: " + calendar.get(Calendar.WEEK_OF_MONTH));
	System.out.println("DATE: " + calendar.get(Calendar.DATE));
	System.out.println("DAY_OF_MONTH: " + calendar.get(Calendar.DAY_OF_MONTH));
	System.out.println("DAY_OF_YEAR: " + calendar.get(Calendar.DAY_OF_YEAR));
	System.out.println("DAY_OF_WEEK: " + calendar.get(Calendar.DAY_OF_WEEK));
	System.out.println("DAY_OF_WEEK_IN_MONTH: "
                	   + calendar.get(Calendar.DAY_OF_WEEK_IN_MONTH));
	System.out.println("AM_PM: " + calendar.get(Calendar.AM_PM));
	System.out.println("HOUR: " + calendar.get(Calendar.HOUR));
	System.out.println("HOUR_OF_DAY: " + calendar.get(Calendar.HOUR_OF_DAY));
	System.out.println("MINUTE: " + calendar.get(Calendar.MINUTE));
	System.out.println("SECOND: " + calendar.get(Calendar.SECOND));
	System.out.println("MILLISECOND: " + calendar.get(Calendar.MILLISECOND));
	System.out.println("ZONE_OFFSET: "
                	   + (calendar.get(Calendar.ZONE_OFFSET)/(60*60*1000)));
	System.out.println("DST_OFFSET: "
                	   + (calendar.get(Calendar.DST_OFFSET)/(60*60*1000)));


        while( true ){
	    System.out.println(" " + calendar.getTime());
	    calendar.set(Calendar.DAY_OF_YEAR,calendar.get(Calendar.DAY_OF_YEAR)+1);
	}
       	if( x.length == 0 ){
	  System.out.println("Usage : <pattern> [<example>]");
	  System.exit(4);
	}

        DateFormat df = new SimpleDateFormat(x[0]) ;

	System.out.println("Current data : "+df.format(new Date()));

	if( x.length > 1 ){
	   Date d = df.parse( x[1] ) ;
	   System.out.println(d.toString());
	}
	*/
   }
}
