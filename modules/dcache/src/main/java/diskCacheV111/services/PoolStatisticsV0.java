// $Id: PoolStatisticsV0.java,v 1.12 2006-12-15 15:45:40 tigran Exp $Cg

package  diskCacheV111.services ;

import java.util.* ;
import java.text.* ;
import java.io.* ;
import java.util.regex.Pattern ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;


import diskCacheV111.poolManager.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *  @Author: Patrick Fuhrmann
  *
  *  PoolStatisticsV0 collects statistical information from the billing cell and
  *  the connected pools and prepares a raw statistics file for further processing.
  *  As an optional second step, PoolStatisticsV0 converts the raw statistics
  *  file into a tree of html pages.
  *
  *
  *    BASIC steps :
  *
  *  Step 1 :
  *     The PoolManager is queried for the name of currently active Pools
  *  Step 2 :
  *     Those pools are queried for the 'per class' statistics information.
  *  Step 3 :
  *     The billing cell is asked for the data flow data per pool and class.
  *  Step 4 :
  *     This information is merged and stored as
  *        lt;year&gt;-&lt;month&gt;-&lt;day&gt;-&lt;hour&gt;.raw and
  *        lt;year&gt;-&lt;month&gt;-&lt;day&gt;-day.raw
  *     in
  *        &lt;dbBase&gt;/&lt;year&gt;/&lt;month&gt;/&lt;day&gt;
  *  Step 5 :
  *     If available, the 'raw' files of today and yesterday are merged
  *     and stored as
  *        lt;year&gt;-&lt;month&gt;-&lt;day&gt;-day.drw
  *     into the same directory.
  *
  *
  *    HTML steps :
  *
  *
  * <base>/<year>/<month>/<day>/class-<class>.html
  * <base>/<year>/<month>/<day>/pool-<pool>.html
  * <base>/<year>/<month>/<day>/pools.html
  * <base>/<year>/<month>/<day>/classes.html
  * <base>/<year>/<month>/<day>/total.drw
  * <base>/<year>/<month>/<day>/index.html
  * <base>/<year>/<month>/<day>/total.raw    #  sum of(total.drw)
  *
  *  updateHtmlMonth
  *  ----------------
  * <base>/<year>/<month>/index.html
  * <base>/<year>/<month>/total.raw   #   sum of float of xx/total.raw and av. of pool of xx/total.raw
  *
  *
  *  first rows are of  pool and storage group
  *
  *     repository at beginning and end of the day
  *
  * 0   bytes on this pool of this storage group of beginning of this day
  * 1   number of files on this pool of this sg of beginning of this day
  * 2   bytes on this pool of this storage group of end of this day
  * 3   number of files on this pool of this sg of end of this day
  *
  *    actions during the day
  *
  * 4   total number of transfer (in and out   client <-> dCache)
  * 5   total number of restores (hsm -> dCache)
  * 6   total number of stores (dCache -> HSM)
  * 7   total number of errors
  *
  * 8  total bytes transferred into the dCache (client -> dCache)
  * 9  total bytes transferred out of the dCache (dCache -> client)
  * 10  total bytes transferred from HSM into the dCache
  * 11  total bytes transferred into the HSM from the dCache
  *
  *
  */
public class PoolStatisticsV0 extends CellAdapter implements CellCron.TaskRunnable {

    private final static Logger _log =
        LoggerFactory.getLogger(PoolStatisticsV0.class);

   private CellNucleus      _nucleus   = null ;
   private Args             _args      = null ;

   /*
    *   Magic spells to get the infos out of the different cells.
    */
   private static final String GET_REP_STATISTICS    = "rep ls -s -binary" ;
   private static final String GET_CELL_INFO         = "xgetcellinfo" ;
   private static final String GET_POOL_STATISTICS   = "get pool statistics" ;
   private static final String RESET_POOL_STATISTICS = "clear pool statistics" ;
   /*
    *  Definition of the counter values from 'billing' cell.
    */
   private static final int YESTERDAY    = 0 ;
   private static final int TODAY        = 2 ;
   private static final int TRANSFER_IN  = 8 ;
   private static final int TRANSFER_OUT = 9 ;
   private static final int RESTORE      = 10 ;
   private static final int STORE        = 11 ;

   private static final String defaultAuthor = "&copy; dCache.org " ;

   private SimpleDateFormat _pathFromDate =
        new SimpleDateFormat("yyyy"+File.separator+"MM"+File.separator+"dd"+File.separator+"yyyy-MM-dd-HH'.raw'");
   private SimpleDateFormat _dayPathFromDate =
        new SimpleDateFormat("yyyy"+File.separator+"MM"+File.separator+"dd"+File.separator+"yyyy-MM-dd-'day.raw'");
   private SimpleDateFormat _dayDiffPathFromDate =
        new SimpleDateFormat("yyyy"+File.separator+"MM"+File.separator+"dd"+File.separator+"yyyy-MM-dd-'day.drw'");


   private SimpleDateFormat _todayHtmlBaseFromDate =
        new SimpleDateFormat("yyyy"+File.separator+"MM"+File.separator+"yyyy-MM-dd-");
   private SimpleDateFormat _htmlPathFromDate =
        new SimpleDateFormat("yyyy"+File.separator+"MM"+File.separator+"dd");

   private SimpleDateFormat _yearOfCalendar     = new SimpleDateFormat("yyyy");
   private SimpleDateFormat _monthOfCalendar    = new SimpleDateFormat("MMM MM yyyy");
   private SimpleDateFormat _dayOfCalendar      = new SimpleDateFormat("MM/dd yyyy (EEE)");
   private SimpleDateFormat _dayOfCalendarKey   = new SimpleDateFormat("EEE-MM/dd-yyyy");

   private CellCron         _cron      = new CellCron(false);
   private Thread           _cronTimer = null ;

   private CellCron.TimerTask _hourly  = null ;

   private File    _dbBase     = null ;
   private File    _htmlBase   = null ;
   private static String  _domainName = "dCache.Unknown.Org" ;
   private Map     _recentPoolStatistics = null ;
   private boolean _createHtmlTree       = true ;
   private String  _images               = "/images" ;
   private static String  _bodyString           = "<body bgcolor=white>" ;

   public PoolStatisticsV0( String name , String args )throws Exception {
      super( name , args , false ) ;
      _args    = getArgs() ;
      _nucleus = getNucleus() ;

      try{

         if( _args.argc() < 1 )
            throw new
            IllegalArgumentException(
               "Usage : ... <baseDirectory> "+
               "[-htmlBase=<htmlBase>|none] [-create] [-images=<images>]");

         _htmlBase = _dbBase    = new File(_args.argv(0)) ;


         String tmp = _args.getOpt("htmlBase") ;
         if( ( tmp != null ) && ( tmp.length() > 0 ) ){
             if( tmp.equals("none") )_createHtmlTree = false ;
             else _htmlBase = new File(tmp) ;
         }
         tmp = _args.getOpt("domain") ;
         if( tmp != null )_domainName = tmp ;

         if( _args.hasOption("create") ){

            if( ! _dbBase.exists() )_dbBase.mkdirs() ;
            if( _createHtmlTree && ! _htmlBase.exists() )_htmlBase.mkdirs() ;

         }else{

            if( ( ! _dbBase.exists() ) ||  ( _createHtmlTree && ! _htmlBase.exists() ) )
               throw new
               IllegalArgumentException( "Either <baseDirectory> or <htmlBase> doesn't exist");

         }
         _cronTimer = _nucleus.newThread( _cron , "Cron" ) ;
         _cronTimer.start() ;

         _hourly = _cron.add( 55 , this , "Hour" ) ;

      }catch(Exception ee){
         start();
         kill() ;
         throw ee ;
      }
      start() ;

   }
   public void getInfo( PrintWriter pw ){
       pw.println("   Cell Name : "+getCellName());
       pw.println("  Cell Class : "+this.getClass().getName() );
       pw.println("     Version : $Revision$");
       pw.println("   Stat Base : "+_dbBase);
       pw.println("   Html Base : "+_htmlBase);
       pw.println("      Images : "+_images);
       pw.println("    Next Run : "+_hourly) ;
   }
   public void messageArrived( CellMessage message ){

      CellPath path        = message.getSourcePath() ;
      String   destination = path.getCellName() ;
      Object   reply       = message.getMessageObject() ;


   }
   private String getNiceDayOfCalendar( Calendar calendar ){
      return _dayOfCalendar.format(calendar.getTime()) ;
   }
   private File getCurrentPath(Calendar calendar){

        return new File( _dbBase , _pathFromDate.format(calendar.getTime()) ) ;

   }
   private File getTodayPath(Calendar calendar){

        return new File( _dbBase , _dayPathFromDate.format(calendar.getTime()) ) ;

   }
   private File getTodayHtmlPath(Calendar calendar){

        return new File( _dbBase , _todayHtmlBaseFromDate.format(calendar.getTime()) ) ;

   }
   private File getTodayDiffPath(Calendar calendar){

        return new File( _dbBase , _dayDiffPathFromDate.format(calendar.getTime()) ) ;

   }
   private File getYesterdayPath(Calendar calendar){

       calendar = (Calendar)calendar.clone() ;

       calendar.set(Calendar.DAY_OF_YEAR,calendar.get(Calendar.DAY_OF_YEAR)-1);

       return getTodayPath( calendar ) ;

   }
   private File getHtmlPath( Calendar calendar ){

        return new File( _htmlBase , _htmlPathFromDate.format(calendar.getTime()) ) ;

   }
   private void copyFile( File from , File to ) throws IOException {

       InputStream in = new FileInputStream(from) ;

       try{

           OutputStream out = new FileOutputStream( to ) ;

           try{

              byte [] buffer = new byte[16*1024] ;

              while( true ){

                 int rc = in.read( buffer , 0 , buffer.length ) ;

                 if( rc <= 0 )break ;

                 out.write( buffer , 0 , rc ) ;
              }

           }finally{
               try{ out.close() ; }catch(Exception iii){}
           }

       }finally{
           try{ in.close() ; }catch(Exception eee){}
       }
   }
   public void createDiffFile( File today , File yesterday , File resultFile ) throws IOException {

       DataStore todayStore = new DataStore( today ) ;

       Map    todayMap        = todayStore.getLinearMap() ;
       String todayTimeString = (String)todayStore.get("timestamp") ;
       long   todayTime       = 0L ;
       try{
          todayTime = todayTimeString == null ? 0L : Long.parseLong(todayTimeString) ;
       }catch( Exception ee ){}

       Map yesterdayMap = new DataStore( yesterday ).getLinearMap() ;

       Map resultMap    = new HashMap() ;

       Iterator i = todayMap.entrySet().iterator() ;

       while( i.hasNext() ){
          Map.Entry entry = (Map.Entry)i.next() ;

          String  key     = (String)entry.getKey() ;
          long [] counter = (long [])entry.getValue() ;

          long [] result  = new long[12] ;

          long [] yesterdayCounter = (long [])yesterdayMap.get( key ) ;
          if( yesterdayCounter == null ){
              result[0] = -1 ;
              result[1] = -1 ;
          }else{
              result[0] = yesterdayCounter[0] ;
              result[1] = yesterdayCounter[1] ;
          }
          for( int j = 0 ; j < 10 ; j++ )result[j+2] = counter[j] ;

          resultMap.put( key , result ) ;
       }

       resultFile.delete();

       PrintWriter pw = new PrintWriter( new FileWriter( resultFile ) ) ;

       try{
           pw.println("#") ;
           Iterator it = todayStore.iterator() ;
           while( it.hasNext() ){
              Map.Entry entry = (Map.Entry)it.next() ;
              pw.println("# "+entry.getKey()+"="+entry.getValue());
           }
           pw.println("#") ;
           it = resultMap.entrySet().iterator() ;
           while( it.hasNext() ){
              Map.Entry entry = (Map.Entry)it.next() ;
              pw.print(entry.getKey());
              long [] c = (long [])entry.getValue() ;
              for( int j = 0 ; j < c.length ; j++ ){
                 pw.print(" ");
                 pw.print(c[j]);
              }
              pw.println("");

           }
       }finally{
           try{ pw.close() ; }catch(Exception eee ){}
       }

   }
   private class HourlyRunner implements Runnable {
       private Calendar _calendar = null ;
       private HourlyRunner( Calendar calendar ){
           _calendar = calendar ;
           _nucleus.newThread(this,"FreeRunner").start();
       }
       public void run(){

          File path   = getCurrentPath( _calendar ) ;
          File parent = path.getParentFile() ;
          if( ! parent.exists() )parent.mkdirs() ;

          try{
              _log.info("Starting hourly run for : "+path);

              createHourlyRawFile( path , _calendar ) ;

              _log.info("Hourly run finished for : "+path);

              File today = getTodayPath( _calendar ) ;
              _log.info("Creating daily file : "+today);

              today.delete() ;
              copyFile( path , today ) ;

              _log.info("Daily file done : "+today);

              File yesterday = getYesterdayPath( _calendar ) ;
              if( yesterday.exists() ){
                 File diffFile  = getTodayDiffPath( _calendar ) ;
                 _log.info("Starting diff run for : "+yesterday);

                 createDiffFile( today , yesterday , diffFile ) ;

                 _log.info("Finishing diff run for : "+diffFile);
              }
              if( _calendar.get(Calendar.HOUR_OF_DAY) == 23 ){
                 resetBillingStatistics() ;
              }

          }catch(Exception ee){
              _log.warn("Exception in full run for : "+path, ee);
              path.delete();
          }

          if( ! _createHtmlTree )return ;
          try{

              _log.info("Creating html tree");
              prepareDailyHtmlFiles(_calendar) ;
              if( _calendar.get(Calendar.HOUR_OF_DAY) == 23 ){
                 updateHtmlMonth(_calendar);
                 updateHtmlYear(_calendar);
                 updateHtmlTop();
              }

          }catch(Exception eee ){
              _log.warn("Exception in creating html tree for : "+path, eee);
           }

       }
   }
   public void run( CellCron.TimerTask task ){

      if( task == _hourly ){

         _log.info("Hourly ticker : "+ new Date() ) ;

         Calendar calendar = (Calendar)task.getCalendar().clone() ;

         new HourlyRunner( calendar ) ;

         task.repeatNextHour();

      }
   }
   private void updateHtmlMonth( Calendar calendar ) throws IOException {
      File    dir  = getHtmlPath(calendar).getParentFile() ;

      File [] list = dir.listFiles( new MonthFileFilter() ) ;

      list = resortFileList( list , -1 ) ;

      long [] counter      = new long[12] ;
      long [] total        = new long[12] ;
      long [] lastInMonth  = new long[12] ;

      BaseStatisticsHtml html = new BaseStatisticsHtml() ;
      html.setSorted(false);
      html.setTitle(_monthOfCalendar.format( calendar.getTime() )) ;
      html.setKeyType("Date") ;
      html.setAuthor(defaultAuthor);
      html.setHeader( new MonthDirectoryHeader(calendar.getTime()));

      for( int i = 0 , n = list.length ; i < n ; i++ ){
          File x = new File( list[i] , "total.raw" ) ;
          if( ! x.exists() )continue ;
          try{
              BufferedReader br = new BufferedReader( new FileReader( x ) ) ;
              try{
                  String line = br.readLine() ;
//                  _log.info("DEBUG : "+i+" "+line);
                  if( ( line == null ) || ( line.length() < 12 ) )continue ;

                  StringTokenizer st = new StringTokenizer(line) ;

                  st.nextToken() ;
                  String key =  _dayOfCalendar.format( new Date( Long.parseLong( st.nextToken() ) ) ) ;
                  for( int  j = 0 ; j < counter.length ; j++ ){
                      counter[j] = Long.parseLong(st.nextToken()) ;
                  }

                  if( i == 0 )System.arraycopy( counter , 0 , lastInMonth , 0 , counter.length  ) ;
                  add( total , counter ) ;
                  html.add( key , list[i].getName()+File.separator+"index.html" , counter ) ;

              }finally{
                 try{ br.close() ; }catch(IOException eee ){}
              }
          }catch(IOException ee ){
              continue ;
          }

      }
      PrintWriter pw = new PrintWriter( new FileWriter( new File( dir , "index.html") ) ) ;
      try{
           html.setPrintWriter(pw) ;
           html.dump() ;
      }finally{
          pw.close() ;
      }

      total[YESTERDAY]   = counter[YESTERDAY] ;
      total[YESTERDAY+1] = counter[YESTERDAY+1] ;
      total[TODAY]       = lastInMonth[TODAY] ;
      total[TODAY+1]     = lastInMonth[TODAY+1] ;

      printTotal( new File( dir , "total.raw") , total, calendar.getTime() ) ;
   }
   private void updateHtmlYear( Calendar calendar ) throws IOException {
      File    dir  = getHtmlPath(calendar).getParentFile().getParentFile() ;
      File [] list = dir.listFiles( new MonthFileFilter() ) ;

      list = resortFileList( list , -1  ) ;

      long [] counter      = new long[12] ;
      long [] total        = new long[12] ;
      long [] lastInMonth  = new long[12] ;

      BaseStatisticsHtml html = new BaseStatisticsHtml() ;
      html.setSorted(false);
      html.setTitle(_yearOfCalendar.format( calendar.getTime() )) ;
      html.setKeyType("Date") ;
      html.setAuthor(defaultAuthor);
      html.setHeader( new YearDirectoryHeader(calendar.getTime()));

      for( int i = 0 , n = list.length ; i < n ; i++ ){
          File x = new File( list[i] , "total.raw" ) ;
          if( ! x.exists() )continue ;
          try{
              BufferedReader br = new BufferedReader( new FileReader( x ) ) ;
              try{
                  String line = br.readLine() ;
                  if( ( line == null ) || ( line.length() < 12 ) )continue ;

                  StringTokenizer st = new StringTokenizer(line) ;

                  st.nextToken() ;
                  String key =  _monthOfCalendar.format( new Date( Long.parseLong( st.nextToken() ) ) ) ;
                  for( int  j = 0 ; j < counter.length ; j++ ){
                      counter[j] = Long.parseLong(st.nextToken()) ;
                  }
                  if( i == 0 )System.arraycopy( counter , 0 , lastInMonth , 0 , counter.length  ) ;
                  add( total , counter ) ;
                  html.add( key , list[i].getName()+File.separator+"index.html" , counter ) ;

              }finally{
                 try{ br.close() ; }catch(IOException eee ){}
              }
          }catch(IOException ee ){
              continue ;
          }

      }
      PrintWriter pw = new PrintWriter( new FileWriter( new File( dir , "index.html") ) ) ;
      try{
           html.setPrintWriter(pw) ;
           html.dump() ;
      }finally{
           pw.close() ;
      }

      total[YESTERDAY]   = counter[YESTERDAY] ;
      total[YESTERDAY+1] = counter[YESTERDAY+1] ;
      total[TODAY]       = lastInMonth[TODAY] ;
      total[TODAY+1]     = lastInMonth[TODAY+1] ;

      printTotal( new File( dir , "total.raw") , total, calendar.getTime() ) ;
   }
   private void updateHtmlTop(  ) throws IOException {
      File    dir  = _htmlBase ;
      File [] list = dir.listFiles( new YearFileFilter() ) ;

      list = resortFileList( list , -1  ) ;

      long [] counter     = new long[12] ;
      long [] total       = new long[12] ;
      long [] lastInYear  = new long[12] ;

      BaseStatisticsHtml html = new BaseStatisticsHtml() ;
      html.setSorted(false);
      html.setTitle("dCache Statistics of dCache Domain : "+_domainName) ;
      html.setKeyType("Year") ;
      html.setAuthor(defaultAuthor);
      html.setHeader( new TopDirectoryHeader());

      for( int i = 0 , n = list.length ; i < n ; i++ ){
          File x = new File( list[i] , "total.raw" ) ;
          if( ! x.exists() )continue ;
          try{
              BufferedReader br = new BufferedReader( new FileReader( x ) ) ;
              try{
                  String line = br.readLine() ;
                  if( ( line == null ) || ( line.length() < 12 ) )continue ;

                  StringTokenizer st = new StringTokenizer(line) ;

                  st.nextToken() ;
                  String key =  _yearOfCalendar.format( new Date( Long.parseLong( st.nextToken() ) ) ) ;
                  for( int  j = 0 ; j < counter.length ; j++ ){
                      counter[j] = Long.parseLong(st.nextToken()) ;
                  }
                  if( i == 0 )System.arraycopy( counter , 0 , lastInYear , 0 , counter.length  ) ;
                  add( total , counter ) ;
                  html.add( key , list[i].getName()+File.separator+"index.html" , counter ) ;

              }finally{
                 try{ br.close() ; }catch(Exception eee ){}
              }
          }catch(IOException ee ){
              continue ;
          }

      }
      PrintWriter pw = new PrintWriter( new FileWriter( new File( dir , "index.html") ) ) ;
      try{
           html.setPrintWriter(pw) ;
           html.dump() ;
      }finally{
           pw.close() ;
      }

      total[YESTERDAY]   = counter[YESTERDAY] ;
      total[YESTERDAY+1] = counter[YESTERDAY+1] ;
      total[TODAY]       = lastInYear[TODAY] ;
      total[TODAY+1]     = lastInYear[TODAY+1] ;

      printTotal( new File( dir , "total.raw") , total, new Date() ) ;
   }
   private File [] resortFileList( File [] list , final int direction ){
      Set<File> sorted = new TreeSet<File>(
                  new Comparator<File>(){
                     public int compare(File f1 , File f2 ){
                        return direction *
                             f1.getName().compareTo( f2.getName() )  ;
                     }
                  }   ) ;
      for( int i = 0 , n = list.length ; i < n ; i++ )sorted.add(list[i]);

      return sorted.toArray( new File[sorted.size()] ) ;
   }
   private static class MonthFileFilter implements FileFilter {
       public boolean accept( File file ){
           return file.isDirectory() && ( file.getName().length() == 2 ) ;
       }
   }
   private static class YearFileFilter implements FileFilter {
       public boolean accept( File file ){
           return file.isDirectory() && ( file.getName().length() == 4 ) ;
       }
   }
   public String hh_set_html_body = "<bodyString> ; eg: \"<body background=/images/bg.svg>\"";
   public String ac_set_html_body_$_1( Args args ){
       _bodyString = args.argv(0);
       return "";
   }
   public String hh_create_html = "[<year> [<month> [[<day>]]]" ;
   public String ac_create_html_$_0_3( Args args )throws IOException {

      if( args.argc() == 3 ){
         int year  = Integer.parseInt( args.argv(0) ) ;
         int month = Integer.parseInt( args.argv(1) ) ;
         int day   = Integer.parseInt( args.argv(2) ) ;

         Calendar calendar = new GregorianCalendar() ;
         calendar.set( Calendar.YEAR         , year ) ;
         calendar.set( Calendar.MONTH        , month  - 1) ;
         calendar.set( Calendar.DAY_OF_MONTH , day ) ;

         prepareDailyHtmlFiles( calendar ) ;
      }else if( args.argc() == 2 ){
         int year  = Integer.parseInt( args.argv(0) ) ;
         int month = Integer.parseInt( args.argv(1) ) ;

         Calendar calendar = new GregorianCalendar() ;
         calendar.set( Calendar.YEAR         , year ) ;
         calendar.set( Calendar.MONTH        , month  - 1) ;
         calendar.set( Calendar.DAY_OF_MONTH , 1 ) ;

         updateHtmlMonth( calendar ) ;
      }else if( args.argc() == 1 ){
         int year  = Integer.parseInt( args.argv(0) ) ;

         Calendar calendar = new GregorianCalendar() ;
         calendar.set( Calendar.YEAR         , year ) ;
         calendar.set( Calendar.MONTH        , 1 ) ;
         calendar.set( Calendar.DAY_OF_MONTH , 1 ) ;

         updateHtmlYear( calendar ) ;
      }else if( args.argc() == 0 ){
         updateHtmlTop() ;
      }
      return "Done" ;
   }
   public String hh_create_stat = "[<outputFileName>]";
   public String ac_create_stat_$_0_1( Args args ) {
      if( args.argc() == 0 ){
         _nucleus.newThread(
            new Runnable(){
               public void run(){
                  try{
                     _log.info("Starting internal Manual run");
                     synchronized( this ){ _recentPoolStatistics = null ; } ;
                     Map map = createStatisticsMap() ;
                     synchronized( this ){ _recentPoolStatistics = map ; } ;
                     _log.info("Finishing internal Manual run");
                  }catch(Exception e){
                     _log.info("Aborting internal Manual run "+e);
                  }
               }
            },
            "internal"
         ).start() ;
         return "Thread started for internal run" ;
      }else{
         final File file = new File( args.argv(0) ) ;
         _nucleus.newThread(
            new Runnable(){
               public void run(){
                  try{
                     _log.info("Starting Manual run for file : "+file);
                     createHourlyRawFile( file , new GregorianCalendar() ) ;
                     _log.info("Finishing Manual run for file : "+file);
                  }catch(Exception e){
                     _log.info("Aborting Manual run for file : "+file+" "+e);
                  }
               }
            },
            file.toString()
         ).start() ;
         return "Thread started for : "+args.argv(0) ;
      }
   }
   private interface Iteratable {
       public void mapEntry( String pool , String className , long [] counters );
   }
   private interface HtmlDrawable {
       public void draw( PrintWriter pw ) ;
   }
   private static class PatternIterator implements Iteratable {
       private Pattern _pattern = null ;
       private StringBuffer _sb = null ;
       private long [] _sum     = new long[16] ;
       private int     _mx      = 0 ;
       private PatternIterator( Pattern pattern ){
          _pattern = pattern ;
          _sb      = new StringBuffer() ;
       }
       public void mapEntry( String poolName , String className , long [] counters ){
          StringBuffer sb = new StringBuffer() ;
          sb.append(poolName+" "+className+" ");
          for( int i = 0 , n =  counters.length ; i < n ; i++ ){
             sb.append(counters[i]);
             sb.append(" ");
          }
          String line = sb.toString() ;
          if( ! _pattern.matcher( line ).matches() )return ;
          _sb.append(line).append("\n");
          for( int i = 0 , n = counters.length , m = _sum.length ; ( i < n ) && ( i < m ) ; i++ ){
             if( _sum[i] > -1 )_sum[i] += counters[i] ;
          }
          _mx = Math.max( _mx , counters.length ) ;
       }
       public long [] getCounters(){
          long [] result = new long[_mx] ;
          System.arraycopy( _sum , 0 , result , 0 , _mx );
          return result ;
       }
       public String toString(){ return _sb.toString() ; }
       public StringBuffer getStringBuffer(){ return _sb; }
   }
   public String hh_show = "[<pattern>]" ;
   public String ac_show_$_0_1(Args args ) {
       Map map = null ;
       synchronized( this ){ map = _recentPoolStatistics ; }
       if( map == null  ){
           return "Pool Statistics not available yet" ;
       }else if( args.argc() == 0 ){
           StringBuffer st = new StringBuffer() ;
           dumpStatistics( map , st ) ;
           return st.toString() ;
       }
       Pattern p = Pattern.compile(args.argv(0));
       PatternIterator pi = new PatternIterator( p ) ;
       dumpStatistics( map , pi ) ;
       StringBuffer sb = pi.getStringBuffer() ;
       long [] counter = (long []) pi.getCounters() ;
       sb.append("* *");
       for( int i = 0 , n = counter.length ; i < n ; i++ )
          sb.append(" ").append(counter[i]);
       return sb.toString();
   }
   private Map doInternalRun() throws InterruptedException , IOException, NoRouteToCellException {
       return mergeStorageClassMaps( getPoolRepositoryStatistics() , getBillingStatistics() ) ;
   }
   private Map createStatisticsMap() throws InterruptedException , IOException, NoRouteToCellException {

      Map pool    = getPoolRepositoryStatistics() ;
      Map billing = getBillingStatistics() ;

      return mergeStorageClassMaps( pool , billing ) ;

   }
   private static class DataStore {
      private Map _data = null ;
      private Map _attributes = new HashMap() ;
      private int _minCount = 64 ;
      private int _maxCount = 0 ;
      public DataStore( Map data ){
          _data = data ;
      }
      public DataStore( File file ) throws IOException {
          load( file ) ;
      }
      public Map getLinearMap(){
          final HashMap map = new HashMap() ;

          dumpStatistics( _data ,

               new Iteratable(){

                   public void mapEntry( String pool , String className , long [] counters ){
                       map.put( pool+" "+className , counters ) ;
                   }

               }
          ) ;

          return map ;
      }
      public Iterator iterator(){ return _attributes.entrySet().iterator() ; }
      public void setTime( Date date ){
         add( "timestamp" , new Long( date.getTime() ) ) ;
         add( "date"      , date ) ;
      }
      public int getMinColumn(){ return _minCount ; }
      public int getMaxColumn(){ return _maxCount ; }
      private void add( String key , Object value ){
          _attributes.put( key , value ) ;
      }
      public Map getMap(){ return _data ; }
      private Object get( String key ){
          return _attributes.get(key);
      }
      public void store( File file ) throws IOException {
         PrintWriter pw = new PrintWriter( new FileWriter( file ) ) ;
         try{
            pw.println("#");
            Iterator it = _attributes.entrySet().iterator() ;
            while( it.hasNext() ){
               Map.Entry entry = (Map.Entry)it.next() ;
               pw.println("# "+entry.getKey()+"="+entry.getValue() );
            }
            pw.println("#");
            dumpStatistics( _data , pw ) ;

         }finally{
            pw.close() ;
         }
      }
      private void scanAttributes( String line  ){
         if( line.length() < 3 )return ;
         line = line.substring(2).trim() ;
         if( line.length() == 0 )return ;
         int indx = line.indexOf('=');
         if( ( indx == -1 ) || ( indx == ( line.length() - 1 ) ) ){
             _attributes.put(line,"");
         }else if( indx == 0 ){
             return ;
         }else{
             _attributes.put( line.substring(0,indx).trim() ,
                              line.substring(indx+1).trim() ) ;
         }
      }
      private void load( File f ) throws IOException {

          BufferedReader br = new BufferedReader(
                                 new FileReader( f ) ) ;

          HashMap map = new HashMap() ;
          try{

              String line = null ;
              while( ( line = br.readLine() ) != null ){

                  if( ( line.length() > 0 )  && ( line.charAt(0) == '#' ) ){

                      scanAttributes( line ) ;
                      continue ;
                  }

                  StringTokenizer st = new StringTokenizer( line ) ;

                  if( ! st.hasMoreTokens() )continue ;
                  String poolName  = st.nextToken() ;
                  if( ! st.hasMoreTokens() )continue ;
                  String className = st.nextToken() ;

                  long [] counter = new long[64] ;
                  int i = 0 ;
                  for( int n = counter.length ; ( i < n ) && st.hasMoreTokens() ; i++ ){
                      counter[i] = Long.parseLong( st.nextToken() ) ;
                  }
                  _minCount = Math.min( _minCount , i ) ;
                  _maxCount = Math.max( _maxCount , i ) ;
                  long [] res = new long[i] ;
                  System.arraycopy( counter , 0 , res , 0 , i ) ;
                  Map classMap = (Map)map.get( poolName ) ;
                  if( classMap == null )map.put( poolName , classMap = new HashMap() ) ;
                  classMap.put( className , res ) ;

              }
              _data = map ;
              return ;

          }finally{
              try{ br.close() ; }catch(Exception ee ){}
          }
      }
   }
   private void createHourlyRawFile( File outputFile , Calendar calendar) throws InterruptedException , Exception {

      if( outputFile.exists() || ! outputFile.getParentFile().canWrite() )
         throw new
         IOException("File exists or directory not writable : "+outputFile);

      DataStore store = new DataStore( createStatisticsMap() ) ;
      Date date = calendar.getTime() ;
      store.setTime( calendar.getTime() ) ;
      store.store( outputFile ) ;

   }
   private static void dumpStatistics( Map result , Iteratable mapentry ){

       Iterator entries = result.entrySet().iterator() ;

       while( entries.hasNext() ){

           Map.Entry entry = (Map.Entry)entries.next() ;

           String poolName = (String)entry.getKey() ;

           Map classes = (Map)entry.getValue() ;

           Iterator classEntries = classes.entrySet().iterator() ;

           while( classEntries.hasNext() ){

               Map.Entry classEntry = (Map.Entry)classEntries.next() ;

               String className = (String)classEntry.getKey() ;

               long [] values = (long [])classEntry.getValue() ;

               mapentry.mapEntry( poolName , className , values ) ;

           }
      }

      return ;
   }
   private void printIndex( File file , String title ) throws IOException {
      PrintWriter pw = new PrintWriter( new FileWriter( file ) ) ;
      try{
          pw.println("<html><head><title>"+title+"</title></head>");
          pw.println(_bodyString);
          pw.print("<center><h2>");
          pw.print(_domainName);
          pw.println("</h2></center>");
          pw.println("<hr>");
          pw.print("<pre>   <a href=\"/docs/statisticsHelp.html\">Help</a>");
          pw.print(  "      <a href=\"/\">Birds Home</a>");
          pw.print(  "      <a href=\"../../../index.html\">Top</a>");
          pw.print(  "      <a href=\"../../index.html\">This Year</a>");
          pw.println("      <a href=\"../index.html\">This Month</a></pre>");
          pw.println("<hr>");
          pw.println("<center><h1>"+title+"</h1></center>");
          pw.println("<center>");
          pw.println("<h3><a href=\"pools.html\">Pools</a></h3>");
          pw.println("<h3><a href=\"classes.html\">Classes</a></h3>");
          pw.println("<h3><a href=\"total.drw\">Raw</a></h3>");
          pw.println("</center>");
          pw.println("</body></html>");
      }finally{
          pw.close() ;
      }
   }
   private void prepareDailyHtmlFiles( Calendar calendar ){

      File diffFile  = getTodayDiffPath( calendar ) ;
      if( ! diffFile.exists() ){
         _log.warn("prepareDailyHtmlFiles : File not found : "+diffFile);
         return ;
      }
      File dir = getHtmlPath( calendar ) ;
      if( ! dir.exists() )dir.mkdirs() ;

      try{

          /*
           * copy the raw file into the html directory
           */
          copyFile( diffFile , new File( dir , "total.drw" ) ) ;
          /*
           * load the raw data file
           */
          Map map = new DataStore( diffFile ).getMap() ;
          /*
           * create todays html files
           */
          prepareDailyHtml( map , dir , calendar.getTime() ) ;
          /*
           * prepare the daily index.html file
           */
          printIndex( new File( dir , "index.html") , getNiceDayOfCalendar(calendar) ) ;

      }catch(IOException ee ){
          _log.warn("Can't prepare Html directory : "+dir, ee ) ;
      }
   }

   private Map [] prepareDailyHtml( Map poolMap , File pathBase , Date date ) throws IOException {

       Iterator entries  = poolMap.entrySet().iterator() ;
       Map poolMap2  = null ;
       Map classMap2 = new HashMap() ;
       DayDirectoryHeader header = new DayDirectoryHeader( date , _dayOfCalendar) ;

       BaseStatisticsHtml allPoolsHtml = new BaseStatisticsHtml() ;
       allPoolsHtml.setSorted(true);
       allPoolsHtml.setTitle("Pools") ;
       allPoolsHtml.setKeyType("PoolNames") ;
       allPoolsHtml.setAuthor(defaultAuthor);
       allPoolsHtml.setHeader(header);

       long [] total = null ;

       while( entries.hasNext() ){

           Map.Entry entry = (Map.Entry)entries.next() ;

           String poolName = (String)entry.getKey() ;

           Map    classMap = (Map)entry.getValue() ;

           Iterator classEntries = classMap.entrySet().iterator() ;

           long [] sum = null ;

           BaseStatisticsHtml html = new BaseStatisticsHtml() ;
           html.setSorted(true);
           html.setTitle("Pool : "+poolName) ;
           html.setKeyType("ClassNames") ;
           html.setAuthor(defaultAuthor);
           html.setHeader(header);
           while( classEntries.hasNext() ){

               Map.Entry classEntry = (Map.Entry)classEntries.next() ;

               String className = (String)classEntry.getKey() ;

               long [] values = (long [])classEntry.getValue() ;

               if( sum == null )sum = new long[values.length] ;
               add( sum , values );
               if( total == null )total = new long[values.length] ;
               add( total , values );
               //
               // poolName , className , values
               //
               poolMap2 = (HashMap)classMap2.get(className) ;
               if( poolMap2 == null )
                   classMap2.put( className , poolMap2 = new HashMap() ) ;

               poolMap2.put( poolName , values ) ;

               //
               // statistics ...
               //
               html.add( className , "class-"+className+".html" , values ) ;
           }

           if( sum != null )allPoolsHtml.add( poolName , "pool-"+poolName+".html" , sum ) ;
           PrintWriter pw = new PrintWriter( new FileWriter( new File( pathBase , "pool-"+poolName+".html" ) ) ) ;
           try{
               html.setPrintWriter(pw);
               html.dump();
           }finally{
               pw.close() ;
           }
      }
      PrintWriter allPw = new PrintWriter( new FileWriter( new File( pathBase , "pools.html" ) ) ) ;
      try{
          allPoolsHtml.setPrintWriter(allPw);
          allPoolsHtml.dump();
      }finally{
          allPw.close() ;
      }

      printTotal( new File( pathBase , "total.raw" ) , total , date ) ;

      BaseStatisticsHtml allClassesHtml = new BaseStatisticsHtml() ;
      allClassesHtml.setSorted(true);
      allClassesHtml.setTitle("Classes") ;
      allClassesHtml.setAuthor(defaultAuthor);
      allClassesHtml.setKeyType("ClassNames") ;
      allClassesHtml.setHeader(header);

      entries = classMap2.entrySet().iterator() ;
      while( entries.hasNext() ){

         Map.Entry entry = (Map.Entry)entries.next() ;

         String className = (String)entry.getKey() ;

         poolMap2  = (Map)entry.getValue() ;
         long [] sum = null ;

         BaseStatisticsHtml html = new BaseStatisticsHtml() ;
         html.setSorted(true);
         html.setTitle("Class "+className) ;
         html.setAuthor(defaultAuthor);
         html.setKeyType("Pools") ;
         html.setHeader(header);

         Iterator poolEntries = poolMap2.entrySet().iterator() ;
         while( poolEntries.hasNext() ){

             Map.Entry poolEntry = (Map.Entry)poolEntries.next() ;

             String poolName = (String)poolEntry.getKey() ;

             long [] values = (long [])poolEntry.getValue() ;

             if( sum == null )sum = new long[values.length] ;
             add( sum , values );

             html.add( poolName , "pool-"+poolName+".html" , values ) ;


         }
         if( sum != null )allClassesHtml.add( className , "class-"+className+".html" , sum ) ;
         PrintWriter pw = new PrintWriter( new FileWriter( new File( pathBase , "class-"+className+".html" ) ) ) ;
         try{
             html.setPrintWriter(pw);
             html.dump();
         }finally{
             pw.close() ;
         }
      }

      allPw = new PrintWriter( new FileWriter( new File( pathBase , "classes.html" ) ) ) ;

      try{
          allClassesHtml.setPrintWriter(allPw);
          allClassesHtml.dump();
      }finally{
          allPw.close() ;
      }


      Map [] result = { poolMap , classMap2 } ;
      return result ;
   }
   private void printTotal( File filename , long [] total , Date date ) throws IOException {
      PrintWriter dayTotal = new PrintWriter( new FileWriter( filename ) ) ;
      try{
          String day = _dayOfCalendarKey.format(date) ;
          dayTotal.print(day);
          dayTotal.print(" ");
          dayTotal.print(date.getTime());
          for( int i = 0 ; i < total.length ; i++ ){
             dayTotal.print(" ");
             dayTotal.print(total[i]) ;
          }
          dayTotal.println("");
      }finally{
          dayTotal.close() ;
      }

   }
   private void add( long [] sum , long [] add ){
      int l = Math.min( sum.length , add.length );
      for( int i = 0 ; i < l ; i++ ){
          if( add[i] > -1 )sum[i] += add[i] ;
      }
   }
   private static void dumpStatistics( Map result , final PrintWriter pw ){
      dumpStatistics( result ,
         new Iteratable(){
           public void mapEntry( String poolName , String className , long [] counters ){
               pw.print(poolName+" "+className+" ");
               for( int i = 0 , n =  counters.length ; i < n ; i++ ){
                  pw.print(counters[i]);
                  pw.print(" ");
               }
               pw.println("");

           }
         }
      );
      pw.flush() ;
   }
   private void dumpStatistics( Map result , final StringBuffer pw ){
      dumpStatistics( result ,
         new Iteratable(){
           public void mapEntry( String poolName , String className , long [] counters ){
               pw.append(poolName+" "+className+" ");
               for( int i = 0 , n =  counters.length ; i < n ; i++ ){
                  pw.append(counters[i]);
                  pw.append(" ");
               }
               pw.append("\n");

           }
         }
      );
   }
   private Map mergeStorageClassMaps( Map fromPools , Map fromBilling ){

       HashMap result = new HashMap() ;

       Iterator pools = fromPools.keySet().iterator() ;

       while( pools.hasNext() ){

          String poolName = (String)pools.next() ;

          Map resultClasses = (Map)result.get(poolName) ;
          if( resultClasses == null )
              result.put( poolName , resultClasses = new HashMap() ) ;

          Map stat = (Map)fromPools.get(poolName) ;
          if( stat == null )continue ;

          Iterator classes = stat.keySet().iterator() ;
          while( classes.hasNext() ){

              String className = (String)classes.next() ;

              long [] values = (long [])stat.get(className) ;
              if( values == null )continue ;

              long [] resultArray = (long [])resultClasses.get(className) ;
              if( resultArray == null ){
                  resultClasses.put( className , resultArray = new long[10] ) ;
                  int preset = fromBilling.get(poolName) == null ? -1 : 0 ;
                  for( int i = 2 ; i < 10 ; i++ )resultArray[i] = preset ;
              }

              for( int i = 0 ; i < 2 ; i ++ )resultArray[i] = values[i] ;

          }
       }

       pools = fromBilling.keySet().iterator() ;

       while( pools.hasNext() ){

          String poolName = (String)pools.next() ;

          Map resultClasses = (Map)result.get(poolName) ;
          if( resultClasses == null )
              result.put( poolName , resultClasses = new HashMap() ) ;

          Map stat = (Map)fromBilling.get(poolName) ;
          if( stat == null )continue ;

          Iterator classes = stat.keySet().iterator() ;
          while( classes.hasNext() ){

              String className = (String)classes.next() ;

              long [] values = (long [])stat.get(className) ;
              if( values == null )continue ;

              long [] resultArray = (long [])resultClasses.get(className) ;
              if( resultArray == null ){
                  resultClasses.put( className , resultArray = new long[10] ) ;
                  int preset = fromPools.get(poolName) == null ? -1 : 0 ;
                  for( int i = 0 ; i < 2 ; i++ )resultArray[i] = preset ;
              }

              for( int i = 2 ; i < 10 ; i ++ )resultArray[i] = values[i-2] ;

          }
       }

       return result ;
   }
   //
   // expected format from 'rep ls -s -binary'
   // Object[*]
   //   Object [2]
   //     0 String <storageClass>
   //     1 long[2]
   //          0  # of bytes in repository
   //          1  # of files in repository
   //
   private Map getPoolRepositoryStatistics()
           throws InterruptedException ,
                  NoRouteToCellException,
                  IOException {

       HashMap map = new HashMap() ;

       CellMessage m =
           new CellMessage( new CellPath("PoolManager") , GET_CELL_INFO ) ;

       _log.info("getPoolRepositoryStatistics : asking PoolManager for cell info");
       m = _nucleus.sendAndWait( m , 20000 ) ;
       if( m == null )
          throw new
          IOException("xgetcellinfo timed out" );

       Object o = m.getMessageObject() ;

       if( ! ( o instanceof PoolManagerCellInfo ) )
          throw new
          IOException( "Illegal Reply from PoolManager : "+o.getClass().getName()) ;

       PoolManagerCellInfo info = (PoolManagerCellInfo)o ;
       _log.info("getPoolRepositoryStatistics :  PoolManager replied : "+info);

       String [] poolList = info.getPoolList() ;

       for( int i = 0 ; i < poolList.length ; i++ ){

          m = new CellMessage( new CellPath(poolList[i]) , GET_REP_STATISTICS ) ;
          try{

             _log.info("getPoolRepositoryStatistics : asking "+poolList[i]+" for statistics");
             m = _nucleus.sendAndWait( m , 20000 ) ;

             if( m == null ){
                _log.warn("getPoolRepositoryStatistics : timeout "+poolList[i] ) ;
                continue ;
             }

             _log.info("getPoolRepositoryStatistics : "+poolList[i]+" replied : "+m);

             Object [] result = (Object [])m.getMessageObject() ;

             HashMap classMap = new HashMap() ;
             for( int j = 0 ; j < result.length ; j++ ){
                 Object [] e = (Object [])result[j] ;
                 classMap.put( e[0] , e[1] ) ;
             }
             _log.info("getPoolRepositoryStatistics : "+poolList[i]+" replied with "+classMap);
             map.put( poolList[i] , classMap ) ;

          }catch(InterruptedException ie ){
             _log.warn("getPoolRepositoryStatistics : sendAndWait interrupted") ;
             throw ie ;
          }catch(NoRouteToCellException eee ){
             _log.warn("getPoolRepositoryStatistics : "+poolList[i]+" : "+eee ) ;
          }
       }

       return map ;

   }
   private void resetBillingStatistics(){
       _log.info("Resetting Billing statistics");
       CellMessage m =
           new CellMessage( new CellPath("billing") , RESET_POOL_STATISTICS ) ;
       try{
           _nucleus.sendMessage( m ) ;
       }catch(NoRouteToCellException ee ){
           _log.warn("Couldn't reset pool statistics : "+ee);
       }
   }
   //
   // structur expected from 'billing' : get pool statistics
   //   map( String <poolName> , long[4] )
   //     0 : # of transfers
   //     1 : # of restore
   //     2 : # of store
   //     3   # of total errors
   //
   // structur expected from 'billing' : get pool statistics <poolName>
   //
   //     storageClassMap( String <storageClass> , long[8] );
   //
   //     0 : # of transfers
   //     1 : # of restore
   //     2 : # of store
   //     3   # of total errors
   //     4 : bytes transferred IN
   //     5 : bytes transferred OUT
   //     6 : bytes restored
   //     7 : bytes stored
   //
   private Map getBillingStatistics()
           throws InterruptedException ,
                  NoRouteToCellException,
                  IOException {

       HashMap map = new HashMap() ;

       CellMessage m =
           new CellMessage( new CellPath("billing") , GET_POOL_STATISTICS ) ;

       _log.info("getBillingStatistics : asking billing for generic pool statistics");
       m = _nucleus.sendAndWait( m , 20000 ) ;
       if( m == null )
          throw new
          IOException("'get pool statistics' timed out" );

       Object o = m.getMessageObject() ;

       if( ! ( o instanceof Map ) )
          throw new
          IOException( "Illegal Reply from billing : "+o.getClass().getName()) ;

       Map generic = (Map)o ;
       _log.info("getBillingStatistics :  billing replied with "+generic);

       Iterator poolNames = generic.keySet().iterator() ;

       for( int i = 0 ; poolNames.hasNext() ; i++ ){

          String poolName = (String) poolNames.next() ;

          m = new CellMessage( new CellPath("billing") , GET_POOL_STATISTICS+" "+poolName ) ;
          try{

             _log.info("getBillingStatistics : asking billing for ["+poolName+"] statistics");
             m = _nucleus.sendAndWait( m , 20000 ) ;

             if( m == null ){
                _log.warn("'get pool statistics' : timed out for "+poolName ) ;
                continue ;
             }

             Map result = (Map)m.getMessageObject() ;
             _log.info("getBillingStatistics :  billing replied with "+result);

             map.put( poolName , result ) ;

          }catch(InterruptedException ie ){
             _log.warn("'get pool statistics' : sendAndWait interrupted") ;
             throw ie ;
          }catch(NoRouteToCellException eee ){
             _log.warn("'get pool statistics' : "+poolName+" : "+eee ) ;
          }
       }

       return map ;

   }
   //
   // HTML stuff
   //
   private void printLegend( PrintWriter pw ){

      String tableHeaderBg = "#007a95" ;
      String tableRowBg    = "white" ;
      String greenBox      = "/images/greenbox.gif" ;
      String redBox        = "/images/greenbox.gif" ;
      String blueBox       = "/images/greenbox.gif" ;
      String yellowBox     = "/images/greenbox.gif" ;
      String navyBox       = "/images/greenbox.gif" ;
      String cyanBox       = "/images/greenbox.gif" ;
      String orangeBox     = "/images/greenbox.gif" ;

      pw.println("<table border=1 cellpadding=0 cellspacing=0 width=\"90%\">");
      pw.println("<tr><th bgcolor=\""+tableHeaderBg+"\"><font color=white>Year</font></th>");
      pw.println("<th bgcolor=\""+tableHeaderBg+"\"><font color=white>Absolute Values</font></th>");
      pw.println("<th bgcolor=\""+tableHeaderBg+"\"><font color=white>Data / GBytes</font></th>");
      pw.println("<th bgcolor=\""+tableHeaderBg+"\"><font color=white>Relative Values</font></th>");
      pw.println("</tr>");
      pw.println("<tr>");
      pw.println("<th rowspan=3 align=center bgcolor=\"white\"><a href=\"2004/index.html\">2004</a>");
      pw.println("</th>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=129 height=10>");
      pw.println("<img src=\""+greenBox+"\" width=370 height=10><img src=\""+greenBox+"\" width=0 height=10></td>");
      pw.println("<td align=center bgcolor=\""+tableRowBg+"\">8310<font color=\"red\">");
      pw.println(" + 23767</font><font color=\"red\"> + 0</font></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=25 height=10");
      pw.println("><img src=\""+greenBox+"\" width=74 height=10><img src=\""+greenBox+"\" width=0 height=10></td>");
      pw.println("</tr>");
      pw.println("<tr>");

      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=174 height=10></td>");
      pw.println("<td align=center bgcolor=\""+tableRowBg+"\">11216</td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=34 height=10></td>");
      pw.println("</tr>");
      pw.println("<tr>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=402 height=10>");
      pw.println("<img src=\""+greenBox+"\" width=0 height=10></td>");
      pw.println("<td align=center bgcolor=\""+tableRowBg+"\">25798 + 0</td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=80 height=10>");
      pw.println("<img src=\""+greenBox+"\" width=0 height=10></td>");
      pw.println("</tr>");
      pw.println("</table>");
      pw.println("</center>");
      pw.println("<p>");
      pw.println("<blockquote>");
      pw.println("<table border=1 cellpadding=4 cellspacing=0>");
      pw.println("<tr>");
      pw.println("<th bgcolor=\""+tableHeaderBg+"\"><font color=white>Type</font></th>");
      pw.println("<th bgcolor=\""+tableHeaderBg+"\"><font color=white>Color</font></th>");
      pw.println("<th bgcolor=\""+tableHeaderBg+"\"><font color=white>Description</font></th>");
      pw.println("<th bgcolor=\""+tableHeaderBg+"\"><font color=white>Color</font></th>");
      pw.println("<th bgcolor=\""+tableHeaderBg+"\"><font color=white>Description</font></th>");
      pw.println("</tr>");
      pw.println("<tr>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>Amount of data in repository</font></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=140 height=10></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>Beginning of interval</font></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=140 height=10></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>End of interval</font></td>");
      pw.println("</tr>");
      pw.println("<tr>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>Data transferred into dCache</font></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=140 height=10></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>from HSM</font></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=140 height=10></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>from client</font></td>");
      pw.println("</tr>");
      pw.println("<tr>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>Data transferred from dCache</font></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=140 height=10></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>to client</font></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><img src=\""+greenBox+"\" width=140 height=10></td>");
      pw.println("<td bgcolor=\""+tableRowBg+"\"><font color=blue>to HSM</font></td>");
      pw.println("</tr>");
      pw.println("</table>");

   }
   public static void main( String [] args )throws Exception {
       /*
       BaseStatisticsHtml html = new BaseStatisticsHtml() ;

       html.setSorted(true) ;
       html.setPrintWriter( new PrintWriter( System.out ) ) ;
       create html 2004 07 13

       long [] counter = { 1000 , 10 , 2000 , 20 ,
                           0,0,0,0,
                           100,200,300,400 } ;

       html.add( "first" , "link:first" , counter ) ;
       html.add( "second" , "link:second" , counter ) ;

       html.dump();
       */

       DataStore store = new DataStore( new File("xxinput") ) ;
       store.store( new File("xxoutput") ) ;
   }
   private static class DayDirectoryHeader implements HtmlDrawable {
       private final Date _date ;
       private final SimpleDateFormat _dayOfCalendar;
       private DayDirectoryHeader( Date date ,SimpleDateFormat  dayOfCalendar){
          _date = date ;
          _dayOfCalendar = dayOfCalendar;
       }
       public void draw( PrintWriter pw ){
          pw.print("<hr><pre>    ");
          pw.print("<a href=\"/docs/statisticsHelp.html\">Help</a>         ");
          pw.print("<a href=\"/\">Birds Home</a>         ");
          pw.print("<a href=\"../../../index.html\">Top</a>         ");
          pw.print("<a href=\"../../index.html\">This Year</a>         ");
          pw.print("<a href=\"../index.html\">This Month</a>         ");
          pw.print("<a href=\"index.html\">Today</a>         ");
          pw.print("<a href=\"pools.html\">Pools</a>         ");
          pw.print("<a href=\"classes.html\">Classes</a>         ") ;
          pw.print("<a href=\"total.drw\">Raw</a>");
          pw.println("</pre><hr>");
          pw.println("<center><h1>"+_dayOfCalendar.format( _date )+"</h1></center>");
       }
   }
   private class MonthDirectoryHeader implements HtmlDrawable {
       private Date _date = null ;
       private MonthDirectoryHeader( Date date ){
          _date = date ;
       }
       public void draw( PrintWriter pw ){
          pw.print("<hr><pre>    ");
          pw.print("<a href=\"/docs/statisticsHelp.html\">Help</a>         ");
          pw.print("<a href=\"/\">Birds Home</a>         ");
          pw.print("<a href=\"../../index.html\">Top</a>         ");
          pw.print("<a href=\"../index.html\">This Year</a>         ");
          pw.println("</pre><hr>");
       }
   }
   private static class YearDirectoryHeader implements HtmlDrawable {
       private Date _date = null ;
       private YearDirectoryHeader( Date date ){
          _date = date ;
       }
       public void draw( PrintWriter pw ){
          pw.print("<hr><pre>    ");
          pw.print("<a href=\"/docs/statisticsHelp.html\">Help</a>         ");
          pw.print("<a href=\"/\">Birds Home</a>         ");
          pw.print("<a href=\"../index.html\">Top</a>         ");
          pw.println("</pre><hr>");
       }
   }
   private static class TopDirectoryHeader implements HtmlDrawable {
       private TopDirectoryHeader(){
       }
       public void draw( PrintWriter pw ){
          pw.print("<hr><pre>    ");
          pw.print("<a href=\"/docs/statisticsHelp.html\">Help</a>         ");
          pw.print("<a href=\"/\">Birds Home</a>         ");
          pw.println("</pre><hr>");
       }
   }
   static private class BaseStatisticsHtml    {

      private int         _height  = 10 ;
      private int         _absoluteWidth   = 500 ;
      private int         _relativeWidth   = 100 ;
      private String  []  _bgcolor = { "white" , "#bebebe" } ;
      private PrintWriter _pw      = null ;
      private String      _imageBase     = "/images/";
      private String      _poolYesterday = _imageBase+"greenbox.gif" ;
      private String      _poolToday     = _imageBase+"redbox.gif" ;
      private String      _restore       = _imageBase+"bluebox.gif" ;
      private String      _transferIn    = _imageBase+"navybox.gif" ;
      private String      _transferOut   = _imageBase+"yellowbox.gif" ;
      private String      _store         = _imageBase+"orangebox.gif" ;
      private long        _absoluteNorm  = 1L ;
      private Map         _map           = null ;
      private long        _maxCounterValue = 0L ;
      private String      _title           = "Title";
      private String      _author        = "dCache Team";
      private String      _tableTitleColor = "#115259" ;
      private String      _keyType = "Key" ;
      private String []   _tableTitles = { _keyType ,
                                         "Absolute Values" ,
                                         "Data / MBytes" ,
                                         "Relative Values" };
      private HtmlDrawable _header = null ;
      private void setHeader( HtmlDrawable drawable ){ _header = drawable ; }
      public void setSorted( boolean sorted ){
         _map = sorted ? (Map)new TreeMap()   : (Map)new LinkedHashMap() ;
         _maxCounterValue = 0L ;
      }
      public void setKeyType(String keyType ){ _tableTitles[0] = keyType ; }
      public void setPrintWriter( PrintWriter pw ){ _pw = pw ; }
      public void reset(){
         _maxCounterValue = 0L ;
      }
      public void add( String title , String link , long [] counter ){
         Object [] o = new Object[3] ;
         o[0] = title ;
         o[1] = makeLink( link ) ;
         o[2] = counter ;
         _maxCounterValue = Math.max( _maxCounterValue , getNorm(counter) );
         _map.put( title , o ) ;
      }
      public void setTitle( String title ){ _title = title ; }
      public void setAuthor( String author ){ _author = author ; }
      public void dump(){
         printHeader( _title ) ;
         if( _header != null )_header.draw(_pw);
         printTitle(_title);
         printTableHeader() ;

         _absoluteNorm = _maxCounterValue ;

         Iterator it = _map.values().iterator() ;
         for( int i = 0 ; it.hasNext() ; i++ ){
             Object [] o = (Object []) it.next() ;
             printRow(
                  (String)o[0] ,
                  (String)o[1] ,
                  (long [])o[2] ,
                  _bgcolor[i%2]
             ) ;
         }

         printTableTrailer() ;

         printTrailer(_author+" ; Created : "+new Date().toString()) ;

      }
      private String makeLink( String link ){
         StringBuffer sb = new StringBuffer() ;
         for( int i= 0 , n = link.length() ; i < n ; i ++ ){
            char c = link.charAt(i) ;
            switch(c){
               case ':' :
                  sb.append("%3A");
               break ;
               default :
                  sb.append(c);
            }
         }
         return sb.toString() ;
      }

      private void printTableHeader(){
         _pw.println("<center><table border=1 cellpadding=0 cellspacing=0 width=\"90%\">");

         _pw.print("<tr>");

         for( int i = 0 , n = _tableTitles.length ; i < n ; i++ ){
             _pw.print("<th bgcolor=\"");
             _pw.print(_tableTitleColor);
             _pw.print("\"><font color=white>");
             _pw.print(_tableTitles[i]);
             _pw.println("</font></th>");
         }

         _pw.println("</tr>");


      }
      private void printTableTrailer(){
         _pw.println("</table></center>");
         _pw.flush();

      }
      private void printHeader(String title ){
         _pw.print("<html><head><title>");
         _pw.print(title);
         _pw.println("</title></head>"+_bodyString);
         _pw.print("<center><h2>");
         _pw.print(_domainName);
         _pw.println("</h2></center>");
      }
      private void printTitle( String title ){
         _pw.print("<center><h1>");
         _pw.print(title);
         _pw.println("</h1></center>");
      }
      private void printTrailer(String author){
          if( author != null ){
            _pw.print("<hr><address>");
            _pw.print(author);
            _pw.println("</address>");
           }
          _pw.println("</body></html>");
          _pw.flush();
      }
      private long getNorm( long [] counter ){
         return  Math.max(
                      counter[YESTERDAY] + counter[RESTORE] + counter[TRANSFER_IN] ,
                      Math.max(
                      counter[TODAY]  ,
                      counter[STORE] + counter[TRANSFER_OUT]
                      )) ;
      }
      private String printUnit( long counter ){
         if( counter <= 0 )return "0" ;
         String unit = ""+counter/(1024L*1024L) ;
         StringBuffer sb = new StringBuffer() ;
         for( int j = unit.length() - 1 , c = 0 ; j >= 0 ; j-- , c++ ){
            if( ( c > 0 ) && ( c%3 == 0 ) )sb.append('.');
            sb.append(unit.charAt(j));
         }
         return sb.reverse().toString();
      }
      private void printRow( String title , String link , long [] counter , String bgColor ){
         int  value  ;
         long norm = getNorm( counter );
         int  content = 0 ;
         printTR() ;

           _pw.print("<th rowspan=3 align=center bgcolor=\"");
           _pw.print(bgColor);
           _pw.print("\">");
           printHREF( title , link ) ;
           _pw.println("</th>");

           printTD( bgColor , false ) ;
             content = 0 ;
             value =  (int) (((double)counter[YESTERDAY])/((double)_absoluteNorm)*((double)_absoluteWidth )) ;
             content += printImage( _poolYesterday , value , _height ) ;

             value =  (int) (((double)counter[RESTORE])/((double)_absoluteNorm)*((double)_absoluteWidth) ) ;
             content += printImage( _restore , value , _height ) ;

             value =  (int) (((double)counter[TRANSFER_IN])/((double)_absoluteNorm)*((double)_absoluteWidth )) ;
             content += printImage( _transferIn , value , _height ) ;

           printTDEnd( content ) ;

           printTD( bgColor , true ) ;
             print(""   +printUnit(counter[YESTERDAY])) ;
             print(" + "+printUnit(counter[RESTORE]) , "red" ) ;
             print(" + "+printUnit(counter[TRANSFER_IN]) , "red" ) ;
           printTDEnd(1) ;

           printTD( bgColor , false ) ;
             content = 0 ;

             value =  (int) (((double)counter[YESTERDAY])/((double)norm)*((double)_relativeWidth )) ;
             content += printImage( _poolYesterday , value , _height ) ;

             value =  (int) (((double)counter[RESTORE])/((double)norm)*((double)_relativeWidth )) ;
             content += printImage( _restore , value , _height ) ;

             value =  (int) (((double)counter[TRANSFER_IN])/((double)norm)*((double)_relativeWidth )) ;
             content += printImage( _transferIn , value , _height ) ;

           printTDEnd(content) ;

         printTREnd() ;

         printTR() ;

           printTD( bgColor , false ) ;
             content = 0 ;
             value =  (int) (((double)counter[TODAY])/((double)_absoluteNorm)*((double)_absoluteWidth )) ;
             content += printImage( _poolToday , value , _height ) ;

           printTDEnd(content) ;

           printTD( bgColor , true ) ;
             print(""+printUnit(counter[TODAY]) ) ;
           printTDEnd(1) ;

           printTD( bgColor , false ) ;
             content = 0 ;
             value =  (int) (((double)counter[TODAY])/((double)norm)*((double)_relativeWidth )) ;
             content += printImage( _poolToday , value , _height ) ;

           printTDEnd(content) ;

         printTREnd() ;

         printTR() ;

           printTD( bgColor , false ) ;
             content = 0 ;
             value =  (int) (((double)counter[TRANSFER_OUT])/((double)_absoluteNorm)*((double)_absoluteWidth )) ;
             content += printImage( _transferOut , value , _height ) ;

             value =  (int) (((double)counter[STORE])/((double)_absoluteNorm)*((double)_absoluteWidth )) ;
             content += printImage( _store , value , _height ) ;

           printTDEnd(content) ;

           printTD( bgColor , true ) ;
             print(""+printUnit(counter[TRANSFER_OUT]) ) ;
             print(" + "+printUnit(counter[STORE]) ) ;
           printTDEnd(1) ;

           printTD( bgColor , false ) ;
             content = 0 ;
             value =  (int) (((double)counter[TRANSFER_OUT])/((double)norm)*((double)_relativeWidth )) ;
             content += printImage( _transferOut , value , _height ) ;

             value =  (int) (((double)counter[STORE])/((double)norm)*((double)_relativeWidth )) ;
             content += printImage( _store , value , _height ) ;

           printTDEnd(content ) ;

         printTREnd() ;

      }
      private void printTR(){ _pw.println("<tr>") ; }
      private void printTREnd(){ _pw.println("</tr>"); }
      private int printImage( String imageFile , int width , int height ){
         if( width <= 0 ){  return 0 ; }
         _pw.print("<img src=\"");
         _pw.print(imageFile);
         _pw.print("\" width=");
         _pw.print(Math.max(width,0)) ;
         _pw.print(" height=");
         _pw.print(height);
         _pw.print(">");
         return 1 ;
      }
      private void printTD( String bgcolor , boolean center ){
         _pw.print("<td ");
         if( center )_pw.print("align=center ");
         _pw.print("bgcolor=\"");
         _pw.print(bgcolor);
         _pw.print("\">");
     }
     private void print(String string ){
        print( string , null )  ;
     }
     private void print(String string , String color ){
        if( color != null ){
           _pw.print("<font color=\"");
           _pw.print(color);
           _pw.print("\">");
        }
        _pw.print(string);
        if( color != null ){
           _pw.print("</font>");
        }
     }
     private void printTDEnd( int content ){
        if( content == 0 )_pw.print("&nbsp;");
        _pw.println("</td>");
     }
     private void printHREF( String title , String link ){
         _pw.print("<a href=\"");
         _pw.print(link);
         _pw.print("\">");
         _pw.print(title);
         _pw.println("</a>");
     }
  }
}
