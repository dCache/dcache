package dmg.util.db ;

import java.util.* ;
import java.io.* ;

public class      DbFileRecord 
       extends    DbGLock 
       implements DbRecordable{

   private DbLockable  _superLock;
   private File        _dataSource;
   private Hashtable<String, Object> _table      = new Hashtable<>() ;
   private boolean     _exists     = true ;
   private boolean     _dataValid;
   
   public DbFileRecord( File source , boolean create )
          throws IOException     {
      this( null , source , create ) ;      
   }
   public DbFileRecord( DbLockable superLock , File source , boolean create )
          throws IOException     {
       
       super( superLock ) ;   
       _superLock  = superLock ;
       _dataSource = source ;
       if( create && _dataSource.exists() ) {
           throw new IllegalArgumentException("DataSource already exists(2)");
       }
       if( ( ! create ) && ( ! _dataSource.exists() ) ) {
           throw new IllegalArgumentException("DataSource not found");
       }
       if( create ){
          FileOutputStream out = new FileOutputStream( _dataSource ) ;
          out.close() ;
       }
   }
   @Override
   public synchronized void open( int mode )
          throws DbLockException, InterruptedException {
       if( ! _exists ) {
           throw new DbLockException("Object removed");
       }
       super.open( mode ) ;
       
   }
   @Override
   public synchronized void setAttribute( String name , String attribute ){
       _table.put( name , attribute ) ;
   }
   @Override
   public synchronized void setAttribute( String name , String [] attribute ){
       _table.put( name , attribute ) ;
   }
   @Override
   public synchronized Object getAttribute( String name ){
        return _table.get(name) ;
   }
   @Override
   public synchronized void remove(){
       _exists = false ;
       _dataSource.delete() ;
   }
   @Override
   public synchronized Enumeration<String> getAttributes(){ return _table.keys() ; }
   public synchronized void read() throws IOException {
      BufferedReader reader = new BufferedReader( 
                                 new FileReader( _dataSource ) ) ;
      
      try{
          int state = 0 ;
          String line, name = null , value;
          Vector<String> vec = null ;
          StringTokenizer st;
          while( ( line = reader.readLine() ) != null ){
             if( state == 0 ){
                st = new StringTokenizer( line , "=" ) ;
                if( st.countTokens() < 2 ) {
                    continue;
                }
                name  = st.nextToken() ;
                value = st.nextToken() ;
                if( value.equals( "***LIST***" ) ){
                   state = 1 ;
                   vec   = new Vector<>() ;
                }else{
                   _table.put( name , value ) ;
                }
             }else if( state == 1 ){
                if( line.equals( "***LIST***" ) ){
                    String [] a = new String[vec.size()] ;
                    vec.copyInto( a ) ;
                    _table.put( name , a ) ;
                    state = 0 ;
                }else{
                    vec.addElement( line ) ;
                }
             }
          }
      }catch(IOException e ){
         throw e ;
      }finally{
         try{ reader.close() ; }catch(Exception ee){}
      }
   }
   public synchronized void write() throws IOException {
      PrintWriter pw = new PrintWriter( 
                                new FileWriter( _dataSource ) ) ;
       for (Object o1 : _table.keySet()) {
           String name = (String) o1;
           Object o = _table.get(name);
           if (o == null) {
           } else if (o instanceof String[]) {
               pw.println(name + "=***LIST***");
               for (String s : (String[]) o) {
                   pw.println(s);
               }
               pw.println("***LIST***");
           } else if (o instanceof String) {
               pw.println(name + "=" + o);
           }
       }
      pw.close() ;
   }
   @Override
   public void readLockGranted() {
     System.out.println( "readLockGranted "+_dataSource ) ;
     if( ! _dataValid ){
        try{ read() ; }catch(Exception eee ){}
        _dataValid = true ;
     }
   }
   @Override
   public void writeLockGranted(){
     System.out.println( "writeLockGranted "+_dataSource ) ;
     if( ! _dataValid ){
        try{ read() ; }catch(Exception eee ){}
        _dataValid = true ;
     }
   }
   @Override
   public void readLockReleased(){
     System.out.println( "readLockReleased "+_dataSource ) ;
   }
   @Override
   public void writeLockReleased(){
     System.out.println( "writeLockReleased "+_dataSource ) ;
     try{ write() ; }catch(Exception eee ){}
   }

   public static void main( String [] args ) throws Exception {
       
       if( args.length < 2 ){
          System.out.println( "... read/write <filename>" ) ;
          System.exit(4) ;
       }
       switch (args[0]) {
       case "read": {
           DbFileRecord rec = new DbFileRecord(new File(args[1]), false);
           long start, opened, fetched, finished;
           Enumeration<String> e;
           for (int l = 0; l < 2; l++) {
               start = System.currentTimeMillis();
               rec.open(DbGLock.WRITE_LOCK);
               opened = System.currentTimeMillis();
               e = rec.getAttributes();
               while (e.hasMoreElements()) {
                   String name = e.nextElement();
                   Object o = rec.getAttribute(name);
                   if (o == null) {
                   } else if (o instanceof String[]) {
//                System.out.println( name+"=***LIST***" ) ;
//                String [] str = (String [] )o ;
//                for( int i = 0 ; i < str.length ; i++ )
//                   System.out.println( str[i] ) ;
//                System.out.println("***LIST***" ) ;
                   } else if (o instanceof String) {
//                System.out.println( name+"="+o) ;
                   }
               }
               fetched = System.currentTimeMillis();
               rec.close();
               finished = System.currentTimeMillis();
               System.out.println("Open  : " + (opened - start));
               System.out.println("Read  : " + (fetched - opened));
               System.out.println("Close : " + (finished - fetched));

           }
           break;
       }
       case "test":
           break;
       case "write": {
           DbFileRecord rec = new DbFileRecord(new File(args[1]), true);
           rec.open(DbGLock.WRITE_LOCK);
           rec.setAttribute("storageGroup", "dst-98");
           String[] bfids = new String[2000];
           String str;
           StringBuffer sb;
           for (int i = 0; i < bfids.length; i++) {
               sb = new StringBuffer();
               str = "" + i;
               sb.append("U");
               for (int j = 5; j >= str.length(); j--) {
                   sb.append('0');
               }
               sb.append(str);
               bfids[i] = sb.toString();
           }
           rec.setAttribute("bfids", bfids);
           rec.close();
           break;
       }
       }

   }

} 
