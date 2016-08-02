package dmg.util.cdb ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class      CdbFileRecord
       extends    CdbGLock
       implements CdbElementable            {

   private CdbLockable  _superLock;
   private File         _dataSource;
   private Hashtable<String, Object> _table      = new Hashtable<>() ;
   private boolean      _exists     = true ;
   private boolean      _dataValid;

   public CdbFileRecord( CdbLockable superLock , File source , boolean create )
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
          throws CdbLockException, InterruptedException {
       if( ! _exists ) {
           throw new CdbLockException("Object removed");
       }
       super.open( mode ) ;

   }
   public synchronized void addListItem( String attributeName , String itemName ){
      addListItem( attributeName , itemName , false ) ;
   }
   public synchronized void addListItem(
                  String  attributeName ,
                  String  itemName ,
                  boolean unique       ){
       Object o = _table.get( attributeName ) ;
       String [] list;
       if( o == null ){
          list    = new String[1] ;
          list[0] = itemName ;
          _table.put( attributeName , list ) ;
       }else if( ! ( o instanceof Object [] ) ){
          throw new IllegalArgumentException( "Not a list : "+attributeName ) ;
       }else{
          list = (String [])o ;
          String [] tmp = new String[list.length+1] ;
          if( unique ){
             int i ;
             for( i = 0 ;
                  ( i < list.length ) &&
                  ( ! list[i].equals(itemName) ) ; i++ ) {
                 tmp[i] = list[i];
             }
             if( i < list.length ) {
                 throw new IllegalArgumentException("duplicated entry");
             }

          }else{
              System.arraycopy(list, 0, tmp, 0, list.length);
          }
          tmp[list.length] = itemName ;
          _table.put( attributeName , tmp ) ;
       }
   }
   public synchronized void removeListItem( String attributeName ,
                                            String itemName ){
       Object o = _table.get( attributeName ) ;
       String [] list;
       if( o == null ){
          return ;
       }else if( ! ( o instanceof Object [] ) ){
          throw new IllegalArgumentException( "Not a list : "+attributeName ) ;
       }else{
          list = (String [])o ;
          if( list.length == 0 ) {
              return;
          }
          int i = 0 ;
          for( ; ( i < list.length ) && ( ! list[i].equals( itemName ) ) ; i++ ) {
          }
          if( i ==  list.length ) {
              return;
          }

          String [] tmp = new String[list.length-1] ;
          int l = 0 ;
          for( int j = 0 ; j < i ; j ++ ) {
              tmp[l++] = list[j];
          }
          for( int j = (i+1) ; j < list.length ; j++ ) {
              tmp[l++] = list[j];
          }
          list = tmp ;
       }
       _table.put( attributeName , list ) ;
   }
   public synchronized void setAttribute( String name , String attribute ){
       _table.put( name , attribute ) ;
   }
   public synchronized void setAttribute( String name , String [] attribute ){
       _table.put( name , attribute ) ;
   }
   public void setAttibute( String name , int value ){
       _table.put(name , String.valueOf(value)) ;
   }
   public synchronized Object getAttribute( String name ){
        return _table.get(name) ;
   }
   public int getIntAttribute( String key ){
      String str = (String)getAttribute( key ) ;
      if( str == null ) {
          return 0;
      }
      try{
         return Integer.parseInt( str ) ;
      }catch( NumberFormatException eee ){
         return 0 ;
      }
   }
   @Override
   public synchronized void remove(){
       _exists = false ;
       _dataSource.delete() ;
   }
   public synchronized String [] getAttributeNames() {
      int size = _table.size() ;
      String [] names = new String[size] ;
       Iterator<String> iterator = _table.keySet().iterator();
      for( int i = 0 ; ( i < size ) && (iterator.hasNext()) ; i++ ) {
          names[i] = iterator.next();
      }
      return names ;
   }
   public String toLine(){
      StringBuilder sb = new StringBuilder() ;
       Iterator<String> iterator = _table.keySet().iterator();
      for( int i = 0 ; iterator.hasNext(); i++ ){
         String key   = iterator.next();
         Object value = _table.get( key ) ;
         if( value instanceof String ){
            sb.append(key).append('=').append((String)value) ;
         }else if( value instanceof String [] ){
            sb.append(key).append('=') ;
            String [] values = (String[])value ;
            for( int j = 0 ; j < values.length ; j++ ){
              if( j > 0 ) {
                  sb.append(',');
              }
              sb.append(values[j]);
            }
         }
         sb.append(';') ;
      }
      return sb.toString() ;
   }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
       Iterator<String> iterator = _table.keySet().iterator();
      for( int i = 0 ; iterator.hasNext(); i++ ){
         //if( i > 0 )sb.append( ";" ) ;
         String key   = iterator.next();
         Object value = _table.get( key ) ;
         if( value instanceof String ){
            sb.append(key).append('=').append((String)value).append('\n') ;
         }else if( value instanceof String [] ){
            sb.append(key).append("=List\n") ;
             for (String s : (String[])value) {
                 sb.append(s).append('\n');
             }
         }
      }
      return sb.toString() ;
   }
   public synchronized Enumeration<String> getAttributes(){ return _table.keys() ; }
   public synchronized void read() throws IOException {
      BufferedReader reader = new BufferedReader(
                                 new FileReader( _dataSource ) ) ;

      try{
          int state = 0 ;
          String line, name = null , value;
          Vector<String> vec = null ;
          while( ( line = reader.readLine() ) != null ){
             if( state == 0 ){
                int pos = line.indexOf('=') ;
                if( pos < 0 ) {
                    continue;
                }
                name  = line.substring(0,pos) ;
                value = pos == (line.length()-1) ? "" : line.substring(pos+1) ;
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
               pw.println(name + '=' + o);
           }
       }
      pw.close() ;
   }
   @Override
   public void readLockGranted() {
//     System.out.println( "readLockGranted "+_dataSource ) ;
     if( ! _dataValid ){
        try{ read() ; }catch(Exception eee ){}
        _dataValid = true ;
     }
   }
   @Override
   public void writeLockGranted(){
//     System.out.println( "writeLockGranted "+_dataSource ) ;
     if( ! _dataValid ){
        try{ read() ; }catch(Exception eee ){}
        _dataValid = true ;
     }
   }
   @Override
   public void readLockReleased(){
//     System.out.println( "readLockReleased "+_dataSource ) ;
   }
   @Override
   public void writeLockReleased(){
//     System.out.println( "writeLockReleased "+_dataSource ) ;
     if( _exists ) {
         try {
             write();
         } catch (Exception eee) {
         }
     }
   }
   @Override
   public void writeLockAborted(){
//      System.out.println( "writeLockAborted "+_dataSource ) ;
      try{ read() ; }catch(Exception eee ){}
   }

//   public String toString(){
//      return super.toString() ;
//   }
   public static void main( String [] args ) throws Exception {

       if( args.length < 2 ){
          System.out.println( "... read/write <filename>" ) ;
          System.exit(4) ;
       }
       switch (args[0]) {
       case "read": {
           CdbFileRecord rec =
                   new CdbFileRecord(null,
                           new File(args[1]),
                           false);
           long start, opened, fetched, finished;
           Enumeration<String> e;
           for (int l = 0; l < 2; l++) {
               start = System.currentTimeMillis();
               rec.open(CdbLockable.WRITE);
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
               rec.close(CdbLockable.COMMIT);
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
           CdbFileRecord rec =
                   new CdbFileRecord(null,
                           new File(args[1]),
                           true);
           rec.open(CdbLockable.WRITE);
           rec.setAttribute("storageGroup", "dst-98");
           String[] bfids = new String[2000];
           String str;
           StringBuffer sb;
           for (int i = 0; i < bfids.length; i++) {
               sb = new StringBuffer();
               str = String.valueOf(i);
               sb.append('U');
               for (int j = 5; j >= str.length(); j--) {
                   sb.append('0');
               }
               sb.append(str);
               bfids[i] = sb.toString();
           }
           rec.setAttribute("bfids", bfids);
           rec.close(CdbLockable.COMMIT);
           break;
       }
       }

   }

}
