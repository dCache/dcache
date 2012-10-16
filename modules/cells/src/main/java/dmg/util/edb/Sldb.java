package dmg.util.edb ;

import java.io.* ;
import java.util.* ;

/**
 *  The Sldb class builds the in memory represetation of a
 *  simple database, which is able to store several million
 *  of entries. The record data is assumed to have a
 *  maximum size and the storage key is determined by the
 *  database.
 * 
 * @version 2.0, 11/24/99
 * @author  Patrick Fuhrmann  patrick.fuhrmann@desy.de
 * @since   JDK1.1
 */
public class Sldb {

   private RandomAccessFile _file;
   //
   // header
   //
   //  0 int   VERSION=1
   //  4 int   bytesPerRecord
   //  8 int   recordsPerBlock 
   // 12 int   blockInUse 
   // 16 long  lastOpen   (write only)
   // 24 long  lastClose  
   // 32 long  dbId
   // 40 UTF   dbName 
   //   .... ( total = 128 byte )
   //
   //
   private static final int __VERSION = 1 ;
   private static final int __headerOffset = 128 ;
   private int     _rpb;
   private int     _bpdr;
   private int     _biu;
   private long    _lastOpen;
   private long    _lastClose;
   private long    _dbId;
   private String  _dbName    = "Unknown" ;
   
   private DirectoryDesc [] _desc;
   private Random _random = new java.util.Random( new Date().getTime());
   
   private class SldbEntryImpl implements SldbEntry {
       private long _cookie;
       private int  _blockPos , _bytePos , _bitPos, _recordPos ;
       private long _filePos ;
       SldbEntryImpl( long cookie ){
          _cookie    = cookie ;
          _blockPos  = (int) (_cookie / _rpb ) ;
          _recordPos = (int) (_cookie % _rpb ) ;
          _bytePos   = _recordPos / 8 ;
          _bitPos    = _recordPos % 8;
          _filePos   = __headerOffset + 
                 _blockPos * ( _rpb / 8 + _rpb * _bpdr ) +
                             ( _rpb / 8 + _recordPos * _bpdr ) ;
         
       }
       @Override
       public long getCookie(){ return _cookie ; }
       @Override
       public SldbEntry getNextEntry(){ return null ; }
       public String toString(){
         return ""+_cookie+":"+_blockPos+":"+_recordPos+":"+
                   _bytePos+":"+_bitPos+":"+_filePos ;
       }
   }
   /**
    *
    *  Creates the representation of a 'Sldb' database.
    *  The database file must exist and must have been
    *  successfully initilized.
    *  @param file is the file object containing the database.
    *  @exception IOException is forwarded from the underlaying
    *                         I/O operations.
    *  @exception IllegalArgumentException is thrown if the
    *             structure of the database file is not compatible
    *             with the structure of a Sldb VERSION I database or
    *             if the file doesn't exit.
    */
   public Sldb( File file ) throws IOException {
      _openExistingFile( file ) ;
   }
   /**
    *
    *  Creates the representation of a 'Sldb' database.
    *  The database file must not exist and will be initialized
    *  with the parameter given.
    *
    *  @param file is the file object which will contain the database.
    *  @param bytesPerDataRecord is the maximum size in byte
    *         a datarecord is allow to have. 
    *  @param recordsPerBlock is the number of records a directory block
    *         contains. 
    *  @exception IOException is forwarded from the underlaying
    *                         I/O operations.
    *  @exception IllegalArgumentException is thrown if one of the
    *             arguments doesn't make sense in conjuction with
    *             a Sldb VERSION I database.
    */
   public Sldb( File file , 
                int bytesPerDataRecord ,
                int recordsPerBlock      ) throws IOException {
   
        if( file.exists() ) {
            throw new IllegalArgumentException("file exists");
        }
        if( bytesPerDataRecord < 1 ) {
            throw new
                    IllegalArgumentException("bytesPerDataRecord < 1");
        }
        
        _file = new RandomAccessFile( file , "rw") ;
        _rpb  = recordsPerBlock < 32 ? 32 : recordsPerBlock ;
        _bpdr = ( ( bytesPerDataRecord - 1 ) / 8 + 1 ) * 8 ;
        _biu  = 1 ;
        _lastOpen  = new Date().getTime() ;
        _lastClose = 0L ;
        
        byte [] dir  = new byte[_rpb/8] ;
        
        try{
           writeHeader() ;
           
           _file.seek( __headerOffset ) ;
           _file.write( dir ) ;
           _file.close() ;
        }catch( IOException e ){
           try{ _file.close() ; }catch(Exception ee){}
           throw e ;
        }
        
        _openExistingFile( file ) ;
   }
   private void _openExistingFile( File file )throws IOException{
      if( ! file.exists() ) {
          throw new
                  IllegalArgumentException("file not found : " + file
                  .getName());
      }
      _file = new RandomAccessFile( file , "rw" ) ;
      try{
      
         readHeader() ;

      }catch( IllegalArgumentException iae ){
         try{ _file.close() ; }catch(Exception ee){}
         throw iae ;
      }catch( IOException e ){
         try{ _file.close() ; }catch(Exception ee){}
         throw e ;
      }
      _desc = new DirectoryDesc[_biu] ;
      for( int i = 0 ; i < _biu ; i++ ){
         _desc[i] = new DirectoryDesc(i) ;
      }
   }
   private void writeHeader() throws IOException {
       _file.seek( 0L ) ;
       _file.writeInt( __VERSION ) ;
       _file.writeInt( _bpdr ) ;
       _file.writeInt( _rpb  ) ;
       _file.writeInt( _biu  ) ;
       _file.writeLong( _lastOpen ) ;
       _file.writeLong( _lastClose ) ;
       _file.writeLong( _dbId ) ;
       _file.writeUTF( _dbName ) ;
   }
   private void readHeader() throws IOException {
       _file.seek( 0L ) ;
       int version = _file.readInt() ;
       if( version != __VERSION ) {
           throw new IllegalArgumentException(
                   "Version mismatch : I need " + __VERSION + " ; I found " + version);
       }

       _bpdr = _file.readInt() ;
       _rpb  = _file.readInt() ;
       _biu  = _file.readInt() ;

       if( ( _bpdr < 8  ) || ( ( _bpdr % 8 ) != 0 ) ||
           ( _rpb  < 32 ) || ( _rpb > ( 64 * 1024 * 1024 ) ) ||
           ( _biu  < 1   )  ) {
           throw new
                   IllegalArgumentException(
                   "Not a Sldb File (bpfr=" + _bpdr + ";rpb=" + _rpb + ")");
       }
        
        _lastOpen = _file.readLong() ;
        _lastClose = _file.readLong() ;
        
        _dbId      = _file.readLong() ;
        _dbName    = _file.readUTF() ;
   }
   /**
    *
    *  closes the database. All subsequent operations on this
    *  object will fail.
    *
    *   @exception IOException  is thrown if the underlying 
    *              file object throws an exception.
    */
   public synchronized void close() throws IOException {
     _lastClose = new Date().getTime() ;
     writeHeader() ;
     _file.close() ;
     _file = null ;
   }
   /**
    *   returns a representation of a Database record 
    *   derived from a cookie.
    *   @param cookie an external representation of a database
    *          record.
    *   @return the internal representation of a database record.
    */
   public synchronized SldbEntry getEntry( long cookie ){
      return new SldbEntryImpl( cookie ) ;
   }
   /**
    *   returns a representation of a newly created Database record.
    *   The record is marked allocated.
    *
    *   @return the internal representation of a database record.
    */
   public synchronized SldbEntry getEntry() throws IOException{
      int s = findSmallestBlock() ;
      DirectoryDesc d = _desc[s] ;
//      System.out.println( " high water "+d.getHighWater() ) ;
      if( d.getHighWater() > 80 ) {
          extend();
      }
      s = findSmallestBlock() ;
//      System.out.println( "choosing : "+s ) ;
      return _desc[s].getEntry() ;
   }
   /**
    *   returns the database record represented by the SldbEntry.
    *   @param e the internal representation of a database
    *             record.
    *   @return the byte array containing the data record.
    */
   public synchronized byte [] get( SldbEntry e )throws IOException {
       SldbEntryImpl sldb =(SldbEntryImpl) e ;
       
       int block = sldb._blockPos ;
       if( block >= _biu ) {
           throw new
                   IllegalArgumentException("Not withing block " + _biu);
       }
      
       if( ! _desc[block].isInUse( sldb ) ) {
           throw new
                   IllegalArgumentException("Record not in use");
       }
       
       _file.seek( sldb._filePos ) ;
       
       int size = _file.readInt() ;
       if( size >= ( _bpdr - 4 ) ) {
           throw new
                   IllegalArgumentException(
                   "Data record corrupted at " + sldb._filePos);
       }
         
       byte [] res = new byte[size] ;
       _file.readFully( res ) ;
       return res ;
   }
   /**
    *  stores a datarecord and return the internal representaion.
    *  @param data the byte array containing the data.
    *  @param off the offset relative to 'data'.
    *  @param size the size of the data portion.
    *  @return the internal record represention.
    *  @exception IOException from the I/O layer.
    */
   public synchronized SldbEntry put( byte [] data , int off , int size )
          throws IOException {
    
       SldbEntryImpl ei = (SldbEntryImpl)getEntry() ;
       return put( ei , data , off , size ) ;      
   }
   /**
    *  stores a datarecord at the position of the internal record represention.
    *  @param e the internal record representation.
    *  @param data the byte array containing the data.
    *  @param off the offset relative to 'data'.
    *  @param size the size of the data portion.
    *  @return the internal record represention.
    *  @exception IOException from the I/O layer.
    */
   public synchronized SldbEntry put( SldbEntry e , byte [] data , int off , int size )
          throws IOException {
    
      if( size > (_bpdr-4) ) {
          throw new
                  IllegalArgumentException(
                  "can only accept " + _bpdr + " bytes");
      }
            
       SldbEntryImpl ei =(SldbEntryImpl) e ;
       
       _file.seek( ei._filePos ) ;
       _file.writeInt( size ) ;
       _file.write( data , off , size ) ;
       
      _desc[ei._blockPos].setInUse( ei , true ) ;
            
      return e ;
   }
   private class DirectoryDesc {
       private long _position;
       private int  _block;
       private int  _inUse;
       private byte [] _dir;
       private DirectoryDesc( int block ) throws IOException {
          if( block >= _biu ) {
              throw new
                      IllegalArgumentException(
                      "block number to large " + block + " >= " + _biu);
          }
                
          _block    = block ;
          _position = __headerOffset + 
                      _block * ( _rpb / 8 + _rpb * _bpdr ) ;
          
          _file.seek( _position ) ;
          _dir = new byte[_rpb/8] ;          
          _file.readFully( _dir  ) ;
          _inUse = countInUse() ;
       }
       private DirectoryDesc() throws IOException {
          _block    = _biu ;
          _inUse    = 0 ;
          _position = __headerOffset + 
                      _block * ( _rpb / 8 + _rpb * _bpdr ) ;
          
          _file.seek( _position ) ;
          _dir = new byte[_rpb/8] ;   
          _file.write( _dir ) ;
               
       }
       private int getHighWater(){
           return (int)(((float)_inUse)/((float)_rpb)*100.);
       }
       private int getBlock(){ return _block ; }
       private void store() throws IOException {
          _file.seek( _position ) ;
          _file.write( _dir ) ;
       }
       private SldbEntryImpl nextUsedEntry( int bytePos , int bitPos ){
          int maxBytes = _rpb / 8 ;
          while( bytePos < _dir.length ){
//              System.out.println( " checking "+bytePos+" "+bitPos) ;
              if( ( _dir[bytePos%maxBytes] & ( 1 << bitPos ) ) != 0 ){
                 long cookie = (long)_block * (long)_rpb +
                               (bytePos%maxBytes) * 8 +
                               bitPos ;
                 SldbEntryImpl ei = new SldbEntryImpl( cookie ) ;
                 return ei ;
              }
              if( ( ++ bitPos ) >= 8 ){
                 bytePos ++ ;
                 bitPos = 0 ;
              }
             
          }
          return null ;
       }
       private SldbEntryImpl nextUsedEntry( SldbEntryImpl e ){
          int bitPos = e._bitPos + 1 ;
          int bytePos = e._bytePos ;
          if( ( bitPos ) >= 8 ){
              bitPos = 0 ;
              bytePos ++ ;
          }  
         
          return nextUsedEntry( bytePos , bitPos )  ;
       }
       private SldbEntryImpl nextUsedEntry(){
          return nextUsedEntry( 0 , 0 ) ;
       }
       private SldbEntryImpl getEntry() throws IOException {
          if( _inUse >= _rpb ) {
              throw new
                      IllegalArgumentException("out of space : " + _block);
          }
          int r = ( _random.nextInt() & 0xffffff )% _rpb ;
          int bitPos   = r % 8 ;
          int bytePos  = r / 8 ;
          int maxBytes = _rpb / 8 ;
          for( int i= 0 ; i < _rpb ; i++ ){
              if( ( _dir[bytePos] & ( 1 << bitPos ) ) == 0 ){
                 long cookie = (long)_block * (long)_rpb +
                               bytePos * 8 +
                               bitPos ;
                 SldbEntryImpl ei = new SldbEntryImpl( cookie ) ;
                 setInUse( ei , true ) ;
                 return ei ;
              }
              if( ( ++ bitPos ) >= 8 ){
                 bytePos = ( bytePos + 1 ) % maxBytes;
                 bitPos = 0 ;
              }
             
          }
          throw new
          IllegalArgumentException("out of space(2) : "+_block) ;
       }
       public int getInUse(){ return _inUse ; }
       public int countInUse(){
          int c = _rpb / 8 ;
          int counter = 0 ;
          for( int i = 0 ; i < c ; i++ ){
             int v = _dir[ i ] ;
             int mask = 1 ;
             for( int j = 0 ; j < 8 ; j++ ){
                if( ( v & mask ) > 0 ) {
                    counter++;
                }
                mask <<= 1 ;
             }
          
          }
          return counter ;
       }
       public void setInUse( SldbEntryImpl e , boolean set ) throws IOException{
          int v = _dir[ e._bytePos ] ;
          int m = 1 << e._bitPos ;
          boolean wasSet = ( v & m ) > 0 ;
          if( set && ! wasSet ){
             v |= m ;  
             _dir[ e._bytePos ] = (byte)v ;
             _inUse++ ;
             store() ;
          }else if( wasSet && ! set ){
             v &= ~ m ;
             _dir[ e._bytePos ] = (byte)v ;
             _inUse-- ;
             store() ;
          }
          
          
       }
       public boolean isInUse( long cookie ){
          
             
          int p = (int)( cookie % _rpb ) ;
          int v = _dir[ p / 8 ] ;
          int s = p % 8 ;
          return ( v & ( 1 << ( p % 8 ) ) ) > 0 ;
       }
       public boolean isInUse( SldbEntryImpl e ){
                 
          return ( _dir[ e._bytePos ] & ( 1 << ( e._bitPos % 8 ) ) ) > 0 ;
          
       }
       public String toString(){
          StringBuilder sb  = new StringBuilder() ;
          sb.append("t=").append(_rpb).append(";u=").append(_inUse)
                  .append(";m=");
          for( int i = 0 ; i < _rpb ; i++ ) {
              sb.append(isInUse(i) ? "1" : "0");
          }
          
          return sb.toString() ;
       }
   }
   String dirToString(){
      StringBuilder sb = new StringBuilder() ;
      for( int i= 0 ;i <  _biu ; i++ ){
         sb.append( _desc[i].toString() ).append("\n") ;
      }
      return sb.toString() ;
   }
   /**
    *  marks a record as unused.
    *   @param cookie is the external data representation of this record.
    *   @exception IOException from the I/O layer.
    */
   synchronized void remove( long cookie ) throws IOException {
      setInUse( cookie , false ) ;
   }
   /**
    *  marks a record as unused.
    *   @param e is the internal data representation of this record.
    *   @exception IOException from the I/O layer.
    */
   public synchronized void remove( SldbEntry e  ) throws IOException {
      setInUse( e , false ) ;
   }
   /**
    *  returns the internal representation of the next used
    *  record following the specified one.
    *   @param e is the internal data representation of this record.
    *   @return the internal representation of the next used record.
    */
   public synchronized SldbEntry nextUsedEntry( SldbEntry e ){
       SldbEntryImpl ei = (SldbEntryImpl)e ;
       SldbEntry re;
       int block = ei._blockPos ;
       if( block >= _biu ) {
           return null;
       }
       if( ( re = _desc[block].nextUsedEntry(ei) ) != null ) {
           return re;
       }
       for( block++ ; block < _biu ; block++ ) {
           if ((re = _desc[block].nextUsedEntry()) != null) {
               return re;
           }
       }
       return null ;
   }
   protected void setInUse( long cookie , boolean set ) throws IOException {
      SldbEntryImpl ei = new SldbEntryImpl( cookie );
      int block = ei._blockPos ;
      if( block >= _biu ) {
          throw new
                  IllegalArgumentException("Not within block " + _biu);
      }
      _desc[block].setInUse( ei , set ) ;
   }
   protected void setInUse( SldbEntry e , boolean set ) throws IOException {
      SldbEntryImpl ei = (SldbEntryImpl) e ;
      int block = ei._blockPos ;
      if( block >= _biu ) {
          throw new
                  IllegalArgumentException("Not within block " + _biu);
      }
      _desc[block].setInUse( ei , set ) ;
   }
   private void extend() throws IOException {
       int size = _desc.length  ;
       DirectoryDesc [] desc = new DirectoryDesc[size+1] ;
       System.arraycopy( _desc , 0 , desc , 0 , size ) ;
       desc[size] = new DirectoryDesc() ;
       if( desc[size].getBlock() != size ) {
           throw new IllegalArgumentException(
                   "PANIC : block mismath while expending");
       }
       _biu ++ ;
       _file.seek( 12L ) ;
       _file.writeInt( _biu ) ;
       _desc = desc ;
   }
   private int findSmallestBlock(){
      int minPos = 0 ;
      int min    = _desc[minPos].getInUse() ;
      for( int i = 1 ; i < _biu ; i++ ){
         int t = _desc[i].getInUse() ;
         if( t < min ) {
             minPos = i;
         }
      }
      return minPos ;
   }
   /*
   public void setInUse( long cookie , boolean set ) throws IOException {
      int block = (int)( cookie / _rpb ) ;
      if( block >= _biu )
          throw new
          IllegalArgumentException( "Not withing block "+_biu) ;
      _desc[block].setInUse( cookie , set ) ;
   }
   */
   public static void main( String [] args )throws Exception{
     if( args.length < 2 ){
        System.err.println( "Usage : ... <filename> create" ) ;
        System.err.println( "Usage : ... <filename> show" ) ;
        System.err.println( "Usage : ... <filename> getcookie" ) ;
        System.err.println( 
            "Usage : ... <filename> write <string> [<cookie>]" ) ;
        System.err.println( 
            "Usage : ... <filename> read <cookie>" ) ;
        System.exit(4) ;
     }
     Sldb sldb = null ;
     if( args[1].equals( "create" ) ){
        sldb = new Sldb( new File("xxx") , 1024 , 32 ) ;
        sldb.close() ;
        return ;
     }
     try{
        sldb = new Sldb( new File(args[0]) ) ;
         switch (args[1]) {
         case "set":
             if (args.length < 3) {
                 throw new
                         IllegalArgumentException("usage : ... <dbFile> set <n>");
             }
             sldb.setInUse(Long.parseLong(args[2]), true);
             break;
         case "unset":
             if (args.length < 3) {
                 throw new
                         IllegalArgumentException("usage : ... unset <n>");
             }
             sldb.setInUse(Long.parseLong(args[2]), false);
             break;
         case "write": {
             if (args.length < 3) {
                 throw new
                         IllegalArgumentException(
                         "usage : ... <dbFile> write <string> [<cookie>]");
             }
             byte[] data = args[2].getBytes();
             SldbEntry e;
             if (args.length < 4) {
                 e = sldb.put(data, 0, data.length);
             } else {
                 e = sldb.getEntry(Long.parseLong(args[3]));
                 sldb.put(e, data, 0, data.length);
             }
             System.out.println("" + e.getCookie());
             break;
         }
         case "next": {
             if (args.length < 3) {
                 throw new
                         IllegalArgumentException(
                         "usage : ... <dbFile> next <cookie>");
             }
             SldbEntry e = sldb.getEntry(Long.parseLong(args[2]));
//           System.out.println( "Starting with : "+e.getCookie() ) ;
             e = sldb.nextUsedEntry(e);
             if (e == null) {
                 throw new IllegalArgumentException("No entries left");

             } else {
                 System.out.println("" + e.getCookie());
             }
             break;
         }
         case "read": {
             if (args.length < 3) {
                 throw new
                         IllegalArgumentException(
                         "usage : ... <dbFile> read <c>");
             }
             SldbEntry e = sldb.getEntry(Long.parseLong(args[2]));
             byte[] data = sldb.get(e);
             System.out.println(new String(data));
             break;
         }
         case "getcookie": {
             if (args.length < 2) {
                 throw new
                         IllegalArgumentException(
                         "usage : ... <dbFile> getcookie");
             }
             SldbEntry e = sldb.getEntry();
             System.out.println("" + e.getCookie());
             break;
         }
         case "show":
             sldb = new Sldb(new File(args[0]));
             System.out.println(sldb.dirToString());
             break;
         }
     }catch(IllegalArgumentException iae ){
        System.err.println( "Wrong Argument : "+iae.getMessage() ) ;
        System.exit(4);
     }catch(IOException e ){
        System.err.println( "IOProblem : "+e.getMessage() ) ;
        System.exit(5);
     }catch(Exception ee ){
        System.err.println( "Problem : "+ee ) ;
        ee.printStackTrace();
        System.exit(6);
     }finally{
//        try{ Thread.sleep(20000) ;}
//        catch(InterruptedException ie ){}
        try{ sldb.close() ;} catch(Exception ee){}
     }
     System.exit(0);
   }
}
