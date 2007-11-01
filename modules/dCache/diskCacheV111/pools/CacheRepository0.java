// $Id: CacheRepository0.java,v 1.2 2001-03-25 18:14:40 cvs Exp $

package diskCacheV111.pools ;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

import java.util.* ;
import java.io.* ;
import dmg.cells.nucleus.CellMessage ;
import dmg.util.Logable ; 

public class CacheRepository0 {
    private Hashtable _pnfsids = new Hashtable() ;
    private long      _totalSize = 0 ;
    private long      _usedSpace = 0 ;
    private File      _baseDir ;
    private File      _controlDir ;
    private File      _dataDir ;
    public final static int  STORE  = 1 ;
    public final static int  CLIENT = 2 ;
    
    public class CacheEntry {
       private String  _pnfsId ;
       private long    _creationTime = new Date().getTime() ;
       private boolean _cached    = false , 
                       _precious  = false ,
                       _busy      = false ; 
       private int     _receiving = 0 ;
       private StorageInfo _storageInfo = null ;
       private Vector      _listener    = null ;
       private long        _size        = 0 ;

       private CacheEntry( String pnfsId ){
          _pnfsId = pnfsId ;
          
       }
       private CacheEntry( PnfsId pnfsId ){
          _pnfsId = pnfsId.toString() ;
          
       }
       public void setBusy( boolean busy ){_busy = busy ;}
       public boolean isBusy(){ return _busy ; }
       public File getDataFile(){
          return new File( _dataDir , _pnfsId ) ;
       }
       public long getCreationTime(){ return _creationTime ; }
       public String toString(){
          return "m="+(_cached?"cached":
                       _precious?"precious":
                       (_receiving==STORE?"receiving.store":
                        _receiving==CLIENT?"receiving.client":
                        "<Invalid>" ) ) +
                        ";si={"+
                      (_storageInfo==null?"<unknown>":_storageInfo.toString())+"}" ;
       }
       public synchronized void addListener( CellMessage msg ){
          if( _listener == null )_listener = new Vector() ;
          _listener.addElement(msg) ;
       }
//       public void setSize(long size ){ _size = size ; }
//       public long getSize(){ return _size ; }
       public synchronized CellMessage nextListener(){ 
           if( ( _listener == null ) ||
               ( _listener.size() == 0 ) )return null ;
           CellMessage msg = (CellMessage)_listener.elementAt(0) ;
           _listener.removeElementAt(0);
           if( _listener.size() == 0 )_listener = null ;
           return msg ;
       }
       public  synchronized void setStorageInfo( StorageInfo info )
               throws CacheException{
           _setStorageInfo( info ) ;
       }
       private  synchronized void _setStorageInfo( StorageInfo info )
               throws CacheException{
       
           File f = new File( _controlDir , ".SI-"+_pnfsId ) ;
           ObjectOutputStream out = null ;
           try{
               
              out = new ObjectOutputStream(
                       new FileOutputStream(f) ) ;
              out.writeObject( info ) ;
           }catch(Exception e){
              f.delete() ;
              throw new
              CacheException(201,_pnfsId+" "+e.toString());
           }finally{
              try{ out.close() ; }catch(Exception we ){}
           }
           if( ! f.renameTo( new File( _controlDir , "SI-"+_pnfsId ) ) ){
              f.delete() ;
              throw new
              CacheException(202,"Rename failed : "+_pnfsId);
           }
           _storageInfo = info ;
       }
       public String getPnfsId(){ return _pnfsId ; }
       public StorageInfo getStorageInfo(){
           return _storageInfo ;
       }
       private StorageInfo _getStorageInfo()
               throws CacheException{
              
              
           File f = new File( _controlDir , "SI-"+_pnfsId ) ;
           ObjectInputStream in   = null ;
           StorageInfo       info = null ;
           try{
              in   = new ObjectInputStream(
                         new FileInputStream(f) ) ;
              info = (StorageInfo)in.readObject() ;
           }catch(Exception e ){
              throw new
              CacheException(201,_pnfsId+" "+e.toString());
           }finally{
              try{ in.close() ; }catch(Exception we ){}
           }
           _creationTime = f.lastModified() ;
           return info ;
       }
       public synchronized void update() throws CacheException {
           _read() ;
           try{
              _storageInfo = _getStorageInfo() ;
           }catch(Exception ee ){
           
           }
       }
       public synchronized void setPrecious() throws CacheException {
           _write( "precious" ) ;
           _precious = true ;
           _cached   = false ;
           _busy     = false ;
       }
       public synchronized void setReceiving(int source ) throws CacheException {
           if( source != CLIENT ){
              _write( "receiving.client" ) ; 
           }else if( source != STORE ){
              _write( "receiving.store" ) ;
           }else
              throw new
              IllegalArgumentException( "Illegal data source : "+source) ;
            
           _receiving = source ;
           _cached    = false ;
           _precious  = false ;
           _busy      = true ;
           return ;
       }
       public  synchronized void setCached() throws CacheException {
           _write( "cached" ) ;
           _cached    = true ;
           _receiving = 0 ;
           _precious  = false ;
           _busy      = false ;
           _size      = new File( _dataDir , _pnfsId.toString() ).length() ;
           _usedSpace += _size ;
       }
       public boolean isPrecious() throws CacheException {
          //_read() ;
          return _precious ; 
       }
       public int isReceiving() throws CacheException {
          //_read() ;
          return _receiving ; 
       }
       public boolean isCached() throws CacheException {
          //_read() ;
          return _cached ; 
       }
       private void destroy(){
         new File( _controlDir ,"SI-"+_pnfsId ).delete() ;
         new File( _controlDir ,".SI-"+_pnfsId ).delete() ;
         new File( _controlDir ,"."+_pnfsId ).delete() ;
         new File( _controlDir ,_pnfsId ).delete() ;
         new File( _dataDir ,_pnfsId ).delete() ;
       }
       private void _write( String str ) throws CacheException {
           PrintWriter pw = null ;
           File f  = new File( _controlDir , "."+_pnfsId ) ;
           File rf = new File( _controlDir , _pnfsId ) ;
           try{
              pw = new PrintWriter(
                     new FileWriter( f ) ) ;
              pw.println(str) ;
           }catch( IOException ioe ){
              f.delete() ;
               throw new 
               CacheException(206,ioe.toString() ) ;
           }finally{
              try{ pw.close() ; }catch(Exception ee){}
           }
           if( ! f.renameTo( rf ) ){
              f.delete() ;
              throw new 
              CacheException( 207,"Rename failed : "+_pnfsId ) ;
           }
       }
       private String _read() throws CacheException {
           File f = new File( _controlDir , _pnfsId ) ;
           BufferedReader br = null; 
           String line = null ;
           try{
              br = new BufferedReader(
                      new FileReader( f ) ) ;
              line = br.readLine() ;
           
           }catch(IOException e ){
               throw new 
               CacheException(205,e.toString() ) ;
           }finally{
               try{ br.close() ; }catch(Exception fe){}
           }
           if( ( line == null ) || ( line.length() == 0 ) )
              throw new
              CacheException( 206,"Illegal control file content (empty)" ) ;
           _receiving = 0 ;
           _precious  = false ;
           _cached    = false ;
           if( line.equals("precious") )_precious = true ;
           else if( line.equals("receiving.store" )  )_receiving = STORE ;
           else if( line.equals("receiving.client" ) )_receiving = CLIENT ;
           else if( line.equals("cached" ) )_cached = true ;
           else  
              throw new 
              CacheException( 210 , "Illegal Control State" ) ;

           return line ;
       }
    }
    public CacheRepository0( File baseDir ){
       _baseDir    = baseDir ;
       _controlDir = new File( _baseDir , "control" ) ;
       _dataDir    = new File( _baseDir , "data" ) ;
    }
    public synchronized void runInventory(){ runInventory(null) ; }
    public synchronized void runInventory( Logable log ){
       String [] list = _dataDir.list() ;
   
       if( log == null )log = new Logable(){
           public void log( String s){} ;
           public void elog(String s){} ;
           public void plog(String s){} ;
       } ;
       log.log( "runInventory : "+(list==null?"No":(""+list.length))+
                " datafile(s) found in "+_dataDir );
       
       CacheEntry entry = null ;
       String pnfsid  ;
       File   f ;
       for( int i = 0 ; i < list.length ; i++ ){
          pnfsid = list[i] ;
          if( ( pnfsid.length() != 24  ) ||
              ( pnfsid.charAt(0)!= '0' )   ){
              
             log.elog( pnfsid + " : ??? ") ;
             continue ;   
          }
          entry  = new CacheEntry( pnfsid ) ;
          try{
             entry.update() ;
             log.log( pnfsid + " : "+ entry ) ; 
             f = new File( _dataDir , pnfsid ) ;
             _usedSpace += f.length() ;
             _pnfsids.put( pnfsid , entry ) ;   
          }catch(Exception e ){
             log.elog( pnfsid + " : "+e.toString() ) ;
          }
       
       }
       log.log( "runInventory : #="+_pnfsids.size()+
                ";space="+_usedSpace+"/"+_totalSize );
       
    }
    public Enumeration pnfsids(){ return _pnfsids.keys() ; }
    public synchronized void setStorageInfo( String pnfsid , StorageInfo info )
           throws CacheException {
     
         CacheEntry e = getEntry( pnfsid ) ;
         e.setStorageInfo( info ) ;
         e.setPrecious() ;
         return ;
               
    }
    public File getDataFile( String pnfsId ){
       return new File( _dataDir , pnfsId ) ;
    }
    public synchronized CacheEntry createEntry( String pnfsId )
           throws CacheException{
        return createEntry( new PnfsId( pnfsId ) ) ;      
    }
    public synchronized CacheEntry createEntry( PnfsId pnfsId )
           throws CacheException{
        System.out.println( "Creating entry for : "+pnfsId ) ;
        CacheEntry entry = get( pnfsId.toString() ) ;
        if( entry != null )
           throw new
           FileInCacheException( "Entry already exists (mem) : "+pnfsId ) ;
    
        File f= new File( _controlDir , pnfsId.toString() ) ;
        if( f.exists() )
           throw new
           CacheException( 203,"Entry already exists (fs) : "+pnfsId ) ;
           
        try { new FileOutputStream(f).close() ; }
        catch( Exception ie ){
           f.delete() ;
           throw new
           CacheException( 204 ,"(fs) : "+ie ) ;
        }
        entry = new CacheEntry( pnfsId ) ;
        _pnfsids.put( pnfsId.toString() , entry ) ;
        return entry ;
    }
    /**
      *  !!! Removing an non existing entry returns 'true'.
      *  This behaviour must not be changed, because some
      *  clients depend on it. We only are allowed to return
      *  'false' is the entry exists and is busy.
      */
    public synchronized boolean destroyEntry( String pnfsId ){
    
        CacheEntry entry = (CacheEntry)_pnfsids.remove( pnfsId ) ;
       
        if( entry != null ){
           if( entry.isBusy() )return false ;
           entry.destroy() ;
           _usedSpace -= new File( _dataDir , pnfsId ).length() ;
        }
        return true ;
    }
    private CacheEntry get( String pnfsId ){
       return (CacheEntry)_pnfsids.get( pnfsId ) ;
    }
    public CacheEntry getEntry( String pnfsId )  throws CacheException {
       CacheEntry e = (CacheEntry)_pnfsids.get( pnfsId ) ;
       if( e ==  null )
         throw new 
         FileNotInCacheException( "Entry not in repository : "+pnfsId ) ;
       return e ;
    }
    public CacheEntry getEntry( PnfsId pnfsId )  throws CacheException {
       CacheEntry e = (CacheEntry)_pnfsids.get( pnfsId.toString() ) ;
       if( e ==  null )
         throw new 
         FileNotInCacheException( "Entry not in repository : "+pnfsId ) ;
       return e ;
    }
    public synchronized void addUsed( long space ){ _usedSpace += space ; }
    public synchronized long getTotalSpace(){ return _totalSize ; }
    public synchronized long getUsedSpace(){ return _usedSpace ; }
    public synchronized void setTotalSpace( long size ){ _totalSize = size ; }

}
