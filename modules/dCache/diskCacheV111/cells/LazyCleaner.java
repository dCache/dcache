package diskCacheV111.cells ;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.io.* ;
import  java.util.*;


/**
  * LazyCleaner - checks for file in the "trash" directory.
  * If a file was found, the LazyCleaner sends a PoolRemoveFilesMessage
  * to all pools which haves files with the specified pnfsID.
  */


public class LazyCleaner extends CellAdapter {

    private String _cellName      = null;
    private CellNucleus _nucleus  = null ;
    private Args   _args          = null;
    private String _dirName       = null;
    private File   _trashLocation = null;
    
    private Thread _cleaningThread    = null;
    private int	   _refreshInterval   = 20;
    private int    _replyTimeout      = 5000 ;
    private int    _removeCounter     = 0 ;
    private int    _unremoveCounter   = 0 ;
    private int    _lastTimeRemoveCounter   = 0 ;
    private int    _lastTimeUnremoveCounter = 0 ;
    private int    _exceptionCounter  = 0 ;
    private int    _loopCounter       = 0 ;
    
    private Date   _started = new Date() ;
    
    public LazyCleaner( String cellName , String args ) throws Exception {
    
	super( cellName , args , false ) ;
	
	_cellName = cellName ;
	_args     = getArgs() ;
	_nucleus  = getNucleus() ;
        
	useInterpreter( true ) ;
	
	try {
	
            //  Usage : ... [-trash=<trashLocationDirectory>]
            //              [-refresh=<refreshInterval_in_sec.>]
             
            //
            // find the trash. If not specified do some filesystem
            // inspections.
            //     
            String refreshString = _args.getOpt("refresh") ;
            if( refreshString != null ){
               try{
                   _refreshInterval = Integer.parseInt( refreshString ) ;
               }catch(Exception ee ){}
            }    
            say("Refresh Interval set to "+_refreshInterval+" seconds");
            
            if( ( ( _dirName = _args.getOpt("trash") ) == null ) ||
                ( _dirName.equals("")                          )    ){
               say("'trash' not defined. Starting autodetect" ) ;
               _dirName = autodetectTrash() ;
            }
            say("'trash' set to "+_dirName) ;
	    _trashLocation = new File(_dirName);
	    
	    if( ! _trashLocation.isDirectory() ){
                say("'trash' not a directory : "+_dirName ) ;
		throw new 
                IllegalArgumentException("'trash' not a directory : "+_dirName ) ;
            }
	    _cleaningThread = _nucleus.newThread( new CleaningThread() ,"cleaning" ) ;
	   
	} catch (Exception e){
	    say ("Exception occurred: "+e);
	    start();
	    kill();
	    throw e;
	}
	
	
	_cleaningThread.start();
	
	// we don't need to be well known
	// getNucleus().export();
	start() ;
    }	

    public void getInfo( PrintWriter pw ){
	pw.println("LazyCleaner");
	pw.println("  Trash location    : " + _dirName ) ; 
	pw.println("  Refresh interval  : " + _refreshInterval);
	pw.println("  ReplyTimeout (ms) : " + _replyTimeout);
	pw.println("  Started           : " + _started);
	pw.println("  Loops             : " + _loopCounter);
	pw.println("  Files removed     : " + _removeCounter+" / "+_unremoveCounter);
	pw.println("  Files removed (l) : " + _lastTimeRemoveCounter+
                   " / "+_lastTimeUnremoveCounter);
	pw.println("  Exception         : " + _exceptionCounter);
    }
    public String toString(){
       return "Removed="+_removeCounter+";X="+_exceptionCounter ;
    }
    public void say( String str ){
	pin( str ) ;
	super.say( str ) ;
    }
    public void esay( String str ){
	pin( str ) ;
	super.esay( str ) ;
    }
    public void esay( Exception e ){
	pin( e.toString() ) ;
	super.esay( e ) ;
    }
    private String autodetectTrash() throws Exception {
       File pnfsSetup = new File( "/usr/etc/pnfsSetup" ) ;
       if( ! pnfsSetup.exists() )
          throw new Exception( "Not a pnfsServer" ) ;
       if( ! pnfsSetup.canRead() )
          throw new Exception( "Can't read pnfsSetup file" ) ;

       BufferedReader br = new BufferedReader( 
                              new FileReader( pnfsSetup ) ) ;
       String line = null ;
       try{
           while( ( line = br.readLine() ) != null ){
              StringTokenizer st = new StringTokenizer(line,"=") ;
              try{
                 String key   = st.nextToken() ;
                 String value = st.nextToken() ;
                 if( key.equals("trash") && ( value.length() > 0 ) )
                    return value+"/2" ;
              }catch(Exception ee ){
              }
           }
       }catch(EOFException eof ){
       
       }finally{
           try{ br.close() ; }catch(Exception eeee ){}
       }
       throw new
       Exception( "'trash' not found in pnfsSetup" ) ;
    }
    private String [] cleanPool( String poolName, PnfsEntry pnfsEntryList [] ) 
        throws Exception {
    
        String [] pnfsList = new String[pnfsEntryList.length] ;
        
        for( int i = 0 ; i < pnfsList.length ; i++ )
             pnfsList[i] = pnfsEntryList[i].getPnfsId();
             
        PoolRemoveFilesMessage msg = new PoolRemoveFilesMessage(poolName);

        msg.setFiles( pnfsList );
        
        CellMessage cellMessage = new CellMessage( new CellPath( poolName ) , msg ) ;
        
        if( ( cellMessage = sendAndWait( cellMessage , _replyTimeout ) ) == null )
           throw new
           Exception( "PoolRemoveFilesMessage timed out" ) ;

        Object reply = cellMessage.getMessageObject() ;
        
        if( reply instanceof PoolRemoveFilesMessage ){
           PoolRemoveFilesMessage prfm = (PoolRemoveFilesMessage)reply ;
           if( prfm.getReturnCode() == 0 )return null ;
           Object o = prfm.getErrorObject() ;
           
           if( o instanceof String [] )return (String [])o ;
           
           throw new
           Exception( "Pool reports : "+(o==null?"<UnspecifiedFailure>":o.toString()));
        }
        
        if( reply instanceof Exception ){
           esay( "cleanPool got : "+reply ) ;
           throw (Exception)reply ;
        }
        throw new 
        Exception( "Weird reply from remove <"+reply.getClass().getName()+"> "+reply ) ;
        
    }
   private class PnfsEntry {
      private String _pnfsId    = null ;
      private File   _filename  = null ;
      private int    _linkCount = 0 ;
      private PnfsEntry( String pnfsId , File filename ){
         _pnfsId   = pnfsId ;
         _filename = filename ;      
      }
      private String getPnfsId(){ return _pnfsId ; }
      private File   getFile(){ return _filename ; }
      private void addLink(){ _linkCount ++ ; }
      private boolean removeLink(){ _linkCount -- ; return _linkCount <= 0 ; }
      public boolean equals( Object obj ){
         if( ! ( obj instanceof PnfsEntry ) )return false ;
         return ((PnfsEntry)obj)._pnfsId.equals( _pnfsId ) ;
      }
      public int hashCode(){ return _pnfsId.hashCode() ; }
      private int getLinkCount(){ return _linkCount ; }
   }
   private class CleaningThread implements Runnable  {
      private HashMap _poolList = null ;
      private HashMap _pnfsIds  = null ;
        
      private void createNet(){
          _poolList = new HashMap();
          _pnfsIds  = new HashMap() ;
      
          //
          // get some files from the trash directory and
          // build the linked lists.
          //
          String [] fileList = _trashLocation.list(
             //
             // should prevent running out of memory (100 files max)
             // we check syntax by using new PnfsId.
             //
             new FilenameFilter(){
               int counter = 0 ;
               public boolean accept( File dir , String name ){
                  if( counter > 100  )return false ;
                  try{ new PnfsId(name) ; }catch(Exception e){return false ;}
                  counter ++ ;
                  return true ;
               }
             } 
          ) ;
          for( int i = 0; i <  fileList.length; i++) {
      
              File file         = new File(_trashLocation, fileList[i]);	
              BufferedReader br = null ;
              int lineCounter   = 0;
              String s          = null ;
              PnfsEntry entry   = null ;		 
              
              say( "Processing "+fileList[i]);
              try{     
                 br = new BufferedReader( new FileReader( file ) );
              }catch( Exception ioe ){
                 esay( "IO error while opening file "+file) ;
                 esay(ioe);
                 continue ;
              }
              if( ( entry = (PnfsEntry)_pnfsIds.get( fileList[i] ) ) == null ){
                 entry = new PnfsEntry( fileList[i] , file ) ;
                 _pnfsIds.put( entry.getPnfsId() , entry ) ;
              }
                    
              try{
                 while( ( s = br.readLine() ) != null ) {
                    //
	            // first line contains statistics only,
                    // so skip it.
                    //
	            if( lineCounter++ == 0 )continue ;
                    //
	            // pool name is the first field in the line.
	            // fields are separated by ','
                    //
                    int pos = s.indexOf(',') ;
	            String poolID = ( pos == -1 ? s : s.substring(0, pos) ).trim() ;
                    
                    if( poolID.length() < 1 )continue ;
                    
                    List l = (List)_poolList.get(poolID) ;
                    if( l  == null )_poolList.put(poolID,l = new ArrayList() );
                    l.add(entry) ;
                    entry.addLink() ;
		 }
              }catch(Exception ee ){
                 _exceptionCounter ++ ;
		 esay( "Io or syntax error in [" + fileList[i] + "] (not removed) : "+ee );
              }finally{
		 try{ br.close(); }catch(Exception closee ){}
              }
           }
      }	
      public  void run() {
	
         
         
         while( ! Thread.currentThread().interrupted() ) {
            _loopCounter ++ ;
            
            try{
               // take a rest till next time 
               Thread.currentThread().sleep( _refreshInterval*1000 );
               
               createNet() ;
               if( _poolList.size() > 0 ){
                  _lastTimeUnremoveCounter = 0 ;
                  _lastTimeRemoveCounter   = 0 ;
               }
               //
               // now walk the pools and send the remove message
               //
               Iterator walk = _poolList.keySet().iterator() ;
               while( walk.hasNext() ){
                  String [] unremoved = null ;
                  String poolID = (String)walk.next();
                 	
                  ArrayList arrayList = (ArrayList)_poolList.get(poolID) ;
                  
                  Object    [] objArray      = arrayList.toArray() ;                 
                  PnfsEntry [] pnfsEntryList = new PnfsEntry[objArray.length] ;
                  
                  System.arraycopy( objArray , 0 , pnfsEntryList , 0 , objArray.length );

                  try{
                  
                     unremoved = cleanPool( poolID , pnfsEntryList );
                     		       
                  }catch( Exception cpe ){
                     esay( "CleanPool Method reported Exception "+cpe);
                     esay(cpe);
                     continue ;
                  }
                  //
                  if( ( unremoved == null ) || ( unremoved.length == 0 ) ){
                     //
                     // all files have been accepted, so decrement them all
                     //
                     for( int i = 0 ; i < pnfsEntryList.length ; i++ )
                        pnfsEntryList[i].removeLink() ;
                  }else{
                     say("Unremove for "+poolID+
                         " returned with "+unremoved.length+" entries");
                         
                     HashSet set = new HashSet() ;
                     
                     for( int i = 0 ; i < unremoved.length ; i++ ){
                        set.add(unremoved[i]) ;
                        say("Unremove : "+unremoved[i]);  
                     }
                        
                     for( int i = 0 ; i < pnfsEntryList.length ; i++ ){
                        if( set.contains( pnfsEntryList[i].getPnfsId() ) ){
                           continue ;
                        }
                        pnfsEntryList[i].removeLink() ;
                     }
                  
                  }
               }
               //
               // we only remove files with 0 linkcount
               //
               walk = _pnfsIds.values().iterator() ;
               while( walk.hasNext() ){
                  PnfsEntry entry = (PnfsEntry)walk.next() ;
                  if( entry.getLinkCount() > 0 ){
                     say( "Non zero link count ("+entry.getLinkCount()+") for "+entry.getPnfsId() ) ;
                      _unremoveCounter ++ ;
                      _lastTimeUnremoveCounter ++ ;
                     continue ;
                  }
                  File file = entry.getFile() ;
                  if( ! file.delete() ){
                      esay( "Couldn't delete file "+file ) ;
                      _exceptionCounter ++ ;
                  }else{
                      say("File deleted : "+file);
                      _removeCounter ++ ;
                      _lastTimeRemoveCounter ++ ;
                  }
               }


            }catch( InterruptedException e){
                esay( "Cleaning Thread got interrupted" ) ;
                break ;
            }catch( Exception e){
                _exceptionCounter ++ ;
	        esay( "Cleaning Thread failed." ) ;
	        esay(e);
                break ;
            }
        }
        say ( "Cleaning Thread finished" ) ;
      }    
    
    }  // end of CleaningThread


}  // end of LazyCleaner
