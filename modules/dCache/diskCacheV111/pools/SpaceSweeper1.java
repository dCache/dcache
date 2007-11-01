// $Id: SpaceSweeper1.java,v 1.1.22.1 2007-04-14 00:06:21 aik Exp $

package diskCacheV111.pools ;

import diskCacheV111.repository.* ;
import diskCacheV111.util.* ;
import diskCacheV111.util.event.* ;
import diskCacheV111.vehicles.StorageInfo ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;
import java.util.* ;
import java.text.SimpleDateFormat ;
import java.io.PrintWriter ;

public class SpaceSweeper1 implements SpaceSweeper , Runnable  {
    private CacheRepository _repository = null ;
    private CellAdapter     _cell       = null ;
    private PnfsHandler     _pnfs       = null ;
    private HsmStorageHandler2 _storage  = null ;

    private ArrayList       _list       = new ArrayList() ;

    private long            _spaceNeeded = 0 ;

    private long            _removableSpace = 0 ;

    private static SimpleDateFormat __format =
               new SimpleDateFormat( "HH:mm-MM/dd" ) ;

    public SpaceSweeper1( CellAdapter cell ,
                          PnfsHandler pnfs ,
                          CacheRepository repository ,
                          HsmStorageHandler2 storage     ){

       _repository = repository ;
       _cell       = cell ;
       _pnfs       = pnfs ;
       _storage    = storage ;

       _repository.addCacheRepositoryListener(this);
       _cell.getNucleus().newThread( this , "sweeper" ).start() ;
    }
    public long getRemovableSpace(){ return _removableSpace ; }
    public long getLRUSeconds(){
        CacheRepositoryEntry e;

        synchronized (this) {
            if (_list.size() == 0)
                return 0L;
            e = (CacheRepositoryEntry) _list.get(0);
        }

        try{
           return ( System.currentTimeMillis() - e.getLastAccessTime() ) / 1000L ;
        }catch(CacheException ee ){
           return 0L ;
        }
    }
    public void actionPerformed( CacheEvent event ){}
    public synchronized void precious( CacheRepositoryEvent event ){
       CacheRepositoryEntry entry = event.getRepositoryEntry() ;
       say("precious event : "+entry) ;
       try{

         if( _list.contains( entry ) )_removableSpace -= entry.getSize() ;

       }catch(CacheException ce ){
          esay(entry.getPnfsId().toString()+" : remove : can't get size "+ce);
          return ;
       }
       _list.remove( entry ) ;
    }
    public void sticky( CacheRepositoryEvent event ){
       //
       // the definition of the sticky callback garanties that
       // we are only called if something really changed.
       //
       boolean isSticky, isPrecious ;
       long    size ;
       CacheRepositoryEntry entry = event.getRepositoryEntry() ;
       try{
         isSticky   = entry.isSticky() ;
         isPrecious = entry.isPrecious() ;
         size       = entry.getSize() ;
       }catch(CacheException ce ){
         //
         // better to do nothing here.
         //
         esay(entry.getPnfsId().toString()+" : can't get status (sticky/size) "+ce ) ;
         return  ;
       }
       if( isSticky  ){
          say( "STICKY : received sticky event : "+entry) ;
          synchronized( this ){
             if( _list.contains( entry ) ){
                _list.remove( entry ) ;
                _removableSpace -= size ;
                say("STICKY : removed from list "+entry);
             }
          }
       }else{
          say("STICKY : received unsticky event : "+entry) ;
          if( ! isPrecious ){
             try{
                entry.touch() ;
             }catch(Exception ee){
                esay(entry.getPnfsId().toString()+" : can't touch"+ee ) ;
             }
             synchronized( this ){
                _list.add( entry ) ;
                _removableSpace += size ;
             }
             say("STICKY : added to remove list "+entry);
          }
       }
    }
    public void available( CacheRepositoryEvent event ){}
    public void created( CacheRepositoryEvent event ){}
    public void destroyed( CacheRepositoryEvent event ){}
    public synchronized void touched( CacheRepositoryEvent event ){

       CacheRepositoryEntry entry = event.getRepositoryEntry() ;
       try{
           entry.touch() ;
       }catch(Exception ee){}

       boolean found =_list.remove( entry ) ;

       try{
           if( found && ! entry.isSticky() )_list.add( entry ) ;
       }catch(CacheException ce ){
          esay( "can't determine stickyness : "+entry ) ;
          _list.add(entry);
       }
       say("touched event ("+found+") : "+entry) ;
    }
    public synchronized void removed( CacheRepositoryEvent event ){
       CacheRepositoryEntry entry = event.getRepositoryEntry() ;
       try{

         if( _list.contains( entry ) )_removableSpace -= entry.getSize() ;

       }catch(CacheException ce ){
          esay(entry.getPnfsId().toString()+" : remove : can't get size "+ce);
          return ;
       }
       _list.remove( entry ) ;
       say("removed event : "+entry) ;
    }
    public synchronized void needSpace( CacheNeedSpaceEvent event ){
       long space = event.getRequiredSpace() ;
       _spaceNeeded += space ;
       say("needSpace event "+space+" -> "+_spaceNeeded ) ;
    }
    public synchronized void scanned( CacheRepositoryEvent event ){
       CacheRepositoryEntry entry = event.getRepositoryEntry() ;
       try{

          synchronized( entry ){
             if( entry.isCached() && ! entry.isSticky()){
                _list.add( entry ) ;
                try{ _removableSpace += entry.getSize() ;}
                catch(Exception ee){
                  esay(entry.getPnfsId().toString()+" : scanned : "+ee ) ;
                }
             }
             say( "scanned event : "+entry ) ;
          }
       }catch(CacheException ce ){
          esay( "scanned event : CE "+ce.getMessage() ) ;
       }
    }
    public synchronized void cached( CacheRepositoryEvent event ){
       CacheRepositoryEntry entry = event.getRepositoryEntry() ;
       try{
          if( entry.isSticky() )return ;
          entry.touch();
          _list.add( entry ) ;
          say( "cached event : "+entry ) ;
          _removableSpace += entry.getSize() ;}
       catch(Exception ee){
          esay(entry.getPnfsId().toString()+" : cached : "+ee ) ;
       }
    }
    public String hh_sweeper_free = "<bytesToFree>" ;
    public String ac_sweeper_free_$_1( Args args )throws Exception {
       long toFree = Long.parseLong( args.argv(0) ) ;
       synchronized(this){
          _spaceNeeded += toFree ;
       }
       return ""+toFree+" bytes added to reallocation queue" ;
    }
    public String hh_sweeper_ls = " [-l] [-s]" ;
    public String ac_sweeper_ls( Args args )throws Exception {
       StringBuffer sb = new StringBuffer() ;
       boolean l = args.getOpt("l") != null ;
       boolean s = args.getOpt("s") != null ;
       ArrayList list = null ;
       synchronized( this ){
           list = new ArrayList( _list ) ;
       }
       for( int i = 0 ; i < list.size() ; i++ ){
          CacheRepositoryEntry entry = (CacheRepositoryEntry)list.get(i);
          if( l ){
             sb.append( Formats.field(""+i,3,Formats.RIGHT) ).append(" ") ;
             sb.append( entry.getPnfsId().toString() ).append("  ") ;
             sb.append( entry.getState() ).append("  ") ;
             sb.append( Formats.field(""+entry.getSize() , 11 , Formats.RIGHT ) ) ;
             sb.append(" ");
             sb.append(__format.format( new Date(entry.getCreationTime()))).append(" ");
             sb.append(__format.format( new Date(entry.getLastAccessTime()))).append(" ");
             StorageInfo info = entry.getStorageInfo() ;
             if( ( info != null ) && s )
                sb.append("\n    ").append(info.toString()) ;
             sb.append("\n");
          }else{
             sb.append(entry.toString()).append("\n");
          }
       }
       return sb.toString() ;
    }
    private String getTimeString( long secin ){
       int sec  = Math.max( 0 , (int)secin ) ;
       int min  =  sec / 60 ; sec  = sec  % 60 ;
       int hour =  min / 60 ; min  = min  % 60 ;
       int day  = hour / 24 ; hour = hour % 24 ;

       String sS = Integer.toString( sec ) ;
       String mS = Integer.toString( min ) ;
       String hS = Integer.toString( hour ) ;

       StringBuffer sb = new StringBuffer() ;
       if( day > 0 )sb.append(day).append(" d ");
       sb.append( hS.length() < 2 ? ( "0"+hS ) : hS ).append(":");
       sb.append( mS.length() < 2 ? ( "0"+mS ) : mS ).append(":");
       sb.append( sS.length() < 2 ? ( "0"+sS ) : sS ) ;

       return sb.toString() ;
    }
    public String hh_sweeper_get_lru = "[-f] # return lru in seconds [-f means formatted]" ;
    public String ac_sweeper_get_lru( Args args ){
       long lru = getLRUSeconds() ;
       boolean f = args.getOpt("f") != null ;
       return f ? getTimeString(lru) : ( ""+lru ) ;
    }
    public void run(){
       say( "started");
       long spaceNeeded = 0  ;
       CacheRepositoryEntry entry = null ;
       ArrayList  tmpList = new ArrayList() ;

       while( ! Thread.currentThread().interrupted() ){
           try{
               Thread.currentThread().sleep(10000) ;
           }catch(InterruptedException e){
               break ;
           }
           //
           // take the needed space out of the 'queue' and
           // added to the managed space.
           //
           synchronized( this ){
               spaceNeeded += _spaceNeeded ;
              _spaceNeeded = 0 ;
           }
           if( spaceNeeded <= 0 )continue ;

           say("SS0 request to remove : "+spaceNeeded ) ;
           //
           // we copy the entries into a tmp list to avoid
           // the ConcurrentModificationExceptions
           //
           Iterator i = _list.iterator() ;
           try{

              long minSpaceNeeded = spaceNeeded ;

              while( i.hasNext() && ( minSpaceNeeded > 0 ) ){

                 entry = (CacheRepositoryEntry)i.next() ;
                 //
                 //  we are not allowed to remove the
                 //  file is
                 //    a) it is locked
                 //    b) it is still in use.
                 //
                 try{
                    if( entry.isLocked() ||
                        ( entry.getLinkCount() > 0 ) ||
                        entry.isSticky()                  ){
                       esay( "SS0 : file skipped by remove (locked,in use,sticky) : "+entry);
                       continue ;
                    }
                    if( entry.isPrecious() ){
                       esay( "SS0 : PANIC file skipped by remove (precious) : "+entry);
                       continue;
                    }
                    long size = entry.getSize() ;
                    tmpList.add( entry ) ;
                    minSpaceNeeded -= size ;
                    say( "SS0 adds to remove list : "+entry.getPnfsId()+
                         " "+size+" -> "+spaceNeeded ) ;
                    //
                    // the _list space will be substracted with the
                    // remove event.
                    //
                 }catch(FileNotInCacheException fce ){
                    esay("SS0 : "+fce ) ;
                    _list.remove(entry);
                 }catch(CacheException ce ){
                    esay("SS0 : "+ce ) ;
                 }
              }
           }catch(ConcurrentModificationException cme){
              esay("SS0 (loop exited, this is not an error) : "+cme ) ;
           }
           //
           // we are not supposed to do exact space allocation.
           //

           //
           // now do it
           //
           i = tmpList.iterator() ;
           try{
              while( i.hasNext() ){

                 entry = (CacheRepositoryEntry)i.next() ;
                 try{
                    long size = entry.getSize() ;
                    say( "SS0 : trying to remove "+entry.getPnfsId()) ;
                    if( _repository.removeEntry( entry ) ){
                        spaceNeeded -= size ;
                    }else{
                        say("SS0 : looked (not removed) : "+entry.getPnfsId() ) ;
                        continue ;
                    }
                    //
                    // the _list space will be substracted with the
                    // remove event.
                    //
                 }catch(FileNotInCacheException fce ){
                    esay("SS0 : "+fce ) ;
                    synchronized(this){ _list.remove(entry); }
                 }catch(CacheException ce ){
                    esay("SS0 : "+ce ) ;
                 }
              }
           }catch(ConcurrentModificationException cme){
              esay("SS0 (loop2 exited) "+cme ) ;
           }
           say( "SS0 loop done [cleaning tmp]");
           spaceNeeded = Math.max( spaceNeeded , 0 ) ;
           tmpList.clear() ;
       }
       _repository.removeCacheRepositoryListener(this);
       say("SS0 : finished");
    }
    public void printSetup( PrintWriter pw ){
       pw.println( "#\n# Nothing from the "+this.getClass().getName()+"#" ) ;
    }
    private void say( String msg ){
       _cell.say( "SWEEPER : "+msg ) ;
    }
    private void esay( String msg ){
       _cell.esay( "SWEEPER ERROR : "+msg ) ;
    }


}
