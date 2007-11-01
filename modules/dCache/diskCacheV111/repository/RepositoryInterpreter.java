// $Id: RepositoryInterpreter.java,v 1.21.14.1 2007-04-13 08:24:11 tigran Exp $

package diskCacheV111.repository ;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;
import dmg.util.*;
import java.util.* ;

public class RepositoryInterpreter {
    private CacheRepository _repository = null ;
    public RepositoryInterpreter( CacheRepository repository ){
       _repository = repository ;
    }
    private void displayEntry( CacheRepositoryEntry entry , StringBuffer sb )throws CacheException {
       String status = 
           entry.isPrecious()?"precious":
           entry.isCached()?"cached":
           entry.isReceivingFromClient()?"Recv. from client":
           entry.isReceivingFromStore()? "Recv. from store":
           "<undefined>" ;
       StorageInfo info = entry.getStorageInfo() ;
       if( info != null ){
          sb.append(Formats.field(info.getHsm(),8,Formats.LEFT)) ;
          String sClass = info.getStorageClass() ;
          sClass = sClass == null ? "-" : sClass ;
          String cClass = info.getCacheClass() ;
          cClass = cClass == null ? "-" : cClass ;
          sb.append(Formats.field(sClass,20,Formats.LEFT)).
             append(Formats.field(cClass,20,Formats.LEFT)) ;
       }
       sb.append( status+(entry.isLocked()?"(locked)":"") ) ;
    }
    public String hh_rep_set_sticky = "<pnfsid> on|off" ;
    public String ac_rep_set_sticky_$_2( Args args ) throws CacheException {
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;
       String state   = args.argv(1) ;
       
       CacheRepositoryEntry entry = _repository.getEntry( pnfsId ) ;
       if( state.equals("on" ) ){
          entry.setSticky(true);
       }else if( state.equals("off") ){
          entry.setSticky(false) ;
       }else
          throw new
          IllegalArgumentException( "invalid sticky state : "+state ) ;
          
       return "" ;
    }
    public String hh_rep_set_bad = "<pnfsid> on|off" ;
    public String ac_rep_set_bad_$_2( Args args ) throws CacheException {
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;
       String state   = args.argv(1) ;
       
       CacheRepositoryEntry entry = _repository.getEntry( pnfsId ) ;
       if( state.equals("on" ) ){
          entry.setBad(true);
       }else if( state.equals("off") ){
          entry.setBad(false) ;
       }else
          throw new
          IllegalArgumentException( "invalid 'bad' state : "+state ) ;
          
       return "" ;
    }
    public String fh_rep_ls = 
       "\n"+
       " Format I  :  [<pnfsId> [<pnfsId> [...]]]\n"+
       " Format II : -l[=<selectionOptions>] [-s]\n"+
       "              Options :\n"+
       "              -l[=splunc]  # selected list\n"+
       "                 s  : sticky files\n"+
       "                 p  : precious files\n"+
       "                 l  : locked files\n"+
       "                 u  : files in use\n"+
       "                 nc : files which are not cached\n" +
       "                 e  : files which error condition\n" +
       "              -s[=kmgt]       # statistics\n" +
       "                 k  : data amount in KBytes\n"+
       "                 m  : data amount in MBytes\n"+
       "                 g  : data amount in GBytes\n"+
       "                 t  : data amount in TBytes\n";
    public String hh_rep_ls = "[-l[=s,l,u,nc,p]] [-s[=kmgt]] | [<pnfsId> [...] ]" ;
    public Object ac_rep_ls_$_0_99( Args args ) throws Exception{
       boolean      more = args.getOpt("l") != null ;
       StringBuffer sb   = new StringBuffer() ;
       if( args.argc() == 0 ){
          
          String stat = args.getOpt("s") ;
          if( stat != null ){
             long dev = 1 ;
             dev = ( stat.indexOf("k") > -1 ) ||
                   ( stat.indexOf("K") > -1 ) ? 1024L : dev ;
             dev = ( stat.indexOf("m") > -1 ) ||
                   ( stat.indexOf("M") > -1 ) ? (1024L*1024L) : dev ;
             dev = ( stat.indexOf("g") > -1 ) ||
                   ( stat.indexOf("G") > -1 ) ? (1024L*1024L*1024L) : dev ;
             dev = ( stat.indexOf("t") > -1 ) ||
                   ( stat.indexOf("T") > -1 ) ? (1024L*1024L*1024L*1024L) : dev ;
             Iterator  e     = _repository.pnfsids() ; 
             HashMap   map   = new HashMap() ; 
             long [] counter = null ; 
             PnfsId  pnfsId  = null ;
             String  sc      = null ;
             CacheRepositoryEntry entry = null ;
             StorageInfo          info  = null ;
             while( e.hasNext() ){
                pnfsId = (PnfsId)e.next() ;
                entry  = _repository.getGenericEntry(pnfsId) ;
                //
                // info can be null if we are 'static' or a write
                // pool and the storageInfo has not arrived.
                //
                if( ( info = entry.getStorageInfo() ) == null )continue ;
                sc     = info.getStorageClass() + "@" +
                         info.getHsm() ;
                if( (counter = (long [])map.get(sc)) == null )
                     map.put(sc,counter = new long[2]) ;
                counter[0] += info.getFileSize() ;
                counter[1] ++ ;
             }
             e = map.keySet().iterator() ;
             if( args.getOpt("binary") != null ){
                Object [] result = new Object[map.size()];
                for( int i = 0 ; e.hasNext() ; i++ ){
                   Object [] ex =  new Object[2] ; 
                   ex[0]  = (String)e.next() ;
                   ex[1]  = (long [])map.get(ex[0]) ;
                   result[i] = ex ;
                }
                return result ;
             }else{
                while( e.hasNext() ){
                   sc = (String)e.next() ;
                   counter = (long [])map.get(sc) ;
                   sb.append(Formats.field(sc,24,Formats.LEFT)).
                      append("  ").
                      append(Formats.field(""+counter[0]/dev,10,Formats.RIGHT)).
                      append("  ").
                      append(Formats.field(""+counter[1],8,Formats.RIGHT)).
                      append("\n") ;
                }
                return sb.toString() ;
             }
          }
          String format = args.getOpt("l") ;
          format = format == null ? "" : format ;
          
          boolean notcached = format.indexOf("nc") > -1 ;
          boolean precious  = format.indexOf('p')  > -1 ;
          boolean locked    = format.indexOf('l')  > -1 ;
          boolean sticky    = format.indexOf('s')  > -1 ;
          boolean used      = format.indexOf('u')  > -1 ;
          boolean bad       = format.indexOf('e')  > -1 ;
          
          Iterator     e    = _repository.pnfsids() ;     
          while( e.hasNext() ){
             PnfsId pnfsid = (PnfsId)e.next() ;
             CacheRepositoryEntry entry = _repository.getGenericEntry(pnfsid) ;
             try{
                if( format.length() == 0  ){
                  sb.append(entry.toString() ).append("\n") ;
                }else if( notcached && ! entry.isCached() ){
                  sb.append(entry.toString() ).append("\n") ;
                }else if( locked && entry.isLocked() ){
                  sb.append(entry.toString() ).append("\n") ;
                }else if( precious && entry.isPrecious() ){
                  sb.append(entry.toString() ).append("\n") ;
                }else if( sticky && entry.isSticky() ){
                  sb.append(entry.toString() ).append("\n") ;
                }else if( bad && entry.isBad() ){
                  sb.append(entry.toString() ).append("\n") ;
                }else if( used && ( entry.getLinkCount() > 0 ) ){
                  sb.append(entry.toString() ).append("\n") ;
                }
             }catch(Exception ce ){
                sb.append(entry.getPnfsId().toString()).
                   append(" : ").
                   append(ce.getMessage() ).
                   append("\n") ;
             }
          }
       }else{
          for( int i = 0 ; i < args.argc() ; i++ ){
             PnfsId pnfsid = new PnfsId( args.argv(i) ) ;
             CacheRepositoryEntry entry = _repository.getGenericEntry(pnfsid) ;
             try{
                sb.append(entry.toString()) ;
             }catch(Exception ce ){
                sb.append(pnfsid.toString()).
                   append(" : ").
                   append(ce.getMessage() ) ;
             }
             sb.append("\n") ;
          }
       
       }
       return sb.toString() ;
    }
    public String hh_rep_rmclass = "<storageClass> # removes the from the cache" ;
    public String ac_rep_rmclass_$_1( Args args )throws Exception {
       String storageClassName = args.argv(0);
       Iterator  e     = _repository.pnfsids() ;
       final ArrayList   map   = new ArrayList() ;
       PnfsId  pnfsId  = null ;
       String  sc      = null ;
       CacheRepositoryEntry entry = null ;
       StorageInfo          info  = null ;
       while( e.hasNext() ){
          pnfsId = (PnfsId)e.next() ;
          entry  = _repository.getGenericEntry(pnfsId) ;
          //
          // info can be null if we are 'static' or a write
          // pool and the storageInfo has not arrived.
          //
          if( ( info = entry.getStorageInfo() ) == null )continue ;
          sc     = info.getStorageClass()  ;
          if( sc.equals(storageClassName) )map.add(entry) ;
       }
       new Thread( 
          new Runnable(){
             public void run(){
                try{
                   Iterator i = map.iterator() ;
                   while(i.hasNext()){
                      CacheRepositoryEntry e = (CacheRepositoryEntry)i.next();
                      if( e.isReceivingFromClient() || e.isReceivingFromStore() )continue;
                      _repository.removeEntry(e);
                      Thread.currentThread().sleep(100);
                   }
                }catch(Exception ee ){
                }
             }
          } , "Background-remove" ).start() ;
       return "Backgrounded ( removing "+map.size()+" entries)"  ;

    }
    public String fh_rep_rm = 
      " rep rm <pnfsid> [-force]\n" +
      "        removes the <pnfsid> from the cache repository.\n"+
      "        The file is only removed if in 'cached' state, which\n"+
      "        means it is not precious and not in an receiving state.\n"+
      "  -force overwrites this protection and tries to remove the file\n"+
      "         in any case. If the link count is not yet 0, the file\n"+
      "         exists until zero is reached.\n"+
      "  SEE ALSO :\n"+
      "     rep rmclass ...\n";
    public String hh_rep_rm = "<pnfsid> [-force]# removes the pnfsfile from the cache" ;
    public String ac_rep_rm_$_1_( Args args )throws Exception {
       boolean forced = args.getOpt("force") != null ;
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;
       CacheRepositoryEntry entry = _repository.getEntry( pnfsId ) ;
       int client = 0 ;
       if( forced || entry.isCached() ){
           boolean rc = _repository.removeEntry( _repository.getEntry(pnfsId) ) ;
           return rc ? ( "Removed "+pnfsId ) : ( "Failed to remove "+pnfsId) ;
       }else if( entry.isPrecious() ){
           throw new
           CacheException( 13 , "Pnfsid is still precious : "+pnfsId ) ;
       }else if( entry.isReceivingFromClient() || entry.isReceivingFromStore() ){
           throw new
           CacheException( 13 , "Pnfsid is still receiving  : "+pnfsId ) ;
       } 
       throw new
           CacheException( 13 , "Pnfsid is in an undefined state : "+pnfsId ) ;
    }
    public String hh_rep_set_precious = "<pnfsId> [-force]" ;
    public String ac_rep_set_precious_$_1( Args args ) throws CacheException {
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;
       CacheRepositoryEntry entry = _repository.getEntry( pnfsId ) ;

       entry.setPrecious( args.getOpt("force") != null ) ;
       
       return "" ;
    }
    public String hh_rep_set_cached = "<pnfsId> # DON'T USE , Potentially dangerous" ;
    public String ac_rep_set_cached_$_1( Args args ) throws CacheException {
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;
       CacheRepositoryEntry entry = _repository.getEntry( pnfsId ) ;

       entry.setCached() ;
       
       return "" ;
    }
    public String hh_rep_lock = "<pnfsId> [on | off | <time/sec>]" ;
    public String ac_rep_lock_$_1_2( Args args )throws Exception {
        long time = 0 ;
        boolean lock = false ;
        if( args.argc() < 2 ){
           lock = true ;
           time = 0 ;
        }else{
           String value = args.argv(1) ;
           if( value.equals("on") || value.equals("off") ){
              lock = value.equals("on") ;
           }else{
              time = (long)( Integer.parseInt(value) * 1000 ) ;
           }
        }
        synchronized( _repository ){
           CacheRepositoryEntry entry = _repository.getEntry( new PnfsId(args.argv(0)));
           if( time > 0 )entry.lock( time ) ;
           else entry.lock( lock ) ;
        }
        return "Done";
    }
    public String hh_rep_sr_ls = "" ;
    public String ac_rep_sr_ls( Args args ){
       return " Reserved Space : "+_repository.getReservedSpace() ;
    }
    public String hh_rep_sr_reserve = "[-blocking] <Space to reserve (bytes)>" ;
    public String ac_rep_sr_reserve_$_1( Args args ) 
           throws CacheException , 
                  InterruptedException {
       
       boolean blocking = args.getOpt("blocking") != null ;
       _repository.reserveSpace( Long.parseLong( args.argv(0) ) , blocking ) ;
       return "" ;
    }
    public String hh_rep_sr_free = "<Space to free from reserve (bytes)>" ;
    public String ac_rep_sr_free_$_1( Args args ) throws CacheException {
       
       _repository.freeReservedSpace( Long.parseLong( args.argv(0) ) );
       return "" ;
    }
    public String hh_rep_sr_apply = "<Space to apply from reserve (bytes)>" ;
    public String ac_rep_sr_apply_$_1( Args args ) throws CacheException {
       
       _repository.applyReservedSpace( Long.parseLong( args.argv(0) ) );
       return "" ;
    }
}
