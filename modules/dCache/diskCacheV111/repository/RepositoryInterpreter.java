// $Id: RepositoryInterpreter.java,v 1.25 2007-10-16 19:28:45 behrmann Exp $

package diskCacheV111.repository ;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;
import dmg.util.*;
import java.util.* ;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.cell.CellCommandListener;

import org.apache.log4j.Logger;

public class RepositoryInterpreter
    implements CellCommandListener
{
    private final static Logger _log =
        Logger.getLogger(RepositoryInterpreter.class);
    private CacheRepository _repository;

    public RepositoryInterpreter(CacheRepository repository)
    {
        _repository = repository;
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
    public String hh_rep_set_sticky = "[-o=<owner>] [-l=<lifetime in ms>] <pnfsid> on|off" ;
    public String ac_rep_set_sticky_$_2( Args args ) throws CacheException {
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;
       String state   = args.argv(1) ;
       String owner = "system";
       if( args.getOpt("o") != null) {
           owner=args.getOpt("o");
       }

       long lifetime = -1;
       if(args.getOpt("l") != null) {
           lifetime= System.currentTimeMillis()+Long.parseLong(args.getOpt("l"));
       }

       CacheRepositoryEntry entry = _repository.getEntry( pnfsId ) ;
       if( state.equals("on" ) ){
           entry.setSticky(owner, lifetime, true);

       }else if( state.equals("off") ){
           entry.setSticky(owner, 0, true);
       }else
          throw new
          IllegalArgumentException( "invalid sticky state : "+state ) ;

       return "" ;
    }

    public String hh_rep_sticky_ls = "<pnfsid>" ;
    public String ac_rep_sticky_ls_$_1( Args args ) throws CacheException {
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;
       CacheRepositoryEntry entry = _repository.getEntry( pnfsId ) ;
        List<StickyRecord> records = entry.stickyRecords();
        StringBuffer sb = new StringBuffer();
        for(StickyRecord record: records) {
            sb.append(record).append('\n');
        }
       return sb.toString() ;
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
       "              -s[=kmgt] [-sum]       # statistics\n" +
       "                 k  : data amount in KBytes\n"+
       "                 m  : data amount in MBytes\n"+
       "                 g  : data amount in GBytes\n"+
       "                 t  : data amount in TBytes\n"+
       " Output is a list of repository entries, one per line\n" +
       " each line has the followin syntax:\n" +
       "<pnfsid> <state> <size> <storageinfo>\n"+
       " state is a sequence of state bits inclosed in angular \"<>\" brackets \n"+
       " bit 1 is \"C\" if entry is cached or \"-\" if not \n"+
       " bit 2 is \"P\" if entry is precious or \"-\" if not \n"+
       " bit 3 is \"C\" if entry is being transfered \"from client\" or \"-\" if not \n"+
       " bit 4 is \"S\" if entry is being transfered \"from store\" or \"-\" if not \n"+
       " bit 5 is \"c\" if entry is being transfered \"to client\" or \"-\" if not \n"+
       " bit 6 is \"s\" if entry is being transfered \"to store\" or \"-\" if not \n"+
       " bit 7 is \"R\" if entry is removed or \"-\" if not \n"+
       " bit 8 is is always \"-\" \n"+
       " bit 9 is \"X\" if entry is sticky or \"-\" if not \n"+
       " bit 10 is \"E\" if entry is in error state or \"-\" if not \n"+
       " bit 11 is \"L(x)(y)\" if entry is in locked or \"-\" if not \n"+
       "        x is epoch until which the entry is locked, 0 for non expiring lock \n"+
       "        y is the link count";
        
    public String hh_rep_ls = "[-l[=s,l,u,nc,p]] [-s[=kmgt]] | [<pnfsId> [...] ]" ;
    public Object ac_rep_ls_$_0_99(final Args args) throws Exception
    {
        if (args.argc() > 0) {
            StringBuilder sb   = new StringBuilder();
            for (int i = 0 ; i < args.argc(); i++) {
                PnfsId pnfsid = new PnfsId(args.argv(i));
                CacheRepositoryEntry entry = _repository.getGenericEntry(pnfsid);
                try {
                    sb.append(entry.toString());
                } catch(Exception ce) {
                    sb.append(pnfsid.toString()).
                        append(" : ").
                    append(ce.getMessage());
                }
                sb.append("\n");
            }
            return sb.toString();
        }

        final DelayedReply reply = new DelayedReply();
        Thread task = new Thread() {
                void reply(Object o)
                {
                    try {
                        reply.send(o);
                    } catch (NoRouteToCellException e) {
                        _log.error("Failed to send reply for 'rep ls': " + e);
                    } catch (InterruptedException e) {
                        _log.warn("Interrupted while sending reply: " + e);
                        Thread.currentThread().interrupt();
                    }
                }

                public void run()
                {
                    try {
                        StringBuilder sb = new StringBuilder();
                        String stat = args.getOpt("s");
                        if (stat != null) {
                            long dev = 1;
                            dev = (stat.indexOf("k") > -1) ||
                                (stat.indexOf("K") > -1) ? 1024L : dev;
                            dev = (stat.indexOf("m") > -1) ||
                                (stat.indexOf("M") > -1) ? (1024L*1024L) : dev;
                            dev = (stat.indexOf("g") > -1) ||
                                (stat.indexOf("G") > -1) ? (1024L*1024L*1024L) : dev;
                            dev = (stat.indexOf("t") > -1) ||
                                (stat.indexOf("T") > -1) ? (1024L*1024L*1024L*1024L) : dev;

                            Iterator<PnfsId>       e1  = _repository.pnfsids();
                            HashMap<String,long[]> map = new HashMap<String,long[]>();
                            long removable = 0L;
                            while (e1.hasNext()) {
                                CacheRepositoryEntry entry = _repository.getGenericEntry(e1.next());
                                //
                                // info can be null if we are 'static' or a write
                                // pool and the storageInfo has not arrived.
                                //
                                StorageInfo info = entry.getStorageInfo();
                                if (info == null) continue;
                                String sc = info.getStorageClass() + "@" + info.getHsm();

                                long[] counter = map.get(sc);
                                if (counter == null)
                                    map.put(sc, counter = new long[8]);

                                long entrySize = info.getFileSize();

                                counter[0] += entrySize;
                                counter[1]++;
                                if (entry.isPrecious()) {
                                    counter[2] += entrySize;
                                    counter[3]++;
                                }
                                if (entry.isSticky()) {
                                    counter[4] += entrySize;
                                    counter[5]++;
                                }
                                if (!entry.isSticky() && !entry.isPrecious()) {
                                    counter[6] += entrySize;
                                    removable  += entrySize;
                                    counter[7]++;
                                }
                            }
                            if (args.getOpt("sum") != null) {
                                long[] counter = new long[10];
                                map.put("total", counter);
                                counter[0] = _repository.getTotalSpace();
                                counter[1] = _repository.getFreeSpace();
                                counter[2] = removable;
                            }

                            Iterator<String> e2 = map.keySet().iterator();
                            if (args.getOpt("binary") != null) {
                                Object [] result = new Object[map.size()];
                                for (int i = 0; e2.hasNext(); i++) {
                                    Object[] ex =  new Object[2];
                                    ex[0]  = e2.next();
                                    ex[1]  = map.get(ex[0]);
                                    result[i] = ex;
                                }
                                reply(result);
                                return;
                            }

                            while (e2.hasNext()) {
                                String sc = e2.next();
                                long[] counter = map.get(sc);
                                sb.append(Formats.field(sc,24,Formats.LEFT)).
                                    append("  ").
                                    append(Formats.field(""+counter[0]/dev,10,Formats.RIGHT)).
                                    append("  ").
                                    append(Formats.field(""+counter[1],8,Formats.RIGHT)).
                                    append("  ").
                                    append(Formats.field(""+counter[2]/dev,10,Formats.RIGHT)).
                                    append("  ").
                                    append(Formats.field(""+counter[3],8,Formats.RIGHT)).
                                    append("  ").
                                    append(Formats.field(""+counter[4]/dev,10,Formats.RIGHT)).
                                    append("  ").
                                    append(Formats.field(""+counter[5],8,Formats.RIGHT)).
                                    append("  ").
                                    append(Formats.field(""+counter[6]/dev,10,Formats.RIGHT)).
                                    append("  ").
                                    append(Formats.field(""+counter[7],8,Formats.RIGHT)).
                                    append("\n") ;
                            }
                        } else  {
                            String format = args.getOpt("l");
                            format = format == null ? "" : format;

                            boolean notcached = format.indexOf("nc") > -1;
                            boolean precious  = format.indexOf('p')  > -1;
                            boolean locked    = format.indexOf('l')  > -1;
                            boolean sticky    = format.indexOf('s')  > -1;
                            boolean used      = format.indexOf('u')  > -1;
                            boolean bad       = format.indexOf('e')  > -1;

                            Iterator<PnfsId> e = _repository.pnfsids();
                            while (e.hasNext()) {
                                CacheRepositoryEntry entry = _repository.getGenericEntry(e.next());
                                try {
                                    if (format.length() == 0) {
                                        sb.append(entry.toString()).append("\n");
                                    } else if (notcached && !entry.isCached()) {
                                        sb.append(entry.toString()).append("\n");
                                    } else if (locked && entry.isLocked()) {
                                        sb.append(entry.toString()).append("\n");
                                    } else if (precious && entry.isPrecious()) {
                                        sb.append(entry.toString()).append("\n");
                                    } else if (sticky && entry.isSticky()) {
                                        sb.append(entry.toString()).append("\n");
                                    } else if (bad && entry.isBad()) {
                                        sb.append(entry.toString()).append("\n");
                                    } else if (used && (entry.getLinkCount() > 0)) {
                                        sb.append(entry.toString()).append("\n");
                                    }
                                } catch (Exception ce) {
                                    sb.append(entry.getPnfsId().toString()).
                                        append(" : ").
                                        append(ce.getMessage()).
                                        append("\n");
                                }
                            }
                        }
                        reply(sb.toString());
                    } catch (CacheException e) {
                        reply(e);
                    }
                }
            };
        task.start();
        return reply;
    }

    public String hh_rep_rmclass = "<storageClass> # removes the from the cache";
    public String ac_rep_rmclass_$_1(Args args) throws Exception
    {
        final String storageClassName = args.argv(0);
        new Thread(new Runnable() {
                public void run()
                {
                    try {
                        List<CacheRepositoryEntry> map =
                            new ArrayList<CacheRepositoryEntry>();
                        Iterator<PnfsId> i
                            = _repository.pnfsids();
                        while (i.hasNext()) {
                            PnfsId pnfsId = i.next();
                            CacheRepositoryEntry entry =
                                _repository.getGenericEntry(pnfsId);
                            //
                            // info can be null if we are 'static' or a write
                            // pool and the storageInfo has not arrived.
                            //
                            StorageInfo info = entry.getStorageInfo();
                            if (info != null) {
                                String sc = info.getStorageClass();
                                if (sc.equals(storageClassName)) map.add(entry);
                            }
                        }

                        for (CacheRepositoryEntry e : map) {
                            if (!e.isReceivingFromClient() && !e.isReceivingFromStore()) {
                                _repository.removeEntry(e);
                                Thread.currentThread().sleep(100);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            } , "Background-remove" ).start();
        return "Backgrounded";
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
           _log.error("Repository Interpreter: removing "+pnfsId+" by admin request");
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

    protected boolean isWriting(PnfsId id) throws CacheException
    {
        CacheRepositoryEntry entry = _repository.getEntry(id);
        return (entry.isReceivingFromClient() || entry.isReceivingFromStore());
    }

    public String hh_rep_set_precious = "<pnfsId> [-force]" ;
    public String ac_rep_set_precious_$_1( Args args ) throws CacheException {
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;

       if (isWriting(pnfsId)) {
           return "File is still being written. The state has not been changed.";
       }

       CacheRepositoryEntry entry = _repository.getEntry( pnfsId ) ;

       entry.setPrecious( args.getOpt("force") != null ) ;

       return "" ;
    }
    public String hh_rep_set_cached = "<pnfsId> # DON'T USE , Potentially dangerous" ;
    public String ac_rep_set_cached_$_1( Args args ) throws CacheException {
       PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;

       if (isWriting(pnfsId)) {
           return "File is still being written. The state has not been changed.";
       }

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
