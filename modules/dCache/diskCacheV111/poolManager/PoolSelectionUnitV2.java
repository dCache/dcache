// $Id: PoolSelectionUnitV2.java,v 1.12.2.1 2006-09-07 12:05:05 patrick Exp $
package diskCacheV111.poolManager ;

import java.util.* ;
import java.net.* ;
import diskCacheV111.util.* ;
import dmg.util.* ;
import java.util.regex.*;

public class PoolSelectionUnitV2  implements PoolSelectionUnit {
    
    private static final String __version = "$Id: PoolSelectionUnitV2.java,v 1.12.2.1 2006-09-07 12:05:05 patrick Exp $" ;
    
    public  String getVersion(){ return __version ; }
    
    private static final int STORE    = 1 ;
    private static final int DCACHE   = 2 ;
    private static final int NET      = 3 ;
    private static final int PROTOCOL = 4 ;
    
    private HashMap _pGroups = new HashMap() ;
    private HashMap _pools   = new HashMap() ;
    private HashMap _links   = new HashMap() ;
    private HashMap _uGroups = new HashMap() ;
    private HashMap _units   = new HashMap() ;
    private boolean _useRegex       = false;
    private boolean _allPoolsActive = false ;

    
    private NetHandler _netHandler = new NetHandler() ;
    
    private class NetHandler {
        private HashMap [] _netList     = new HashMap[33] ;
        private String  [] _maskStrings = new String[33] ;
        private long    [] _masks       = new long[33] ;
        private NetHandler(){
            long mask   = 0 ;
            long xmask  = 0 ;
            long cursor = 1 ;
            for( int i = 0 ; i < _maskStrings.length ; i++ ){
                
                _masks[i] = xmask = ~ mask ;
                
                int a = (int) ( ( xmask >> 24 ) & 0xff ) ;
                int b = (int) ( ( xmask >> 16 ) & 0xff ) ;
                int c = (int) ( ( xmask >>  8 ) & 0xff ) ;
                int d = (int) ( ( xmask  ) & 0xff ) ;
                
                _maskStrings[i] = a+"."+b+"."+c+"."+d ;
                
                mask |= cursor ;
                cursor <<= 1 ;
            }
        }
        private void clear(){
            for(int i = 0 ; i <_netList.length ; i++ )
                if( _netList[i] != null )_netList[i].clear() ;
        }
        private long inetAddressToLong( InetAddress address ){
            byte [] raw  = address.getAddress() ;
            long    addr = 0L ;
            
            for( int i = 0 ; i < raw.length ; i++ ){
                addr <<= 8 ;
                addr |=  ((int)raw[i])&0xff ;
            }
            return addr ;
        }
        private String longAddressToString( long addr ){
            int a = (int) ( ( addr >> 24 ) & 0xff ) ;
            int b = (int) ( ( addr >> 16 ) & 0xff ) ;
            int c = (int) ( ( addr >>  8 ) & 0xff ) ;
            int d = (int) ( ( addr  ) & 0xff ) ;
            
            return a+"."+b+"."+c+"."+d ;
        }
        private void add(  NetUnit net ){
            int bit = net.getHostBits() ;
            if( _netList[bit] == null )_netList[bit] = new HashMap() ;
            
            long addr = inetAddressToLong( net.getHostAddress() ) ;
            _netList[bit].put( new Long( addr & _masks[bit] ) , net ) ;
        }
        private void remove( NetUnit net ){
            
            int bit = net.getHostBits() ;
            if( _netList[bit] == null )return ;
            
            long addr = inetAddressToLong( net.getHostAddress() ) ;
            
            _netList[bit].remove( new Long( addr ) ) ;
            if( _netList.length == 0 )_netList[bit] = null ;
        }
        private NetUnit find( NetUnit net ){
            
            int bit = net.getHostBits() ;
            if( _netList[bit] == null )return null ;
            
            long addr = inetAddressToLong( net.getHostAddress() ) ;
            
            return (NetUnit)_netList[bit].get( new Long( addr & _masks[bit]) ) ;
        }
        private NetUnit match( String inetAddress )throws UnknownHostException {
            long    addr = inetAddressToLong( InetAddress.getByName( inetAddress ) ) ;
            Map     map  = null ;
            long    mask     = 0 ;
            long    cursor   = 1 ;
            NetUnit unit     = null ;
            for( int i = 0 ; i < _netList.length ; i++ ){
                if( ( map = _netList[i] ) != null ){
                    Long l = new Long( addr & ~ mask ) ;
                    //                System.out.println("Trying to find : "+Long.toHexString(l.longValue()) ) ;
                    unit = (NetUnit)map.get( l ) ;
                    if( unit != null )return unit ;
                }
                mask   |= cursor ;
                cursor <<= 1 ;
            }
            return null ;
        }
        private long bitsToMask( int bits ){
            return _masks[bits] ;
        }
        private String bitsToString( int bits ){
            return _maskStrings[bits];
        }
    }
    protected class PoolCore {
        
        protected String _name       = null ;
        protected Map    _linkList   = new HashMap() ;
        
        protected PoolCore( String name ){ _name = name ; }
        public String getName(){ return _name ; }
    }
    private class PGroup extends PoolCore {
        
        private Map    _poolList = new HashMap() ;
        
        private PGroup( String name ){ super(name) ; }
        public String toString(){
            return _name+"  (links="+_linkList.size()+";pools="+_poolList.size()+")" ;
        }
    }
    private class Pool extends PoolCore implements SelectionPool {
        
        private Map     _pGroupList = new HashMap() ;
        private boolean _enabled    = true ;
        private long    _active     = 0L ;
        private boolean _ping       = true ;
        private long    _serialId   = 0L ;
        private boolean _rdOnly     = false ;
 
        private Pool( String name ){ super(name) ; }
        public void setActive( boolean active ){
            _active = ( ! _ping ) ? 0L : active ? System.currentTimeMillis() : 100000000L ; }
        public long getActive(){
            return  _ping ? ( System.currentTimeMillis() - _active ) : 0L;
        }
        public void setReadOnly( boolean rdOnly ){ _rdOnly = rdOnly ; }
        public boolean isReadOnly(){ return _rdOnly ; }
        public void setEnabled( boolean enabled ){ _enabled = enabled ; }
        public boolean isEnabled(){ return _enabled ; }
        public void setPing( boolean ping ){ _ping = ping ; }
        public boolean isPing(){ return _ping ; }
        public String toString(){
            return _name+
            "  (enabled="+_enabled+
            ";active="+(getActive()/1000)+
            ";rdOnly="+isReadOnly()+
            ";links="+_linkList.size()+
            ";pgroups="+_pGroupList.size()+")" ;
        }
        public boolean setSerialId( long serialId ){
            if( serialId == _serialId )return false ;
            _serialId = serialId ;
            return true ;
        }
    }
    private class Link implements SelectionLink {
        
        private String _name       = null ;
        private Map    _poolList   = new HashMap() ;
        private Map    _uGroupList = new HashMap() ;
        
        private int    _readPref  = 0 ;
        private int    _writePref = 0 ;
        private int    _cachePref = 0 ;
        private int    _p2pPref   = -1 ;
        private String _tag       = null; 
        
        public String getName(){ return _name ; }
        private Link( String name ){ _name = name ; }
        public String toString(){
            return _name+"  (pref="+_readPref+"/"+
            _cachePref+"/"+
            _p2pPref+"/"+
            _writePref+";"+
            (_tag==null?"":_tag)+";"+
            "ugroups="+_uGroupList.size()+
            ";pools="+_poolList.size()+")" ;
        }
        public String getAttraction(){
            return "-readpref="+_readPref+
            " -writepref="+_writePref+
            " -cachepref="+_cachePref+
            " -p2ppref="+_p2pPref +
            ( _tag == null ? "" : " -section="+_tag ) ;
        }
        public Iterator pools(){
            ArrayList list        = new ArrayList() ;
            Iterator  poolObjects = _poolList.values().iterator() ;
            
            while( poolObjects.hasNext() ){
                
                Object o = poolObjects.next() ;
                
                if( o instanceof Pool ){
                    list.add( o ) ;
                }else if( o instanceof PGroup ){
                    Iterator pools = ((PGroup)o)._poolList.values().iterator() ;
                    while( pools.hasNext() )list.add( pools.next() ) ;
                    
                }
            }
            return list.iterator() ;
        }
        public boolean exec( Map variableMap ){ return true ; }
    }
    private class UGroup {
        private String _name     = null ;
        private Map    _linkList = new HashMap() ;
        private Map    _unitList = new HashMap() ; // !!! DCache, STore, Net names must
        //     be different
        private UGroup( String name ){ _name = name ; }
        private String getName(){ return _name ; }
        public String toString(){
            return _name+"  (links="+_linkList.size()+";units="+_unitList.size()+")" ;
        }
    }
    private class Unit {
        
        private String _name       = null ;
        private int    _type       = 0 ;
        private Map    _uGroupList = new HashMap() ;
        
        private Unit( String name , int type ){
            _name   = name ;
            _type   = type  ;
        }
        public String getName(){ return _name ; }
        public String getCanonicalName(){ return getName() ; }
        protected void setName( String name ){ _name = name ; }
        private String getType(){
            return _type==STORE?"Store":
                   _type==DCACHE?"DCache":
                   _type==PROTOCOL?"Protocol":
                   _type==NET?"Net":"Unknown" ;
        }
        public String toString(){
            return _name+
            "  (type="+getType()+
            ";canonical="+getCanonicalName()+
            ";uGroups="+_uGroupList.size()+
            ")" ;
        }
    }
   public synchronized String [] getDefinedPools( boolean enabledOnly ){
      Iterator pools = _pools.values().iterator() ;
      ArrayList list = new ArrayList() ;
      while( pools.hasNext() ){
         Pool pool = (Pool)pools.next() ;
         if( ( ! enabledOnly ) || pool.isEnabled() )list.add( pool.getName() ) ;
      }
      String [] result = new String[list.size()] ;
      Object [] x      = list.toArray() ;
      for( int i = 0 ; i < result.length ; i++ )result[i] = (String)x[i];
      return result ;
   }
    public synchronized String [] getActivePools(){
        Iterator pools = _pools.values().iterator() ;
        ArrayList list = new ArrayList() ;
        while( pools.hasNext() ){
            Pool pool = (Pool)pools.next() ;
            if( pool.isEnabled() && ( pool.getActive() < (5*60*1000) ) )
                list.add( pool.getName() ) ;
        }
        String [] result = new String[list.size()] ;
        Object [] x      = list.toArray() ;
        for( int i = 0 ; i < result.length ; i++ )result[i] = (String)x[i];
        //      System.arraycopy( list.toArray() , 0 , result , 0 , result.length ) ;
        return result ;
    }
    public synchronized void dumpSetup( StringBuffer sb ){
        sb.append( "#\n# Printed by ").
        append( this.getClass().getName() ).
        append( " at " ).
        append( new Date().toString() ).
        append( "\n#\n#\n");
        sb.append( "#\n# The units ...\n#\n");
        Iterator units = _units.values().iterator() ;
        while( units.hasNext() ){
            Unit unit = (Unit)units.next() ;
            int type = unit._type ;
            sb.append( "psu create unit ").
            append( type == STORE    ? "-store " :
                    type == DCACHE   ? "-dcache" :
                    type == PROTOCOL ? "-protocol" : 
                                       "-net   " ).
            append( " ").
            append( unit.getName() ).
            append( "\n" ) ;
        }
        sb.append( "#\n# The unit Groups ...\n#\n");
        Iterator groups = _uGroups.values().iterator() ;
        while( groups.hasNext() ){
            UGroup group = (UGroup)groups.next() ;
            sb.append( "psu create ugroup " ).
            append( group.getName() ).
            append( "\n" );
            units = group._unitList.values().iterator() ;
            while( units.hasNext() ){
                Unit unit = (Unit)units.next() ;
                sb.append("psu addto ugroup ").
                append( group.getName() ).
                append( " ").
                append( unit.getName() ).
                append( "\n" ) ;
            }
        }
        sb.append( "#\n# The pools ...\n#\n");
        Iterator pools = _pools.values().iterator() ;
        while( pools.hasNext() ){
            Pool pool = (Pool)pools.next() ;
            sb.append( "psu create pool " ).
            append( pool.getName() ) ;
            
            if( ! pool.isPing() )sb.append( " -noping" ) ;
            if( ! pool.isEnabled() )sb.append( " -disabled" ) ;
            
            sb.append("\n" ) ;
        }
        sb.append( "#\n# The pool groups ...\n#\n");
        groups = _pGroups.values().iterator() ;
        while( groups.hasNext() ){
            PGroup group = (PGroup)groups.next() ;
            sb.append( "psu create pgroup " ).
            append( group.getName() ).
            append( "\n" ) ;
            pools = group._poolList.values().iterator() ;
            while( pools.hasNext() ){
                Pool pool = (Pool)pools.next() ;
                sb.append( "psu addto pgroup ").
                append( group.getName() ).
                append( " ").
                append( pool.getName() ).
                append( "\n" ) ;
            }
        }
        sb.append( "#\n# The links ...\n#\n");
        Iterator links = _links.values().iterator() ;
        while( links.hasNext() ){
            Link link = (Link)links.next() ;
            sb.append( "psu create link " ).
            append( link.getName() ) ;
            groups = link._uGroupList.values().iterator() ;
            while( groups.hasNext() ){
                sb.append(" ").append( ((UGroup)groups.next()).getName() );
            }
            sb.append( "\n" ) ;
            sb.append( "psu set link " ).
            append( link.getName() ).
            append( " ").
            append( link.getAttraction() ).
            append("\n");
            Iterator poolCores = link._poolList.values().iterator() ;
            while( poolCores.hasNext() ){
                PoolCore poolCore = (PoolCore)poolCores.next() ;
                sb.append( "psu add link ").
                append( link.getName() ).
                append( " " ).
                append( poolCore.getName() ).
                append( "\n" );
            }
        }
    }
    private class NetUnit extends Unit {
        private InetAddress _address = null ;
        private long        _mask    = 0 ;
        private int         _bits    = 0 ;
        private String      _canonicalName = null ;
        private NetUnit( String name ) throws UnknownHostException {
            super( name , NET ) ;
            
            int n = name.indexOf( '/' ) ;
            if( n < 0 ){
                //
                // no netmask found (is -host)
                //
                _address = InetAddress.getByName(name) ;
                //
            }else{
                if( ( n == 0 ) || ( n == ( name.length()-1) ) )
                    throw new
                    IllegalArgumentException( "host or net part missing") ;
                
                String hostPart = name.substring( 0 , n ) ;
                String netPart  = name.substring( n + 1 ) ;
                
                //
                //count hostbits
                //
                byte [] raw = InetAddress.getByName( netPart ).getAddress();
                _mask = ((((int)raw[0])&0xff) << 24 ) |
                ((((int)raw[1])&0xff) << 16 ) |
                ((((int)raw[2])&0xff) <<  8 ) |
                (((int)raw[3])&0xff) ;
                long cursor = 1 ;
                _bits       = 0 ;
                for( _bits =  0 ; _bits < 32 ; _bits ++ ){
                    if( ( _mask & cursor ) > 0 )break ;
                    cursor <<= 1 ;
                }
                _address = InetAddress.getByName( hostPart ) ;
            }
            _canonicalName = _address.getHostAddress()+"/"+_netHandler.bitsToString(_bits) ;
        }
        public int getHostBits(){ return _bits ; }
        public InetAddress getHostAddress(){ return _address ; }
        public String getCanonicalName(){ return _canonicalName ; }
    }
    private class StoreUnit extends Unit {
        private StoreUnit( String name ){
            super( name , STORE ) ;
        }
    }
    private class ProtocolUnit extends Unit {
       private String _protocol = null ;
       private int    _version  = -1 ;
       private ProtocolUnit( String name ){
          super( name , PROTOCOL ) ;
          int pos = name.indexOf("/") ;
          if( ( pos < 0 ) || ( pos == 0 ) || ( ( name.length() - 1 ) == pos ) )
             throw new
             IllegalArgumentException("Wrong format for protocol unit <protocol>/<version>");

          _protocol = name.substring(0,pos);
          String version = name.substring(pos+1) ;
          try{
             _version  = version.equals("*") ? -1 : Integer.parseInt( version ) ;
          }catch(Exception ee ){
             throw new
              IllegalArgumentException("Wrong format : Protocol version must be * or numerical");
          }
       }
       public String getName(){ return _protocol+( _version > -1 ? ( "/"+_version ) : "/*" ) ; }
    }
    public PoolSelectionUnitV2(){
        try{
            
            HashSet set = new HashSet() ;
            
            set.add( "hallo" ) ;
            StringBuffer sb = new StringBuffer() ;
            sb.append("hallo");
            
            //       System.out.println("Contains : "+set.contains( sb.toString() ) ) ;
        }catch(Exception ee ){
            ee.printStackTrace() ;
        }
    }
    public synchronized void clear(){
        _netHandler.clear() ;
        _pGroups.clear() ;
        _pools.clear() ;
        _links.clear() ;
        _uGroups.clear() ;
        _units.clear() ;
        
    }
    public synchronized void setActive( String poolName , boolean active ){
        Pool pool = (Pool)_pools.get( poolName ) ;
        if( pool != null )pool.setActive( active ) ;
        return ;
    }
    public synchronized long getActive( String poolName ){
        Pool pool = (Pool)_pools.get(poolName) ;
        return pool == null ? 100000000L : pool.getActive() ;
    }
    public synchronized void setEnabled( String poolName , boolean enabled ){
        Pool pool = (Pool)_pools.get( poolName ) ;
        if( pool != null )pool.setEnabled( enabled ) ;
        return ;
    }
    public synchronized boolean isEnabled( String poolName ){
        Pool pool = (Pool)_pools.get(poolName) ;
        return pool == null ? false : pool.isEnabled() ;
    }
    public synchronized SelectionPool getPool( String poolName ){
        return (SelectionPool)_pools.get(poolName) ;
    }
    public synchronized SelectionPool getPool( String poolName , boolean create ){
       Pool pool = (Pool)_pools.get(poolName) ;
       if( ( pool != null ) ||
           ( ( pool == null ) && ( ! create ) )  )return pool ;


       pool = new Pool(poolName);
       _pools.put( pool.getName() , pool ) ;

       try{
          addtoPoolGroup( "default" , poolName ) ;
       }catch(Exception e ){
          return null ;
       }
       return (SelectionPool)pool ;
    }

    public synchronized Map match( Map map , Unit unit ) {
        
        Map newmap = match( unit ) ;
        if( map == null )return newmap ;
        
        Map resultMap  = new HashMap() ;
        Iterator links = map.values().iterator() ;
        while( links.hasNext() ){
            Link link = (Link)links.next() ;
            if( newmap.get( link.getName() ) != null )
                resultMap.put( link.getName() , link ) ;
        }
        return resultMap ;
    }
    private class LinkMap {
        private class LinkMapEntry {
            private Link _link ;
            private int  _counter = 0 ;
            private LinkMapEntry( Link link ){
                _link    = link ;
                _counter = link._uGroupList.size() - 1 ;
            }
            private void touch(){ _counter-- ; }
            private boolean isTriggered(){ return _counter < 1 ; }
        }
        private Map _linkHash = new HashMap() ;
        private Iterator iterator(){
            ArrayList list = new ArrayList() ;
            Iterator i = _linkHash.values().iterator() ;
            while( i.hasNext() ){
                LinkMapEntry e = (LinkMapEntry)i.next() ;
                if( e._counter <= 0 )list.add( e._link ) ;
            }
            return list.iterator() ;
        }
        private void addLink( Link link ){
            LinkMapEntry found = (LinkMapEntry)_linkHash.get( link.getName() ) ;
            if( found == null ){
                _linkHash.put( link.getName() ,  new LinkMapEntry(link) ) ;
            }else{
                found._counter-- ;
            }
        }
    }
    private synchronized LinkMap match( LinkMap linkMap , Unit unit ){
        Map      map   = match( unit ) ;
        Iterator links = map.values().iterator() ;
        while( links.hasNext() )linkMap.addLink( (Link)links.next() ) ;
        return linkMap ;
    }
    private class LinkComparator implements Comparator {
        private int _type = 0 ;
        private LinkComparator( String type ){
            _type =
               type.equals("read")  ? 0 :
               type.equals("cache") ? 1 : 
               type.equals("write") ? 2 : 3 ;
        }
        public int compare( Object o1 , Object o2 ){
            Link link1 = (Link)o1 ;
            Link link2 = (Link)o2 ;
            switch( _type ){
                case 0 : // read
                    return link1._readPref == link2._readPref ? link1._name.compareTo(link2._name) :
                        link1._readPref >  link2._readPref ? -1 : 1 ;
                case 1 : // cache
                    return link1._cachePref == link2._cachePref ? link1._name.compareTo(link2._name) :
                        link1._cachePref >  link2._cachePref ? -1 : 1 ;
                case 2 : // write
                    return link1._writePref == link2._writePref ? link1._name.compareTo(link2._name) :
                        link1._writePref >  link2._writePref ? -1 : 1 ;
                case 3 : // p2p
                  int pref1 = link1._p2pPref < 0 ? link1._readPref : link1._p2pPref ;
                  int pref2 = link2._p2pPref < 0 ? link2._readPref : link2._p2pPref ;
                  return pref1 == pref2 ? link1._name.compareTo(link2._name) :
                         pref1 >  pref2 ? -1 : 1 ;
            }
            throw new
            IllegalArgumentException("Wrong comparator mode" ) ;
        }
    }
    /*
    public synchronized List[] match( String type ,
    String storeUnitName ,
    String dCacheUnitName ,
    String netUnitName  ,
    Map    variableMap     ) {
        return match( type , storeUnitName , dCacheUnitName , netUnitName , null , variableMap ) ;
    }
    */
    public synchronized PoolPreferenceLevel [] 
         match( String type ,
                String storeUnitName ,
                String dCacheUnitName ,
                String netUnitName  ,
                String protocolUnitName  ,
                Map    variableMap         ) {
        //
        // resolve the unit from the unitname (or net unit mask)
        //
        //regex code added by rw2 12/5/02
        //original code is in the else
        //
        ArrayList list = new ArrayList() ;
        if( storeUnitName != null ){
            if( _useRegex ) {
                Iterator unitIterator = _units.values().iterator();
                Unit unit = null;
                Unit universalCoverage = null;
                Unit classCoverage =null;
                //System.out.println("using regex");
                while( unitIterator.hasNext() ) {
                    unit = (Unit) unitIterator.next();
                    if( unit._type != STORE )continue ;
                    //System.out.println("iterating: " + unit.getName());
                    if( unit.getName().equals("*@*") ) {
                        universalCoverage = unit;
                    }
                    else if( unit.getName().equals("*@"+storeUnitName) ){
                        classCoverage = unit;
                    }
                    else {
                        if( Pattern.matches(unit.getName(), storeUnitName)) {
                            list.add(unit);
                            break;
                        }
                    }
                }
                //
                //If a pattern matches then use it, fail over to a class,
                //then universal.  If nothing, throw exception
                //
                if(list.size() == 0) {
                    if( classCoverage != null) {
                        list.add(classCoverage);
                    }
                    else if( universalCoverage != null){
                        list.add(universalCoverage);
                    }
                    else {
                        throw new
                        IllegalArgumentException("Unit not found : "+storeUnitName ) ;
                    }
                }
                
            }else{
                Unit unit = (Unit)_units.get( storeUnitName ) ;
                if( unit == null ){
                    int ind = storeUnitName.lastIndexOf("@") ;
                    if( ( ind > 0 ) && ( ind < (storeUnitName.length()-1) ) ){
                        String template = "*@"+storeUnitName.substring(ind+1) ;
                        if( ( unit = (Unit)_units.get( template ) ) == null ){
                            
                            if( ( unit = (Unit)_units.get("*@*") ) == null )
                                throw new
                                IllegalArgumentException("Unit not found : "+storeUnitName ) ;
                        }
                    }else{
                        throw new
                        IllegalArgumentException("IllegalUnitFormat : "+storeUnitName ) ;
                    }
                }
                list.add( unit ) ;
            }
        }
        if( protocolUnitName != null ){
        
           Unit unit = findProtocolUnit( protocolUnitName ) ;
           //
           if( unit == null )
              throw new
              IllegalArgumentException("Unit not found : "+protocolUnitName ) ;
           //
           list.add( unit ) ;
        }
        if( dCacheUnitName != null ){
            Unit unit = (Unit)_units.get( dCacheUnitName ) ;
            if( unit == null )
                throw new
                IllegalArgumentException("Unit not found : "+dCacheUnitName ) ;
            list.add( unit ) ;
        }
        if( netUnitName != null ){
            try{
                Unit unit = (Unit)_netHandler.match( netUnitName ) ;
                if( unit == null )
                    throw new
                    IllegalArgumentException("Unit not matched : "+netUnitName ) ;
                list.add( unit ) ;
            }catch(UnknownHostException uhe ){
                throw new
                IllegalArgumentException("NetUnit not resolved : "+netUnitName ) ;
            }
        }
        //System.out.println("PSUDEBUG : list of units : "+list );
        //
        // match the requests ( logical AND )
        //
        //
        // Map      map   = null ;
        // while( units.hasNext() )map = match( map , (Unit)units.next() ) ;
        // Iterator links     = map.values().iterator() ;
        //
        Iterator units = list.iterator() ;
        LinkMap  map   = new LinkMap() ;
        while( units.hasNext() )map = match( map , (Unit)units.next() ) ;
        
        
        //
        //   i) sort according to the type (read,write,cache)
        //  ii) the and is only OK if we have at least as many
        //      units (from the arguments) as required by the
        //      number of uGroupList(s).
        // iii) check for the hashtable if required.
        //
        int      fitCount  = list.size() ;
        TreeSet  sortedSet = new TreeSet( new LinkComparator(type) ) ;        
        for( Iterator links = map.iterator() ; links.hasNext() ; ){
        
            Link link = (Link)links.next() ;
            //System.out.println( "PSUDEBUG  link : "+link.toString() ) ;
            if( (     link._uGroupList.size() <= fitCount             ) &&
                ( ( variableMap == null ) || link.exec( variableMap ) )    ){
                
                sortedSet.add( link ) ;
                //System.out.println( "PSUDEBUG  added : "+link);
            }
        }
        int       pref     = -1 ;
        ArrayList listList = new ArrayList() ;
        ArrayList current  = null ;
        Iterator  links    = sortedSet.iterator() ;
        
        if( type.equals( "read" ) ){
            while( links.hasNext() ){
                Link link = (Link)links.next() ;
                if( link._readPref < 1 )continue ;
                if( link._readPref != pref ){
                    listList.add( current = new ArrayList() ) ;
                    pref = link._readPref ;
                }
                current.add( link ) ;
            }
        }else if( type.equals( "cache" ) ){
            while( links.hasNext() ){
                Link link = (Link)links.next() ;
                if( link._cachePref < 1 )continue ;
                if( link._cachePref != pref ){
                    listList.add( current = new ArrayList() ) ;
                    pref = link._cachePref ;
                }
                current.add( link ) ;
            }
        }else if( type.equals( "p2p" ) ){
           while( links.hasNext() ){
              Link link = (Link)links.next() ;
              int tmpPref = link._p2pPref < 0 ? link._readPref : link._p2pPref ;
              if( tmpPref < 1 )continue ;
              if( tmpPref != pref ){
                 listList.add( current = new ArrayList() ) ;
                 pref = tmpPref ;
              }
              current.add( link ) ;
           }
        }else{
            while( links.hasNext() ){
                Link link = (Link)links.next() ;
                if( link._writePref < 1 )continue ;
                if( link._writePref != pref ){
                    listList.add( current = new ArrayList() ) ;
                    pref = link._writePref ;
                }
                current.add( link ) ;
            }
        }
        //System.out.println("PSUDEBUG : result list : "+listList);
        Object [] x = listList.toArray() ;
        PoolPreferenceLevel [] result = new PoolPreferenceLevel[x.length] ;
        //
        // resolve the links to the pools
        //
        for( int i = 0 ; i < x.length ; i++ ){
        
            List      linkList   = (List)x[i] ;
            ArrayList resultList = new ArrayList() ;
            String    tag        = null ;
            
            for( Iterator nlinks = linkList.iterator() ; nlinks.hasNext() ; ){
            
                Link     link = (Link)nlinks.next() ;
                //
                // get the link if available
                //
                if( ( tag == null ) && ( link._tag != null ) )tag = link._tag ;
                
                Iterator poolCores = link._poolList.values().iterator() ;
                
                while( poolCores.hasNext() ){
                    PoolCore poolCore = (PoolCore)poolCores.next() ;
                    if( poolCore instanceof Pool ){
                        Pool pool =(Pool)poolCore ;
                        if( ( pool.isEnabled() ) &&
                        ( ( type.equals("read") ) || ! pool.isReadOnly() ) &&
                        ( _allPoolsActive || ( pool.getActive() < 5*60*1000 ) ) ){
                            
                            resultList.add( pool.getName() ) ;
                        }
                    }else{
                        Iterator pools = ((PGroup)poolCore)._poolList.values().iterator() ;
                        while( pools.hasNext() ){
                            Pool pool = (Pool)pools.next() ;
                            if( ( pool.isEnabled() ) &&
                            ( ( type.equals("read") ) || ! pool.isReadOnly() ) &&
                            ( _allPoolsActive || ( pool.getActive() < 5*60*1000 ) ) ){
                                
                                resultList.add( pool.getName() ) ;
                            }
                        }
                    }
                }
            }
            result[i] = new PoolPreferenceLevel( resultList , tag ) ;
        }
        return result ;
    }
    public String getProtocolUnit( String protocolUnitName ){
       Unit unit = findProtocolUnit( protocolUnitName ) ;
       return unit == null ? null : unit.getName() ;
    }
    //
    //   Legal formats :  <protocol>/<version>
    //
    private boolean _protocolsChecked = false ;
    private void protocolConfig (){
       if( _protocolsChecked )return ;
       _protocolsChecked = true ;
       boolean found = false ;
       for( Iterator i = _units.values().iterator() ; i.hasNext() ; ){
          if(  i.next() instanceof ProtocolUnit ){ found = true ; break ;}
       }
       if( ! found )_units.put( "*/*" , new ProtocolUnit("*/*") ) ;
       return ;
    }
    public synchronized Unit findProtocolUnit( String protocolUnitName ){
       //
       if( ( protocolUnitName == null ) || ( protocolUnitName.length() == 0 ) )return null ;
       //
       
       protocolConfig() ;
       
       int position = protocolUnitName.indexOf('/') ;
       //
       // 
       if( ( position < 0  ) || 
           ( position == 0 ) || 
           ( position == ( protocolUnitName.length() -1 ) ) ){

         throw new
            IllegalArgumentException("Not a valid protocol specification : "+protocolUnitName);
       }
       //
       //   we try :
       //          <protocol>/<majorVersion>
       //          <protocol>/*
       //                   */*
       //
       Unit unit = (Unit)_units.get( protocolUnitName ) ;
       if( unit != null )return unit ;
       //
       //
       if( unit == null ){
           //
           //
           unit = (Unit)_units.get( protocolUnitName.substring(0,position)+"/*" ) ;
           //
           if( unit == null ){
               //
               unit = (Unit)_units.get( "*/*" ) ;
               //
           }
       }
       //
       return unit ;

    }
    public synchronized String getNetIdentifier( String address ){
        try{
            NetUnit unit = _netHandler.match(address) ;
            return unit.getCanonicalName() ;
        }catch(Exception e ){
            return "NoSuchHost" ;
        }
    }
    public synchronized Map match( Unit unit ) {
        
        Map      map    = new HashMap() ;
        Iterator groups = unit._uGroupList.values().iterator() ;
        
        while( groups.hasNext() ){
            UGroup   uGroup = (UGroup)groups.next() ;
            Iterator links  = uGroup._linkList.values().iterator() ;
            while( links.hasNext() ){
                Link link = (Link)links.next() ;
                map.put( link.getName() , link ) ;
            }
            
        }
        return map ;
        
    }
    public String hh_psu_set_allpoolsactive = "on|off" ;
    public String ac_psu_set_allpoolsactive_$_1( Args args ) throws CommandSyntaxException {
       String mode = args.argv(0) ;
       if( mode.equals( "on" ) || mode.equals("true") ){
           _allPoolsActive = true ;
       }else if( mode.equals("off") || mode.equals("false") ){
           _allPoolsActive = false ;
       }else{
            throw new
            CommandSyntaxException("Syntax error");
       }
 
       return "" ;
    }
    public String hh_psu_netmatch = "<hostAddress>" ;
    public String ac_psu_netmatch_$_1( Args args )throws UnknownHostException {
        NetUnit unit = _netHandler.match( args.argv(0) )  ;
        if( unit == null )
            throw new
            IllegalArgumentException("Host not a unit : "+args.argv(0));
        return unit.toString() ;
    }
    public String hh_psu_match =
    "read|cache|write|p2p <storeUnit>|* <dCacheUnit>|* <netUnit>|* <protocolUnit>|* " ;
    public String ac_psu_match_$_5( Args args ) throws Exception {
        try{
            long start = System.currentTimeMillis() ;
            PoolPreferenceLevel [] list = 
                     match( args.argv(0).equals("*") ? null : args.argv(0) ,
                            args.argv(1).equals("*") ? null : args.argv(1) ,
                            args.argv(2).equals("*") ? null : args.argv(2) ,
                            args.argv(3).equals("*") ? null : args.argv(3) ,
                            args.argv(4).equals("*") ? null : args.argv(4) ,
                            null 
                          ) ;
                          
           start = System.currentTimeMillis() - start ;
            
            StringBuffer sb = new StringBuffer() ;
            for( int i = 0 ; i < list.length ; i++ ){
                String tag = list[i].getTag() ;
                sb.append( "Preference : ").append(i).append("\n") ;
                sb.append( "       Tag : ").append( tag == null ? "NONE" : tag ).append("\n") ;                
                for( Iterator links = list[i].getPoolList().iterator() ; links.hasNext() ; ){
                    sb.append( "  " ).append( links.next().toString() ).append("\n");
                }
            }
            sb.append( "(time used : ").append( start ).append( " millis)\n" ) ;
            return sb.toString() ;
        }catch(Exception ee){
            ee.printStackTrace() ;
            throw ee;
        }
    }
    public String hh_psu_match2 = "<unit> [...] [-net=<netUnit>}" ;
    public String ac_psu_match2_$_1_99( Args args ) throws Exception {
        StringBuffer sb  = new StringBuffer() ;
        Map          map = null ;
        int required     = args.argc() ;
        synchronized( this ){
            for( int i = 0  ; i < args.argc() ; i++ ){
                String unitName = args.argv(i) ;
                Unit   unit     = (Unit)_units.get( unitName ) ;
                if( unit == null )
                    throw new
                    IllegalArgumentException( "Unit not found : "+unitName ) ;
                
                map = match( map , unit ) ;
            }
            String netUnitName = args.getOpt("net") ;
            if( netUnitName != null ){
                Unit unit = _netHandler.find( new NetUnit( netUnitName ) ) ;
                if( unit == null )
                    throw new
                    IllegalArgumentException( "Unit not found in netList : "+netUnitName ) ;
                
                map = match( map , unit ) ;
            }
            Iterator links = map.values().iterator() ;
            while( links.hasNext() ){
                Link link = (Link)links.next() ;
                if( link._uGroupList.size() != required )continue ;
                sb.append( "Link : " ).append(link.toString()).append("\n") ;
                Iterator pools = link.pools() ;
                while( pools.hasNext() ){
                    sb.append( "    " ).
                    append(  ((Pool)pools.next()).getName()).
                    append("\n" ) ;
                }
            }
            
        }
        return sb.toString() ;
    }
    ///////////////////////////////////////////////////////////////////////////////
    //
    // the CLI
    //
    //..............................................................
    //
    //  the create's
    //
    public String hh_psu_create_pgroup = "<pGroup>" ;
    public String ac_psu_create_pgroup_$_1( Args args ){
        String name = args.argv(0) ;
        synchronized( this ){
            if( _pGroups.get( name ) != null )
                throw new
                IllegalArgumentException("Duplicated entry : "+name ) ;
            
            PGroup group = new PGroup( name ) ;
            
            _pGroups.put( group.getName() , group ) ;
        }
        return "" ;
    }
    public String hh_psu_set_regex = "on | off";
    public String ac_psu_set_regex_$_1(Args args) {
        String retVal;
	String onOff = args.argv(0);
        if(onOff.equals("on")) {
            _useRegex = true;
            retVal = "regex turned on";
        }
        else if( onOff.equals("off") ){
            _useRegex = false;
            retVal = "regex turned off";
        }
        else {
            throw new IllegalArgumentException( "please set regex either on or off");
        }
        return retVal;
    }
    
    public String hh_psu_create_pool = "<pool> [-noping]" ;
    public String ac_psu_create_pool_$_1( Args args ){
        String name = args.argv(0) ;
        synchronized( this ){
            if( _pools.get( name ) != null )
                throw new
                IllegalArgumentException("Duplicated entry : "+name ) ;
            
            Pool pool = new Pool( name ) ;
            if( args.getOpt("noping") != null )pool.setPing(false);
            if( args.getOpt("disabled") != null )pool.setEnabled(false);
            _pools.put( pool.getName() , pool ) ;
        }
        return "" ;
    }
    public String hh_psu_set_pool = "<poolName> enabled|disabled|ping|noping|rdonly|notrdonly" ;
    public String ac_psu_set_pool_$_2( Args args ) throws Exception {
        String poolName = args.argv(0) ;
        String mode     = args.argv(1) ;
        
        synchronized( this ){
            Pool pool = (Pool)_pools.get( poolName ) ;
            if( pool == null )
                throw new
                IllegalArgumentException("Not found : "+poolName) ;
            
            if( mode.equals( "enabled" ) ){
                pool.setEnabled( true ) ;
            }else if( mode.equals( "disabled" ) ){
                pool.setEnabled( false ) ;
            }else if( mode.equals( "ping" ) ){
                pool.setPing(true) ;
            }else if( mode.equals( "noping" ) ){
                pool.setPing(false) ;
            }else if( mode.equals( "rdonly" ) ){
               pool.setReadOnly(true) ;
            }else if( mode.equals( "notrdonly" ) ){
               pool.setReadOnly(false) ;
            }else{
                throw new
                IllegalArgumentException("mode not supported : "+mode ) ;
            }
            
        }
        return "" ;
    }
    public String hh_psu_set_enabled = "<poolName>" ;
    public String hh_psu_set_disabled = "<poolName>" ;
    public String ac_psu_set_enabled_$_1( Args args ) {
        setEnabled( args.argv(0) , true ) ;
        return "" ;
    }
    public String ac_psu_set_disabled_$_1( Args args ) {
        setEnabled( args.argv(0) , false ) ;
        return "" ;
    }
    public String hh_psu_create_link = "<link> <uGroup> [...]" ;
    public String ac_psu_create_link_$_2_99( Args args ){
        
        String name       = args.argv(0) ;
        
        synchronized( this ){
            
            if( _links.get( name ) != null )
                throw new
                IllegalArgumentException("Duplicated entry : "+name ) ;
            
            Link link = new Link( name ) ;
            //
            // we have to check if all the ugroups really exists.
            // only after we know, that all exist we can
            // add ourselfs to the uGroupLinkList
            //
            for( int i = 1 ; i < args.argc() ; i++ ){
                String uGroupName = args.argv(i) ;
                
                UGroup uGroup = (UGroup)_uGroups.get( uGroupName ) ;
                if( uGroup == null )
                    throw new
                    IllegalArgumentException( "uGroup not found : "+uGroupName ) ;
                
                link._uGroupList.put( uGroup.getName() , uGroup ) ;
                
            }
            Iterator groups = link._uGroupList.values().iterator() ;
            while( groups.hasNext() ){
                ((UGroup)groups.next())._linkList.put( link.getName() , link ) ;
            }
            _links.put( link.getName() , link ) ;
            
        }
        return "" ;
    }
    public String hh_psu_create_ugroup = "<uGroup>" ;
    public String ac_psu_create_ugroup_$_1( Args args ){
        String name = args.argv(0) ;
        synchronized( this ){
            if( _uGroups.get( name ) != null )
                throw new
                IllegalArgumentException("Duplicated entry : "+name ) ;
            
            UGroup group = new UGroup( name ) ;
            
            _uGroups.put( group.getName() , group ) ;
        }
        return "" ;
    }
    public String hh_psu_create_unit = "<unit> -net|-store|-dcache" ;
    public String ac_psu_create_unit_$_1( Args args ) throws UnknownHostException{
        String name = args.argv(0) ;
        Unit   unit = null ;
            synchronized( this ){
                if( args.getOpt( "net" ) != null ){
                    NetUnit net = new NetUnit( name ) ;
                    _netHandler.add( net ) ;
                    unit = net ;
                }else if( args.getOpt( "store" ) != null ){
                    unit = new Unit( name , STORE  ) ;
                }else if( args.getOpt( "dcache" ) != null ){
                    unit = new Unit( name , DCACHE  ) ;
                }else if( args.getOpt( "protocol" ) != null ){
                   unit = new ProtocolUnit( name ) ;
                }
                if( unit == null )
                    throw new
                    IllegalArgumentException("Unit type missing net/store/dcache/protocol") ;

                String canonicalName = name ; // will use the input name
                if( _units.get( canonicalName ) != null )
                    throw new
                    IllegalArgumentException("Duplicated entry : "+canonicalName ) ;
                
                _units.put( canonicalName , unit ) ;
            }
        return "" ;
        
    }
    //
    //..................................................................
    //
    //  the 'psux ... ls'
    //
    public String hh_psux_ls_pool = "[<pool>]" ;
    public Object ac_psux_ls_pool_$_0_1( Args args )throws Exception {
        if( args.argc() == 0 ){
            synchronized( this ){
                return _pools.keySet().toArray();
            }
        }else{
            synchronized( this ){
                String poolName = args.argv(0) ;
                Pool pool = (Pool)_pools.get( poolName ) ;
                if( pool == null )
                    throw new
                    IllegalArgumentException("Not found : "+poolName) ;
                
                Object [] result = new Object[6] ;
                result[0] = poolName ;
                result[1] = pool._pGroupList.keySet().toArray() ;
                result[2] = pool._linkList.keySet().toArray() ;
                result[3] = new Boolean(pool._enabled) ;
                result[4] = new Long( pool.getActive() );
                result[5] = new Boolean(pool._rdOnly) ;
                return result ;
            }
        }
    }
    public String hh_psux_ls_pgroup = "[<pgroup>]" ;
    public Object ac_psux_ls_pgroup_$_0_1( Args args )throws Exception {
        if( args.argc() == 0 ){
            synchronized( this ){
                return _pGroups.keySet().toArray();
            }
        }else{
            synchronized( this ){
                String groupName = args.argv(0) ;
                PGroup group = (PGroup)_pGroups.get( groupName ) ;
                if( group == null )
                    throw new
                    IllegalArgumentException("Not found : "+groupName) ;
                
                Object [] result = new Object[3] ;
                result[0] = groupName ;
                result[1] = group._poolList.keySet().toArray() ;
                result[2] = group._linkList.keySet().toArray() ;
                return result ;
            }
        }
    }
    public String hh_psux_ls_unit = "[<unit>]" ;
    public Object ac_psux_ls_unit_$_0_1( Args args )throws Exception {
        if( args.argc() == 0 ){
            synchronized( this ){
                return _units.keySet().toArray();
            }
        }else{
            synchronized( this ){
                String unitName = args.argv(0) ;
                Unit   unit = (Unit)_units.get( unitName ) ;
                if( unit == null )
                    throw new
                    IllegalArgumentException("Not found : "+unitName) ;
                
                Object [] result = new Object[3] ;
                result[0] = unitName ;
                result[1] = unit._type == STORE    ? "Store"  :
                            unit._type == PROTOCOL ? "Protocol" :
                            unit._type == DCACHE   ? "dCache" :
                            unit._type == NET      ? "Net"    : "Unknown" ;
                result[2] = unit._uGroupList.keySet().toArray() ;
                return result ;
            }
        }
    }
    public String hh_psux_ls_ugroup = "[<ugroup>]" ;
    public Object ac_psux_ls_ugroup_$_0_1( Args args )throws Exception {
        if( args.argc() == 0 ){
            synchronized( this ){
                return _uGroups.keySet().toArray();
            }
        }else{
            synchronized( this ){
                String groupName = args.argv(0) ;
                UGroup group = (UGroup)_uGroups.get( groupName ) ;
                if( group == null )
                    throw new
                    IllegalArgumentException("Not found : "+groupName) ;
                
                Object [] result = new Object[3] ;
                result[0] = groupName ;
                result[1] = group._unitList.keySet().toArray() ;
                result[2] = group._linkList.keySet().toArray() ;
                return result ;
            }
        }
    }
    public String hh_psux_ls_link = "[<link>] [-x]" ;
    public Object ac_psux_ls_link_$_0_1( Args args )throws Exception {
        if( args.argc() == 0 ){
            synchronized( this ){
               if( args.getOpt("x") == null ) {
                   return _links.keySet().toArray();
               }else{
                   List array = new ArrayList() ;
                   for( Iterator i = _links.values().iterator() ; i.hasNext() ; ){
                     array.add( fillLinkProperties( (Link)i.next() )  ) ;
                   }
                   return array ;
               }
            }
        }else{
            synchronized( this ){
                 String linkName = args.argv(0) ;
                 Link   link     = (Link)_links.get( linkName ) ;
                 if( link == null )
                     throw new
                     IllegalArgumentException("Not found : "+linkName) ;

                 return fillLinkProperties( link ) ;
             }
        }
    }
    private Object [] fillLinkProperties( Link link ){
       Iterator i = link._poolList.values().iterator() ;
       ArrayList pools  = new ArrayList() ;
       ArrayList groups = new ArrayList() ;
       while( i.hasNext() ){
           PoolCore core = (PoolCore)i.next() ;
           if( core instanceof Pool )pools.add(core.getName());
           else groups.add(core.getName()) ;
       }
       Object [] result = new Object[9] ;
       result[0] = link._name ;
       result[1] = new Integer( link._readPref ) ;
       result[2] = new Integer( link._cachePref ) ;
       result[3] = new Integer( link._writePref ) ;
       result[4] = link._uGroupList.keySet().toArray() ;
       result[5] = pools.toArray() ;
       result[6] = groups.toArray() ;
       result[7] = new Integer( link._p2pPref ) ;
       result[8] = link._tag;
       return result ;
    }
    public String hh_psux_match = "read|cache|write <storeUnit> <dCacheUnit> <netUnit> <protocolUnit>" ;
    public Object ac_psux_match_$_5( Args args )throws Exception {
        synchronized( this ){
            PoolPreferenceLevel [] list =
                           match( args.argv(0) ,
                                  args.argv(1).equals("*") ? null : args.argv(1) ,
                                  args.argv(2).equals("*") ? null : args.argv(2) ,
                                  args.argv(3).equals("*") ? null : args.argv(3) ,
                                  args.argv(4).equals("*") ? null : args.argv(4) ,
                                  null )  ;
            return list ;
        }
    }
    
    //..................................................................
    //
    //  the 'ls'
    //
    public String hh_psu_ls_pool = "[-l] [-a] [<pool> [...]]" ;
    public String ac_psu_ls_pool_$_0_99( Args args ){
        StringBuffer sb = new StringBuffer() ;
        boolean more    = args.getOpt("a") != null ;
        boolean detail  = ( args.getOpt("l") != null ) || more ;
        
        synchronized( this ){
            Iterator i = null ;
            if( args.argc() == 0 ){
                i = _pools.values().iterator() ;
            }else{
                ArrayList l = new ArrayList() ;
                for( int n = 0 ; n < args.argc() ; n++ ){
                    Object o = _pools.get( args.argv(n) ) ;
                    if( o != null )l.add( o ) ;
                }
                i = l.iterator() ;
            }
            while( i.hasNext() ){
                Pool pool = (Pool)i.next() ;
                if( ! detail ){
                    sb.append( pool.getName() ).append("\n") ;
                }else{
                    sb.append( pool.toString() ).append("\n") ;
                    sb.append( " linkList   :\n" ) ;
                    Iterator i2 = pool._linkList.values().iterator() ;
                    while( i2.hasNext() ){
                        sb.append( "   " ).append( i2.next().toString() ).append("\n") ;
                    }
                    if( more ){
                        sb.append( " pGroupList : \n" ) ;
                        Iterator i1 = pool._pGroupList.values().iterator() ;
                        while( i1.hasNext() ){
                            sb.append( "   " ).append( i1.next().toString() ) .append("\n");
                        }
                    }
                }
            }
        }
        return sb.toString() ;
    }
    public String hh_psu_ls_pgroup = "[-l] [-a] [<pgroup> [...]]" ;
    public String ac_psu_ls_pgroup_$_0_99( Args args ){
        StringBuffer sb = new StringBuffer() ;
        boolean more    = args.getOpt("a") != null ;
        boolean detail  = ( args.getOpt("l") != null ) || more ;
        
        synchronized( this ){
            Iterator i = null ;
            if( args.argc() == 0 ){
                i = _pGroups.values().iterator() ;
            }else{
                ArrayList l = new ArrayList() ;
                for( int n = 0 ; n < args.argc() ; n++ ){
                    Object o = _pGroups.get( args.argv(n) ) ;
                    if( o != null )l.add( o ) ;
                }
                i = l.iterator() ;
            }
            while( i.hasNext() ){
                PGroup group = (PGroup)i.next() ;
                sb.append( group.getName() ).append("\n") ;
                if( detail ){
                    sb.append( " linkList :\n" ) ;
                    Iterator i2 = group._linkList.values().iterator() ;
                    while( i2.hasNext() ){
                        sb.append( "   " ).append( i2.next().toString() ).append("\n") ;
                    }
                    sb.append( " poolList :\n" ) ;
                    Iterator i1 = group._poolList.values().iterator() ;
                    while( i1.hasNext() ){
                        sb.append( "   " ).append( i1.next().toString() ).append("\n") ;
                    }
                }
            }
        }
        return sb.toString() ;
    }
    public String hh_psu_ls_link = "[-l] [-a] [ <link> [...]]" ;
    public String ac_psu_ls_link_$_0_99( Args args ){
        
        StringBuffer sb = new StringBuffer() ;
        boolean more    = args.getOpt("a") != null ;
        boolean detail  = ( args.getOpt("l") != null ) || more ;
        
        synchronized( this ){
            Iterator i = null ;
            if( args.argc() == 0 ){
                i = _links.values().iterator() ;
            }else{
                ArrayList l = new ArrayList() ;
                for( int n = 0 ; n < args.argc() ; n++ ){
                    Object o = _links.get( args.argv(n) ) ;
                    if( o != null )l.add( o ) ;
                }
                i = l.iterator() ;
            }
            while( i.hasNext() ){
                Link link = (Link)i.next() ;
                sb.append( link.getName() ).append("\n") ;
                if( detail ){
                    sb.append( " readPref  : " ).append( link._readPref ).append( "\n" ) ;
                    sb.append( " cachePref : " ).append( link._cachePref ).append( "\n" ) ;
                    sb.append( " writePref : " ).append( link._writePref).append( "\n" ) ;
                    sb.append( " p2pPref   : " ).append( link._p2pPref).append( "\n" ) ;
                    sb.append( " section   : " ).append( link._tag == null ? "None" : link._tag ).append( "\n" ) ;
                    sb.append( " UGroups :\n" ) ;
                    Iterator groups = link._uGroupList.values().iterator() ;
                    while( groups.hasNext() ){
                        UGroup group = (UGroup)groups.next() ;
                        sb.append("   ").append(group.toString()).append("\n") ;
                    }
                    if( more ){
                        sb.append( " poolList  :\n" ) ;
                        Iterator i1 = link._poolList.values().iterator() ;
                        while( i1.hasNext() ){
                            sb.append( "   " ).append( i1.next().toString() ).append("\n") ;
                        }
                    }
                }
            }
        }
        return sb.toString() ;
    }
    public String hh_psu_ls_ugroup = "[-l] [-a] [<uGroup> [...]]" ;
    public String ac_psu_ls_ugroup_$_0_99( Args args ){
        
        StringBuffer sb = new StringBuffer() ;
        boolean more    = args.getOpt("a") != null ;
        boolean detail  = ( args.getOpt("l") != null ) || more ;
        
        synchronized( this ){
            Iterator i = null ;
            if( args.argc() == 0 ){
                i = _uGroups.values().iterator() ;
            }else{
                ArrayList l = new ArrayList() ;
                for( int n = 0 ; n < args.argc() ; n++ ){
                    Object o = _uGroups.get( args.argv(n) ) ;
                    if( o != null )l.add( o ) ;
                }
                i = l.iterator() ;
            }
            while( i.hasNext() ){
                UGroup group = (UGroup)i.next() ;
                sb.append( group.getName() ).append("\n") ;
                if( detail ){
                    sb.append( " unitList :\n" ) ;
                    Iterator i2 = group._unitList.values().iterator() ;
                    while( i2.hasNext() ){
                        sb.append( "   " ).append( i2.next().toString() ).append("\n") ;
                    }
                    if( more ){
                        sb.append( " linkList :\n" ) ;
                        Iterator i1 = group._linkList.values().iterator() ;
                        while( i1.hasNext() ){
                            sb.append( "   " ).append( i1.next().toString() ) .append("\n");
                        }
                    }
                }
            }
        }
        return sb.toString() ;
    }
    public String hh_psu_ls_netunits = "" ;
    public String ac_psu_ls_netunits( Args args ){
        StringBuffer sb = new StringBuffer() ;
        synchronized( this ){
            for( int i = 0 ; i < _netHandler._netList.length ; i++ ){
                HashMap map = _netHandler._netList[i] ;
                if( map == null )continue ;
                String stringMask = _netHandler.bitsToString(i) ;
                sb.append( stringMask ).
                append("/").
                append(i).
                append("\n");
                Iterator list = map.values().iterator() ;
                while( list.hasNext() ){
                    NetUnit net = (NetUnit)list.next() ;
                    sb.append( "   " ).
                    append( net.getHostAddress().getHostName() ) ;
                    if( i > 0 )sb.append("/").append(stringMask) ;
                    sb.append("\n");
                }
                
                
            }
        }
        return sb.toString() ;
    }
    public String hh_psu_ls_unit = " [-a] [<unit> [...]]" ;
    public String ac_psu_ls_unit_$_0_99( Args args ){
        StringBuffer sb = new StringBuffer() ;
        boolean more    = args.getOpt("a") != null ;
        boolean detail  = ( args.getOpt("l") != null ) || more ;
        
        synchronized( this ){
            Iterator i = null ;
            if( args.argc() == 0 ){
                i = _units.values().iterator() ;
            }else{
                ArrayList l = new ArrayList() ;
                for( int n = 0 ; n < args.argc() ; n++ ){
                    Object o = _units.get( args.argv(n) ) ;
                    if( o != null )l.add( o ) ;
                }
                i = l.iterator() ;
            }
            while( i.hasNext() ){
                Unit unit = (Unit)i.next() ;
                if( detail ){
                    sb.append( unit.toString() ).append("\n");
                    if( more ){
                        sb.append( " uGroupList :\n" ) ;
                        Iterator i2 = unit._uGroupList.values().iterator() ;
                        while( i2.hasNext() ){
                            sb.append( "   " ).append( i2.next().toString() ).append("\n") ;
                        }
                    }
                }else{
                    sb.append( unit.getName() ).append("\n") ;
                }
            }
        }
        return sb.toString() ;
    }
    public String hh_psu_dump_setup = "" ;
    public String ac_psu_dump_setup( Args args ){
        StringBuffer sb = new StringBuffer(4*1024) ;
        dumpSetup( sb ) ;
        return sb.toString() ;
    }
    //
    //.............................................................................
    //
    // the 'removes'
    //
    public String hh_psu_remove_unit = "<unit> [-net]" ;
    public String ac_psu_remove_unit_$_1( Args args ) throws UnknownHostException{
        String unitName = args.argv(0) ;
        
        synchronized( this ){
            if( args.getOpt("net") != null ){
                NetUnit netUnit = _netHandler.find( new NetUnit(unitName) ) ;
                if( netUnit == null )
                    throw new
                    IllegalArgumentException( "Not found in netList : "+unitName ) ;
                unitName = netUnit.getName() ;
            }
            Unit unit = (Unit)_units.get( unitName ) ;
            if( unit == null )
                throw new
                IllegalArgumentException( "Unit not found : "+unitName ) ;
            
            if( unit instanceof NetUnit )_netHandler.remove( (NetUnit)unit ) ;
            
            Iterator groups = unit._uGroupList.values().iterator() ;
            while( groups.hasNext() ){
                UGroup group = (UGroup)groups.next() ;
                group._unitList.remove( unit.getCanonicalName() ) ;
            }
            
            _units.remove( unitName ) ;
        }
        return "" ;
    }
    public String hh_psu_remove_ugroup = "<uGroup>" ;
    public String ac_psu_remove_ugroup_$_1( Args args ){
        String groupName = args.argv(0) ;
        synchronized( this ){
            UGroup group = (UGroup)_uGroups.get( groupName ) ;
            if( group == null )
                throw new
                IllegalArgumentException( "UGroup not found : "+groupName ) ;
            
            if( group._unitList.size() > 0 )
                throw new
                IllegalArgumentException( "UGroup not empty : "+groupName ) ;
            
            if( group._linkList.size() > 0 )
                throw new
                IllegalArgumentException(
                "Still link(s) pointing to us : "+groupName ) ;
            
            _uGroups.remove( groupName ) ;
            
        }
        return "" ;
    }
    public String hh_psu_remove_pgroup = "<pGroup>" ;
    public String ac_psu_remove_pgroup_$_1( Args args ){
        String name  = args.argv(0) ;
        synchronized( this ){
            PGroup group = (PGroup)_pGroups.get( name ) ;
            if( group == null )
                throw new
                IllegalArgumentException("PGroup not found : "+name ) ;
            
            //
            // check if empty
            //
            if( group._poolList.size() != 0 )
                throw new
                IllegalArgumentException("PGroup not empty : "+name ) ;
            //
            // remove the links
            //
            PoolCore core = (PoolCore)group ;
            Iterator i = core._linkList.values().iterator() ;
            while( i.hasNext() ){
                Link link = (Link)i.next() ;
                link._poolList.remove( core.getName() ) ;
            }
            //
            // remove from global
            //
            _pGroups.remove( name ) ;
        }
        return "" ;
    }
    public String hh_psu_remove_pool = "<pool>" ;
    public String ac_psu_remove_pool_$_1( Args args ){
        String name  = args.argv(0) ;
        synchronized( this ){
            Pool pool = (Pool)_pools.get( name ) ;
            if( pool == null )
                throw new
                IllegalArgumentException("Pool not found : "+name ) ;
            //
            // remove from groups
            //
            Iterator i = pool._pGroupList.values().iterator() ;
            while( i.hasNext() ){
                PGroup group = (PGroup)i.next() ;
                group._poolList.remove( pool.getName() ) ;
            }
            //
            // remove the links
            //
            PoolCore core = (PoolCore)pool ;
            i = core._linkList.keySet().iterator() ;
            while( i.hasNext() ){
                Link link = (Link)i.next() ;
                link._poolList.remove( core.getName() ) ;
            }
            //
            // remove from global
            //
            _pools.remove( name ) ;
        }
        return "" ;
    }
    public String hh_psu_removefrom_ugroup = "<uGroup> <unit> -net" ;
    public String ac_psu_removefrom_ugroup_$_2( Args args ) throws UnknownHostException{
        String groupName = args.argv(0) ;
        String unitName  = args.argv(1) ;
        synchronized( this ){
            UGroup group = (UGroup)_uGroups.get( groupName ) ;
            if( group == null )
                throw new IllegalArgumentException( "UGroup not found : "+groupName ) ;
            
            if( args.getOpt("net") != null ){
                NetUnit netUnit = _netHandler.find( new NetUnit(unitName) ) ;
                if( netUnit == null )
                    throw new
                    IllegalArgumentException( "Not found in netList : "+unitName ) ;
                unitName = netUnit.getName() ;
            }
            Unit unit = (Unit)_units.get( unitName ) ;
            if( unit == null )
                throw new IllegalArgumentException( "Unit not found : "+unitName ) ;
            String canonicalName = unit.getCanonicalName() ;
            if( group._unitList.get( canonicalName ) == null )
                throw new IllegalArgumentException( unitName +" not member of "+groupName ) ;
            
            group._unitList.remove( canonicalName ) ;
            unit._uGroupList.remove( groupName ) ;
        }
        return "" ;
    }
    public String hh_psu_removefrom_pgroup = "<pGroup> <pool>" ;
    public String ac_psu_removefrom_pgroup_$_2( Args args ){
        String groupName = args.argv(0) ;
        String poolName  = args.argv(1) ;
        synchronized( this ){
            Pool pool = (Pool)_pools.get( poolName ) ;
            if( pool == null )
                throw new
                IllegalArgumentException("Pool not found : "+poolName ) ;
            
            PGroup group = (PGroup)_pGroups.get( groupName ) ;
            if( group == null )
                throw new
                IllegalArgumentException("PGroup not found : "+groupName ) ;
            
            if( group._poolList.get( poolName ) == null )
                throw new
                IllegalArgumentException(poolName+" not member of "+groupName ) ;
            
            group._poolList.remove( poolName ) ;
            pool._pGroupList.remove( groupName ) ;
        }
        return "" ;
    }
    public String hh_psu_remove_link = "<link>" ;
    public String ac_psu_remove_link_$_1( Args args ){
        String name  = args.argv(0) ;
        synchronized( this ){
            Link link = (Link)_links.get( name ) ;
            if( link == null )
                throw new
                IllegalArgumentException("Link not found : "+name ) ;
            //
            // remove from pools
            //
            Iterator pools = link._poolList.values().iterator() ;
            while( pools.hasNext() ){
                PoolCore core = (PoolCore)pools.next() ;
                core._linkList.remove( name ) ;
            }
            //
            // remove from unit group
            //
            Iterator groups = link._uGroupList.values().iterator() ;
            while( groups.hasNext() ){
                UGroup group = (UGroup)groups.next() ;
                group._linkList.remove( name ) ;
            }
            //
            // remove from global
            //
            _links.remove( name ) ;
        }
        return "" ;
    }
    //
    //........................................................................
    //
    //  relations
    //
    public String hh_psu_addto_pgroup = "<pGroup> <pool>" ;
    public String ac_psu_addto_pgroup_$_2( Args args ){
       String pGroupName = args.argv(0) ;
       String poolName   = args.argv(1) ;

       addtoPoolGroup( pGroupName , poolName ) ;

       return "" ;
    }
    private void addtoPoolGroup( String pGroupName , String poolName )
           throws IllegalArgumentException {

       synchronized( this ){
          PGroup group = (PGroup)_pGroups.get( pGroupName ) ;
          if( group == null )
             throw new
             IllegalArgumentException( "Not found : "+pGroupName ) ;
          Pool pool = (Pool)_pools.get( poolName ) ;
          if( pool == null )
             throw new
             IllegalArgumentException( "Not found : "+poolName ) ;
          //
          // shall we disallow more than one parent group ?
          //
          //         if( pool._pGroupList.size() > 0 )
          //            throw new
          //            IllegalArgumentException( poolName +" already member" ) ;
 
          pool._pGroupList.put( group.getName() , group ) ;
          group._poolList.put( pool.getName() , pool ) ;
       }
       return ;
    }
    public String hh_psu_addto_ugroup = "<uGroup> <unit>" ;
    public String ac_psu_addto_ugroup_$_2( Args args ) throws UnknownHostException{
        
        String uGroupName = args.argv(0) ;
        String unitName   = args.argv(1) ;
        
        synchronized( this ){
            if( args.getOpt("net") != null ){
                NetUnit netUnit = _netHandler.find( new NetUnit(unitName) ) ;
                if( netUnit == null )
                    throw new
                    IllegalArgumentException( "Not found in netList : "+unitName ) ;
                unitName = netUnit.getName() ;
            }
            UGroup group = (UGroup)_uGroups.get( uGroupName ) ;
            if( group == null )
                throw new
                IllegalArgumentException( "Not found : "+uGroupName ) ;
            Unit unit = (Unit)_units.get( unitName ) ;
            if( unit == null )
                throw new
                IllegalArgumentException( "Not found : "+unitName ) ;
            
            String canonicalName = unit.getCanonicalName() ;
            if( group._unitList.get( canonicalName ) != null )
                throw new
                IllegalArgumentException( unitName +" already member of "+uGroupName ) ;
            
            unit._uGroupList.put( group.getName() , group ) ;
            group._unitList.put( canonicalName , unit ) ;
        }
        return "" ;
    }
    public String hh_psu_unlink = "<link> <pool>|<pGroup>" ;
    public String ac_psu_unlink_$_2( Args args ){
        String linkName = args.argv(0) ;
        String poolName = args.argv(1) ;
        
        synchronized( this ){
            Link link = (Link)_links.get( linkName ) ;
            if( link == null )
                throw new
                IllegalArgumentException( "Not found : "+linkName ) ;
            
            PoolCore core = (PoolCore)_pools.get( poolName ) ;
            if( core == null )core = (PoolCore)_pGroups.get(poolName) ;
            if( core == null )
                throw new
                IllegalArgumentException("Not found : "+poolName ) ;
            
            if( core._linkList.get( linkName ) == null )
                throw new
                IllegalArgumentException(poolName+" not member of "+linkName) ;
            
            core._linkList.remove( linkName ) ;
            link._poolList.remove( poolName ) ;
        }
        
        return "" ;
    }
    public String hh_psu_add_link = "<link> <pool>|<pGroup>" ;
    public String ac_psu_add_link_$_2( Args args ){
        String linkName = args.argv(0) ;
        String poolName = args.argv(1) ;
        
        synchronized( this ){
            Link link = (Link)_links.get( linkName ) ;
            if( link == null )
                throw new
                IllegalArgumentException( "Not found : "+linkName ) ;
            
            PoolCore core = (PoolCore)_pools.get( poolName ) ;
            if( core == null )core = (PoolCore)_pGroups.get(poolName) ;
            if( core == null )
                throw new
                IllegalArgumentException("Not found : "+poolName ) ;
            
            core._linkList.put( link.getName() , link ) ;
            link._poolList.put( core.getName() , core ) ;
        }
        
        return "" ;
    }
    public String hh_psu_set_active = "<poolName>|* [-no]" ;
    public String ac_psu_set_active_$_1( Args args ){
        String  poolName = args.argv(0) ;
        boolean active   = args.getOpt("no") == null ;
        if( poolName.equals( "*") ){
            synchronized( this ){
                Iterator pools = _pools.values().iterator() ;
                while( pools.hasNext() ){
                    Pool pool = (Pool)pools.next() ;
                    pool.setActive(active) ;
                }
            }
        }else{
            Pool pool = (Pool)_pools.get(poolName) ;
            if( pool == null )
                throw new
                IllegalArgumentException("Pool not found : "+poolName);
            pool.setActive( active ) ;
        }
        return "" ;
    }
    public String hh_psu_set_link =
    "<link> [-readpref=<readpref>] [-writepref=<writepref>] [-cachepref=<cachepref>] [-p2ppref=<p2ppref>] [-section=<section>|NONE]" ;
    public String ac_psu_set_link_$_1( Args args ){
        String linkName = args.argv(0) ;
        synchronized( this ){
            Link link = (Link)_links.get( linkName ) ;
            if( link == null )
                throw new
                IllegalArgumentException("Not found : "+linkName ) ;
            
            String tmp = args.getOpt( "readpref" ) ;
            if( tmp != null )link._readPref = Integer.parseInt( tmp ) ;
            tmp = args.getOpt( "cachepref" ) ;
            if( tmp != null )link._cachePref = Integer.parseInt( tmp ) ;
            tmp = args.getOpt( "writepref" ) ;
            if( tmp != null )link._writePref = Integer.parseInt( tmp ) ;
            tmp = args.getOpt( "p2ppref" ) ;
            if( tmp != null )link._p2pPref = Integer.parseInt( tmp ) ;
            tmp = args.getOpt( "section" ) ;
            if( tmp != null )link._tag = tmp.equals("NONE") ? null : tmp ;
            
            
        }
        return "" ;
    }
    public String hh_psu_clear_im_really_sure = "# don't use this command" ;
    public String ac_psu_clear_im_really_sure( Args args ){
        clear() ;
        return "Voila, now everthing is really gone" ;
    }
}
