package dmg.cells.applets.spy ;

import java.util.* ;
import dmg.cells.network.* ;
import dmg.cells.nucleus.* ;


public class CanonTopo {

   private String   []  _domainNames ;
   private LinkPair []  _linkPairs ;

   /**
    *   The CanonTopo helper class created a canonical form
    *   of a topology. This makes it possible to compare
    *   different topoligies which are essentially identical.
    */
   public CanonTopo(){}
   public CanonTopo( CellDomainNode [] in ){
      _domainNames = new String[in.length] ;
      //
      // copy the domain names into _domainNames
      //
      for( int i = 0 ; i < in.length ; i++ ) {
          _domainNames[i] = in[i].getName();
      }
      //
      // get some kind of order ( canonical ) into names
      //
      _sort( _domainNames ) ;
      //
      // produce a 'name to index' hash
      //
      Hashtable nameHash = new Hashtable() ;

      for( int i= 0 ; i < in.length ; i++ ) {
          nameHash.put(_domainNames[i], Integer.valueOf(i));
      }
      //
      // produce the 'link hash'
      // the hashtable will essentially remove
      // the duplicated entries.
      //
      Hashtable linkHash = new Hashtable() ;
//      System.out.println( "Creating linkHashtable" ) ;

      for( int i = 0 ; i < in.length ; i++ ){
          String thisDomain = in[i].getName() ;
          int    thisPosition = ((Integer)nameHash.get( thisDomain )).intValue() ;
//          System.out.println( "  domain "+thisDomain+" at position "+thisPosition ) ;
          CellTunnelInfo [] links = in[i].getLinks() ;
          if( links == null ) {
              continue;
          }

          for( int j = 0 ; j < links.length ; j++ ){
             CellDomainInfo info = links[j].getRemoteCellDomainInfo() ;
             if( info == null ) {
                 continue;
             }
             String thatDomain =  info.getCellDomainName() ;
             int thatPosition  = ((Integer)nameHash.
                                 get( thatDomain )).
                                 intValue() ;
             LinkPair pair = new LinkPair( thisPosition , thatPosition ) ;
//             System.out.println( "     link "+thatDomain+"  : "+pair ) ;
             linkHash.put( pair , pair ) ;
          }
      }
      _linkPairs    = new LinkPair[linkHash.size()] ;
      Enumeration e = linkHash.elements() ;
      for( int i = 0  ; e.hasMoreElements() ; i++ ){
         _linkPairs[i] = (LinkPair)e.nextElement() ;
      }
      _sort( _linkPairs ) ;
      //

   }
   public int links(){ return _linkPairs.length ; }
   public int domains(){ return _domainNames.length ; }
   public String getDomain( int i ){ return _domainNames[i] ; }
   public LinkPair getLinkPair( int i ){ return _linkPairs[i] ; }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CanonTopo)) {
            return false;
        }
        CanonTopo topo = (CanonTopo) other;
        return
            Arrays.equals(_domainNames, topo._domainNames) &&
            Arrays.equals(_linkPairs, topo._linkPairs);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(_domainNames) ^ Arrays.hashCode(_linkPairs);
    }

   private void _sort( LinkPair [] x ){
      if( x.length < 2 ) {
          return;
      }
      LinkPair y;

      for( int j = 1 ; j < x.length ; j++ ){
         for( int i = 0 ; i < (x.length-1) ; i++ ){
            if( x[i].compareTo( x[i+1] ) > 0 ){
               y = x[i] ; x[i] = x[i+1] ; x[i+1] = y ;
            }
         }
      }
   }
   private void _sort( String [] x ){
      if( x.length < 2 ) {
          return;
      }
      String y;
      for( int j = 1 ; j < x.length ; j++ ){
         for( int i = 0 ; i < (x.length-1) ; i++ ){
            if( x[i].compareTo( x[i+1] ) > 0 ){
               y = x[i] ; x[i] = x[i+1] ; x[i+1] = y ;
            }
         }
      }

   }
   public static void main( String [] args ){
       LinkPair [] linkPairs = new LinkPair[4] ;
       CanonTopo x = new CanonTopo();
       int pos = 0 ;
       linkPairs[pos++] = new LinkPair( 1 , 10 ) ;
       linkPairs[pos++] = new LinkPair( 4 , 5 ) ;
       linkPairs[pos++] = new LinkPair( 7 , 10 ) ;
       linkPairs[pos++] = new LinkPair( 1 , 10 ) ;

       x._sort( linkPairs ) ;
       Hashtable hash = new Hashtable() ;
       for( int i = 0 ; i < linkPairs.length ; i++ ){
         System.out.println( " "+i+" "+linkPairs[i] ) ;
         hash.put( linkPairs[i] , linkPairs[i] ) ;
       }
       System.out.println( " hash entries : "+hash.size() ) ;
   }
   public static void main2( String [] args ){
      CanonTopo x = new CanonTopo() ;
      x._sort( args ) ;
      for( int i = 0 ;i < args.length ; i++ ) {
          System.out.println(" --> " + args[i]);
      }

   }
}
