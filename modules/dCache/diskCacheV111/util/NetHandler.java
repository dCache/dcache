// $Id: NetHandler.java,v 1.2 2006-12-15 15:38:06 tigran Exp $
package diskCacheV111.util ;

import java.util.* ;
import java.net.* ;

public class NetHandler {
   private HashMap [] _netList     = new HashMap[33] ;
   private String  [] _maskStrings = new String[33] ;
   private long    [] _masks       = new long[33] ;

   static public class Unit {
       private int         _hostBits = 0 ;
       private InetAddress _address  = null ;
       public Unit( String address )throws UnknownHostException {
          int pos = address.indexOf( "/" ) ;
          if( pos < 0 ){
             setUnit( InetAddress.getByName(address) , 32 ) ;
             return ;
          }
          int netBits = Integer.parseInt( address.substring(pos+1) ) ;
          String netName = address.substring(0,pos) ;
          setUnit( InetAddress.getByName( netName )  , netBits ) ;
       }
       public Unit( String address , int netBits ) throws UnknownHostException {
          this( InetAddress.getByName( address ) , netBits ) ;
       }
       public Unit( InetAddress address , int netBits ){
          setUnit( address , netBits ) ;
       }
       public void setUnit( InetAddress address , int netBits ){
          _hostBits = 32 - netBits ;
          _address  = address ;
       }
       public int getHostBits() { return _hostBits ; }
       public InetAddress getHostAddress(){ return _address ; }
   }
   public NetHandler(){
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
   public void put(  Unit net , Object value ){
      int bit = net.getHostBits() ;
      if( _netList[bit] == null )_netList[bit] = new HashMap() ;

      long addr = inetAddressToLong( net.getHostAddress() ) ;
      _netList[bit].put( Long.valueOf( addr & _masks[bit] ) , value ) ;
   }
   public Object remove( Unit net ){

      int bit = net.getHostBits() ;
      if( _netList[bit] == null )return null ;

      long addr = inetAddressToLong( net.getHostAddress() ) ;

      Object value = _netList[bit].remove( Long.valueOf( addr ) ) ;
      if( _netList.length == 0 )_netList[bit] = null ;
      return value ;
   }
   public Object find( Unit net ){

      int bit = net.getHostBits() ;
      if( _netList[bit] == null )return null ;

      long addr = inetAddressToLong( net.getHostAddress() ) ;

      return _netList[bit].get( Long.valueOf( addr & _masks[bit]) ) ;
   }
   public Object match( String inetAddress )throws UnknownHostException {
      long    addr = inetAddressToLong( InetAddress.getByName( inetAddress ) ) ;
      Map     map  = null ;
      long    mask     = 0 ;
      long    cursor   = 1 ;
      Object  value    = null ;
      for( int i = 0 ; i < _netList.length ; i++ ){
          if( ( map = _netList[i] ) != null ){
             Long l = Long.valueOf( addr & ~ mask ) ;
//             System.out.println("Trying to find : "+Long.toHexString(l.longValue()) ) ;
             value = map.get( l ) ;
             if( value != null )return value ;
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
   public static void main( String [] args ) throws Exception {
      NetHandler nh = new NetHandler() ;
      nh.put( new NetHandler.Unit( "131.169.1.222" ) , "host" ) ;
      nh.put( new NetHandler.Unit( "131.169.1.222/24" ) , "subnet1" ) ;
      nh.put( new NetHandler.Unit( "131.169.2.222/24" ) , "subnet2" ) ;
      nh.put( new NetHandler.Unit( "131.169.1.222/0" ) ,  "world" ) ;
      String result = (String)nh.match( args[0] ) ;
      System.out.println(result) ;
   }
}
