// $Id: PoolV2Mode.java,v 1.2 2004-02-25 15:15:54 patrick Exp $


package diskCacheV111.pools;


public class PoolV2Mode implements java.io.Serializable {

    public static final int ENABLED             = 0x00 ;
    public static final int DISABLED            = 0x01 ;
    public static final int DISABLED_FETCH      = 0x02 ;
    public static final int DISABLED_STORE      = 0x04 ;
    public static final int DISABLED_STAGE      = 0x08 ;
    public static final int DISABLED_P2P_CLIENT = 0x10 ;
    public static final int DISABLED_P2P_SERVER = 0x20 ;
    public static final int DISABLED_DEAD       = 0x40 ;
    public static final int DISABLED_STRICT     = 
                                     DISABLED |
                                     DISABLED_FETCH |
                                     DISABLED_STORE |
                                     DISABLED_STAGE |
                                     DISABLED_P2P_CLIENT |
                                     DISABLED_P2P_SERVER ;
    public static final int DISABLED_RDONLY     = 
                                     DISABLED |
                                     DISABLED_STORE |
                                     DISABLED_STAGE |
                                     DISABLED_P2P_CLIENT ;
                                     
    private static final String [] __modeString = {    
       "fetch" , "store" , "stage" , "p2p-client" , "p2p-server"  , "dead"
    } ;
    private int _mode = ENABLED ;
    
    public String toString(){
    
       if( _mode == ENABLED )return "enabled" ;
       
       StringBuffer sb = new StringBuffer() ;
       
       sb.append("disabled(") ; 

       for( int i = 0 , mode = _mode ; i < __modeString.length ; i++ ){
          mode >>= 1 ;
          if(( mode & 1 ) != 0 )sb.append(__modeString[i]).append(",");
       }
       sb.append(")") ;      
       return sb.toString() ;
    }
    public PoolV2Mode(){}
    
    public PoolV2Mode( int mode ){ 
       _mode = mode ; }
    public synchronized void setMode( int mode ){
       _mode = mode == 0 ? 0 : ( mode | DISABLED ) ;
    }
    public synchronized int getMode(){ return _mode ; }
    public synchronized boolean isDisabled( int mask ){
       return ( _mode & mask ) == mask ;
    }
    public synchronized boolean isDisabled(){ return _mode != 0 ; }
    public synchronized boolean isEnabled(){ return _mode == 0 ; }
    
    public static void main( String [] args )throws Exception {
        PoolV2Mode mode = new PoolV2Mode(DISABLED_STRICT) ;
        System.out.println(mode.toString()); 
        mode = new PoolV2Mode(DISABLED_RDONLY) ;
        System.out.println(mode.toString()); 
        mode = new PoolV2Mode() ;
        System.out.println(mode.toString()); 
        mode = new PoolV2Mode(DISABLED) ;
        System.out.println(mode.toString()); 
        System.out.println(mode.toString()); 
    }
}
