package dmg.apps.psdl.vehicles ;

import dmg.apps.psdl.pnfs.* ;
import java.io.* ;

public class PsdlIoRequest extends PsdlHsmRequest {

    private String        _clientHost = null ;
    private int           _clientPort = 0 ;
    private long          _size       = 0 ;
    private int           _selectedIndex = 0 ;
    private String  []    _selectedPools = null ;
    
    public PsdlIoRequest(   String type ,
                            String clientHost ,
                            int    clientPort ,
                            PnfsId id ,
                            HsmProperties hsm ,
                            long size            ){
                        
       super( type , id , hsm ) ;
       _clientHost = clientHost ;
       _clientPort = clientPort ;
       _size       = size ;
    }
    public PsdlIoRequest(   String type ,
                            String clientHost ,
                            int    clientPort ,
                            PnfsId id ,
                            PnfsId parentId ,
                            long size            ){
                        
       super( type , id , parentId ) ;
       _clientHost = clientHost ;
       _clientPort = clientPort ;
       _size       = size ;
    }
    public void setSelectedPools( String [] pools ){
        _selectedPools = pools ;
        _selectedIndex = 0 ;
    }
    public String nextSelectedPool(){
       if( ( _selectedPools == null ) ||
           ( _selectedIndex >= _selectedPools.length ) )return null ;
       return _selectedPools[_selectedIndex++] ;
    }
    public long   getSize(){ return _size ; }
    public String getHostname(){ return _clientHost ; } 
    public int    getPortNumber(){ return _clientPort ; }
    public String toString(){
       return super.toBaseString()+
              ";size="+_size+
              ";Host="+_clientHost+":"+_clientPort+
              super.toReturnString() ;
    }

}
