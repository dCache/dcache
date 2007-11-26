package dmg.apps.psdl.vehicles ;

import dmg.apps.psdl.pnfs.* ;
import java.io.* ;

public class PsdlHsmRequest extends PsdlCoreRequest {

    private HsmProperties _hsmProps   = null ;
    private String        _hsmManager = "HsmMgr" ;
    
    public PsdlHsmRequest(  String type , PnfsId id , HsmProperties hsm ){
       super( type , id ) ;
       _hsmProps = hsm ;
    }
    public PsdlHsmRequest(  String type , PnfsId id , PnfsId parentId ){
       super( type , id , parentId ) ;
    }
    public HsmProperties getHsmProperties(){ return _hsmProps ; }
    public void setHsmProperties( HsmProperties props ){ _hsmProps = props ; }
    public void setHsmManager( String hsmMgr ){ _hsmManager = hsmMgr ; }
    public String getHsmManager(){ return _hsmManager ; }
    public String toString(){
       return super.toBaseString()+
              ";hsmKey="+_hsmProps.getHsmKey()+
              super.toReturnString() ;
    }
    public String toBaseString(){
       return super.toBaseString()+
              ";hsmKey="+(_hsmProps==null?"None":_hsmProps.getHsmKey()) ;
    }


}
