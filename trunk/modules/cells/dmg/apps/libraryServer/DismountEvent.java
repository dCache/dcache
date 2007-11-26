package dmg.apps.libraryServer ;
import java.awt.* ;
import java.awt.event.* ;
import java.util.*;

public class DismountEvent extends HsmEvent {
    private String _drive     = null ;
    private String _cartridge = null ;
    public DismountEvent( Object source , String drive , String cartridge ){
       super(source,300,drive+":"+cartridge) ;
       _drive = drive ;
       _cartridge = cartridge ;
    }
    public String getCartridge(){ return _cartridge ; }
    public String getDrive(){ return _drive ; }
}
