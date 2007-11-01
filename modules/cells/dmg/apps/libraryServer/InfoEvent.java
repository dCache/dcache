package dmg.apps.libraryServer ;
import java.awt.* ;
import java.awt.event.* ;
import java.util.*;

public class InfoEvent extends HsmEvent {
    private String _info     = null ;
    public InfoEvent( Object source , String info ){
       super(source,300,info) ;
       _info = info ;
    }
    public String getInfo(){ return _info ; }
}
 
