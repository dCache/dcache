package dmg.util.edb ;

import java.io.* ;
import java.util.* ;

public interface SldbEntry {
   public long getCookie() ;
   public SldbEntry getNextEntry() ;
}
