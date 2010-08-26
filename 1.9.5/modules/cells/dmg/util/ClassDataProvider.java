package dmg.util ;

import java.io.* ;

public interface ClassDataProvider {

   public byte [] getClassData( String className )
      throws IOException ;
 
}
