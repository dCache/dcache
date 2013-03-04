package dmg.util ;

import java.io.IOException;

public interface ClassDataProvider {

   public byte [] getClassData( String className )
      throws IOException ;

}
