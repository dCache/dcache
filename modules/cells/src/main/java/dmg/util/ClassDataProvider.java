package dmg.util ;

import java.io.IOException;

public interface ClassDataProvider {

   byte [] getClassData(String className)
      throws IOException ;

}
