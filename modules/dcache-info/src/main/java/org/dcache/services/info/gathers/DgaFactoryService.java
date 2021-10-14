package org.dcache.services.info.gathers;

import dmg.cells.nucleus.UOID;
import java.util.Set;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateUpdateManager;

/**
 * A service that can create one or more Data-Gathering Activities will implement this interface and
 * register the class' name in the file
 * <p>
 * META-INF/services/org.dcache.services.info.gathers.DgaFactoryService
 * <p>
 * which is located within a jar file.
 */
public interface DgaFactoryService {

    Set<Schedulable> createDgas(StateExhibitor exhibitor, MessageSender sender,
          StateUpdateManager sum, MessageMetadataRepository<UOID> msgMetaRepo);
}
