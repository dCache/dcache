package org.dcache.srm.taperecallscheduling;

import java.util.List;
import java.util.Map;

public interface TapeInfoProvider {

    Map<String, TapeInfo> getTapeInfos(List<String> tapes);

    Map<String, TapefileInfo> getTapefileInfos(List<String> fileids);
}
