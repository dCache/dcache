/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.srm.taperecallscheduling.spi;

import java.util.List;
import java.util.Map;
import org.dcache.srm.taperecallscheduling.TapeInfo;
import org.dcache.srm.taperecallscheduling.TapefileInfo;

public interface TapeInfoProvider {

    /**
     * Returns information on tapes requested by name
     *
     * @param tapes list of tape names
     * @return associated tape infos
     */
    Map<String, TapeInfo> getTapeInfos(List<String> tapes);

    /**
     * Returns information on tape located files requested by name
     *
     * @param fileids list of files requested by identifyer (full srm path)
     * @return tapefile infos
     */
    Map<String, TapefileInfo> getTapefileInfos(List<String> fileids);

    /**
     * Returns a Strinmg describing the tape info provider, including potentially stored state and
     * configuration.
     *
     * @return Descriptive String
     */
    String describe();

    /**
     * Triggers a reload of the tape info provider's cached state if supported.
     *
     * @return
     */
    boolean reload();
}
