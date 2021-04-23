/*
 * dCache - http://www.dcache.org/
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
package org.dcache.mock;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.util.CacheException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.SelectedPool;
import org.dcache.vehicles.FileAttributes;

/**
 * This class provides a minimal concrete implementation of the Partition.
 * All partition-specific behaviour is left unimplemented.
 */
public class SimplePartition extends Partition {
  private static final Map<String,String> DEFAULTS = Collections.emptyMap();
  private static final Map<String,String> INHERITED = Collections.emptyMap();

  public SimplePartition(Map<String,String> args)
  {
    super(DEFAULTS, INHERITED, args);
  }

  @Override
  protected Partition create(Map<String, String> inherited, Map<String, String> defined) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getType() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SelectedPool selectWritePool(CostModule cm, List<PoolInfo> pools,
      FileAttributes attributes, long preallocated) throws CacheException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SelectedPool selectReadPool(CostModule cm, List<PoolInfo> pools,
      FileAttributes attributes) throws CacheException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public P2pPair selectPool2Pool(CostModule cm, List<PoolInfo> src,
      List<PoolInfo> dst, FileAttributes attributes, boolean force)
      throws CacheException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SelectedPool selectStagePool(CostModule cm, List<PoolInfo> pools,
      Optional<PoolInfo> previous, FileAttributes attributes)
      throws CacheException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
