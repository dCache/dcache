package org.dcache.pool.statistics;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryChangeEvent;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StickyChangeEvent;
import org.dcache.vehicles.FileAttributes;
import org.springframework.beans.factory.annotation.Required;

public class StatisticsListener implements StateChangeListener {
  private final Map<String, StorageUnitSpaceStatistics> statistics = new HashMap<>();
  private Repository repository;

  public void initialize() {
    repository.addListener(this);
  }

  @Override
  public void stateChanged(StateChangeEvent event) {
    updateStatistics(event, event.getOldState(), event.getNewState());
  }

  @Override
  public void accessTimeChanged(EntryChangeEvent event) {
  }

  @Override
  public void stickyChanged(StickyChangeEvent event) {
    updateStatistics(event, event.getOldEntry().getState(), event.getNewEntry().getState());
  }

  @Required
  public void setRepository(Repository repository) {
    this.repository = repository;
  }

  public synchronized Map<String, long[]> toMap() {
    Map<String, long[]> map = new HashMap<>();
    for (Map.Entry<String, StorageUnitSpaceStatistics> entry : statistics.entrySet()) {
      map.put(entry.getKey(), entry.getValue().toArray());
    }
    return map;
  }

  public synchronized Map<String, StorageUnitSpaceStatistics> toJson() {
    return ImmutableMap.copyOf(statistics);
  }

  public synchronized long getOtherBytes() {
    long sum = 0;
    for (StorageUnitSpaceStatistics stats : statistics.values()) {
      sum += stats.otherBytes.get();
    }
    return sum;
  }

  private boolean isPrecious(CacheEntry entry) {
    return entry.getState() == ReplicaState.PRECIOUS;
  }

  private boolean isSticky(CacheEntry entry) {
    return entry.isSticky();
  }

  private boolean isOther(CacheEntry entry) {
    return !isPrecious(entry) && !isSticky(entry);
  }

  private StorageUnitSpaceStatistics getStatistics(FileAttributes fileAttributes) {
    String store = fileAttributes.getStorageClass() + "@" + fileAttributes.getHsm();
    return statistics.computeIfAbsent(store, s -> new StorageUnitSpaceStatistics());
  }

  private void removeStatistics(FileAttributes fileAttributes) {
    String store = fileAttributes.getStorageClass() + "@" + fileAttributes.getHsm();
    statistics.remove(store);
  }

  private synchronized void updateStatistics(EntryChangeEvent event, ReplicaState oldState,
      ReplicaState newState) {
    if (oldState == ReplicaState.CACHED || oldState == ReplicaState.PRECIOUS) {
      CacheEntry oldEntry = event.getOldEntry();
      StorageUnitSpaceStatistics stats = getStatistics(oldEntry.getFileAttributes());
      stats.totalBytes.addAndGet(-oldEntry.getReplicaSize());
      stats.totalEntries.decrementAndGet();
      if (isPrecious(oldEntry)) {
        stats.preciousBytes.addAndGet(-oldEntry.getReplicaSize());
        stats.preciousEntries.decrementAndGet();
      }
      if (isSticky(oldEntry)) {
        stats.stickyBytes.addAndGet(-oldEntry.getReplicaSize());
        stats.stickyEntries.decrementAndGet();
      }
      if (isOther(oldEntry)) {
        stats.otherBytes.addAndGet(-oldEntry.getReplicaSize());
        stats.otherEntries.decrementAndGet();
      }
      if (stats.totalEntries.get() == 0) {
        removeStatistics(oldEntry.getFileAttributes());
      }
    }
    if (newState == ReplicaState.CACHED || newState == ReplicaState.PRECIOUS) {
      CacheEntry newEntry = event.getNewEntry();
      StorageUnitSpaceStatistics stats = getStatistics(newEntry.getFileAttributes());
      stats.totalBytes.addAndGet(newEntry.getReplicaSize());
      stats.totalEntries.incrementAndGet();
      if (isPrecious(newEntry)) {
        stats.preciousBytes.addAndGet(newEntry.getReplicaSize());
        stats.preciousEntries.incrementAndGet();
      }
      if (isSticky(newEntry)) {
        stats.stickyBytes.addAndGet(newEntry.getReplicaSize());
        stats.stickyEntries.incrementAndGet();
      }
      if (isOther(newEntry)) {
        stats.otherBytes.addAndGet(newEntry.getReplicaSize());
        stats.otherEntries.incrementAndGet();
      }
    }
  }
}
