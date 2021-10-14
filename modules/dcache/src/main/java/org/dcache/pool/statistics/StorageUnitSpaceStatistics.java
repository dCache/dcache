package org.dcache.pool.statistics;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

public class StorageUnitSpaceStatistics implements Serializable {

    private static final long serialVersionUID = 2882047073656851531L;

    public static void aggregate(Map<String, StorageUnitSpaceStatistics> current,
          Map<String, StorageUnitSpaceStatistics> using) {
        using.keySet().stream()
              .forEach(key -> {
                  StorageUnitSpaceStatistics nextValue = using.get(key);
                  StorageUnitSpaceStatistics currentValue
                        = current.computeIfAbsent(key, k -> new StorageUnitSpaceStatistics());
                  currentValue.aggregateData(nextValue);
              });
    }

    final AtomicLong totalBytes = new AtomicLong(0L);
    final AtomicInteger totalEntries = new AtomicInteger(0);
    final AtomicLong preciousBytes = new AtomicLong(0L);
    final AtomicInteger preciousEntries = new AtomicInteger(0);
    final AtomicLong stickyBytes = new AtomicLong(0L);
    final AtomicInteger stickyEntries = new AtomicInteger(0);
    final AtomicLong otherBytes = new AtomicLong(0L);
    final AtomicInteger otherEntries = new AtomicInteger(0);

    public StorageUnitSpaceStatistics() {
    }

    public void aggregateData(@Nonnull StorageUnitSpaceStatistics using) {
        totalBytes.addAndGet(using.totalBytes.get());
        totalEntries.addAndGet(using.totalEntries.get());
        preciousBytes.addAndGet(using.preciousBytes.get());
        preciousEntries.addAndGet(using.preciousEntries.get());
        stickyBytes.addAndGet(using.stickyBytes.get());
        stickyEntries.addAndGet(using.stickyEntries.get());
        otherBytes.addAndGet(using.otherBytes.get());
        otherEntries.addAndGet(using.otherEntries.get());
    }

    public Long getTotalBytes() {
        return totalBytes.get();
    }

    public void setTotalBytes(Long totalBytes) {
        this.totalBytes.set(totalBytes);
    }

    public Integer getTotalEntries() {
        return totalEntries.get();
    }

    public void setTotalEntries(Integer totalEntries) {
        this.totalEntries.set(totalEntries);
    }

    public Long getPreciousBytes() {
        return preciousBytes.get();
    }

    public void setPreciousBytes(Long preciousBytes) {
        this.preciousBytes.set(preciousBytes);
    }

    public Integer getPreciousEntries() {
        return preciousEntries.get();
    }

    public void setPreciousEntries(Integer preciousEntries) {
        this.preciousEntries.set(preciousEntries);
    }

    public Long getStickyBytes() {
        return stickyBytes.get();
    }

    public void setStickyBytes(Long stickyBytes) {
        this.stickyBytes.set(stickyBytes);
    }

    public Integer getStickyEntries() {
        return stickyEntries.get();
    }

    public void setStickyEntries(Integer stickyEntries) {
        this.stickyEntries.set(stickyEntries);
    }

    public Long getOtherBytes() {
        return otherBytes.get();
    }

    public void setOtherBytes(Long otherBytes) {
        this.otherBytes.set(otherBytes);
    }

    public Integer getOtherEntries() {
        return otherEntries.get();
    }

    public void setOtherEntries(Integer otherEntries) {
        this.otherEntries.set(otherEntries);
    }

    public long[] toArray() {
        return new long[]{totalBytes.get(), totalEntries.get(), preciousBytes.get(),
              preciousEntries.get(),
              stickyBytes.get(), stickyEntries.get(), otherBytes.get(), otherEntries.get()};
    }
}
