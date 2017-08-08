package org.dcache.pool.statistics;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import com.google.common.base.MoreObjects;

import java.util.concurrent.TimeUnit;

/**
 * This class stores statistics about read an write processes
 */

public class IoStatistics {

    private long _readRequestNum = 0;
    private double[] _readSpeeds = new double[8192];
    private int _readSpeedsArrayIndex = 0;
    private double _minReadSpeed;
    private double _maxReadSpeed;
    private double _avgReadSpeed;
    private double _95ReadSpeed;
    private long _totalReadTime;
    private long _totalReadBytes;
    private long _totalRequestedReadBytes;
    private long _avgRequestedReadBytes;

    private long _writeRequestNum = 0;
    private double[] _writeSpeeds = new double[8192];
    private int _writeSpeedsArrayIndex = 0;
    private double _minWriteSpeed;
    private double _maxWriteSpeed;
    private double _avgWriteSpeed;
    private double _95WriteSpeed;
    private long _totalWriteTime;
    private long _totalWrittenBytes;
    private long _totalRequestedWriteBytes;
    private long _avgRequestedWriteBytes;

    private final Percentile percentile = new Percentile(0.95);

    /**
     *
     * Update all statistics depending on read requests.
     *
     * @param  readBytes
     *         The number of read bytes; must be non-negative
     *
     * @param  readTime
     *         The duration of the read request in nanoseconds; must be non-negative
     *
     * @param  requestedReadBytes
     *         The number of requested read bytes; must be non-negative
     */

    public void updateRead(long readBytes, long readTime, long requestedReadBytes) {
        if(readBytes >= 0) {
            double readSpeed = calculateSpeed(readBytes, readTime);
            _readSpeeds[_readSpeedsArrayIndex] = readSpeed;
            _readSpeedsArrayIndex = (_readSpeedsArrayIndex + 1) % _readSpeeds.length;

            _minReadSpeed = (_readRequestNum == 0) ? readSpeed : Math.min(readSpeed, _minReadSpeed);
            _maxReadSpeed = (_readRequestNum == 0) ? readSpeed : Math.max(readSpeed, _maxReadSpeed);
            _avgReadSpeed = (_readRequestNum == 0) ? readSpeed : calculateNewAvg(_readRequestNum,_avgReadSpeed, readSpeed);
            _95ReadSpeed = (_readRequestNum == 0) ? readSpeed : percentile.evaluate(_readSpeeds, 0, (_readSpeedsArrayIndex - 1));

            _totalReadBytes += readBytes;
        }
        _totalReadTime += readTime;
        _readRequestNum ++;

        _totalRequestedReadBytes += requestedReadBytes;
        _avgRequestedReadBytes += (_readRequestNum == 0) ? requestedReadBytes : calculateNewAvg(_readRequestNum, _avgRequestedReadBytes, requestedReadBytes);
    }

    /**
     *
     * Update all statistics depending on write requests.
     *
     * @param  writeBytes
     *         The number of written bytes; must be non-negative
     *
     * @param  writeTime
     *         The duration of the write request in nanoseconds; must be non-negative
     *
     * @param  requestedWriteBytes
     *         The number of requested write bytes; must be non-negative
     */

    public void updateWrite(long writeBytes, long writeTime, long requestedWriteBytes) {

        double writeSpeed = calculateSpeed(writeBytes, writeTime);
        _writeSpeeds[_writeSpeedsArrayIndex] = writeSpeed;
        _writeSpeedsArrayIndex = (_writeSpeedsArrayIndex + 1) % _writeSpeeds.length;


        _minWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : Math.min(writeSpeed, _minWriteSpeed);
        _maxWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : Math.max(writeSpeed, _maxWriteSpeed);
        _avgWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : calculateNewAvg(_writeRequestNum,_avgWriteSpeed, writeSpeed);
        _95WriteSpeed = (_writeRequestNum == 0) ? writeSpeed : percentile.evaluate(_writeSpeeds, 0, (_writeSpeedsArrayIndex - 1));

        _totalWriteTime += writeTime;
        _writeRequestNum ++;
        _totalWrittenBytes += writeBytes;

        _totalRequestedWriteBytes += requestedWriteBytes;
        _avgRequestedWriteBytes += (_writeRequestNum == 0) ? requestedWriteBytes : calculateNewAvg(_writeRequestNum, _avgRequestedWriteBytes, requestedWriteBytes);
    }

    /**
     *
     * Calculate the speed of a request.
     *
     * @param  bytes
     *         The number of processed bytes; must be non-negative
     *
     * @param  time
     *         The duration of the request in milliseconds; must be non-negative
     *
     * @return The speed of the process in bytes per seconds.
     */

    private double calculateSpeed(long bytes, long time){
        return (double) bytes / ((double) time / TimeUnit.SECONDS.toNanos(1));
    }

    /**
     *
     * Calculate the new average of the speed of all read or write requests.
     *
     * @param  requestNum
     *         The number of read or write requests so far; must be non-negative
     *
     * @param  oldAvg
     *         The old average of the speed of all read or write requests; must be non-negative
     *
     * @param  newSpeed
     *         A new speed value of a read or write request; must be non-negative
     *
     * @return The new average of the speed of all read or write requests.
     */

    private double calculateNewAvg(long requestNum, double oldAvg, double newSpeed){
        return (requestNum * oldAvg + newSpeed) / (requestNum + 1);
    }

    public long getReadRequestNum() {
        return _readRequestNum;
    }

    public long getTotalReadBytes() {
        return _totalReadBytes;
    }

    public double getMinReadSpeed() {
        return _minReadSpeed;
    }

    public double getMaxReadSpeed() {
        return _maxReadSpeed;
    }

    public double getAvgReadSpeed() {
        return _avgReadSpeed;
    }

    public double get95ReadSpeed() {
        return _95ReadSpeed;
    }

    public long getTotalReadTime() {
        return _totalReadTime;
    }

    public long getWriteRequestNum() {
        return _writeRequestNum;
    }

    public long getTotalWrittenBytes() {
        return _totalWrittenBytes;
    }

    public double getMinWriteSpeed() {
        return _minWriteSpeed;
    }

    public double getMaxWriteSpeed() {
        return _maxWriteSpeed;
    }

    public double getAvgWriteSpeed() {
        return _avgWriteSpeed;
    }

    public double get95WriteSpeed() {
        return _95WriteSpeed;
    }

    public long getTotalWriteTime() {
        return _totalWriteTime;
    }

    public long getTotalRequestedReadBytes() {
        return _totalRequestedReadBytes;
    }

    public long getAvgRequestedReadBytes() {
        return _avgRequestedReadBytes;
    }

    public long getTotalRequestedWriteBytes() {
        return _totalRequestedWriteBytes;
    }

    public long getAvgRequestedWriteBytes() {
        return _avgRequestedWriteBytes;
    }

    public String toString(){

       return MoreObjects.toStringHelper(this)
                .add("number of read requests:", _readRequestNum)
                .add("min readSpeed:", _minReadSpeed)
                .add("max readSpeed:", _maxReadSpeed)
                .add("avg readSpeed:", _avgReadSpeed)
                .add("95percentile readSpeed:", _95ReadSpeed)
                .add("total readTime:", _totalReadTime)
                .add("total read bytes:", _totalReadBytes)
                .add("total requested read bytes:", _totalRequestedReadBytes)
                .add("avg requested read bytes:", _totalRequestedReadBytes)
                .add("number of write requests:", _writeRequestNum)
                .add("total written bytes:", _totalWrittenBytes)
                .add("min writeSpeed:", _minWriteSpeed)
                .add("max writeSpeed:", _maxWriteSpeed)
                .add("avg writeSpeed:", _avgWriteSpeed)
                .add("95percentile writeSpeed:", _95WriteSpeed)
                .add("total writeTime:", _totalWriteTime)
                .add("total requested write bytes:", _totalRequestedWriteBytes)
                .add("avg requested write bytes:", _totalRequestedWriteBytes)
                .omitNullValues()
                .toString();
    }

}