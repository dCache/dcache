package org.dcache.pool.statistics;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 * This class stores statistics about read an write processes
 */

public class IoStatistics {

    private long _readRequestNum = 0;
    private long _readBytes;
    private double[] _readSpeeds = new double[8192];
    private int _readSpeedsArrayIndex = 0;
    private double _minReadSpeed;
    private double _maxReadSpeed;
    private double _avgReadSpeed;
    private double _95ReadSpeed;
    private long _totalReadTime;

    private long _writeRequestNum = 0;
    private long _writtenBytes;
    private double[] _writeSpeeds = new double[8192];
    private int _writeSpeedsArrayIndex = 0;
    private double _minWriteSpeed;
    private double _maxWriteSpeed;
    private double _avgWriteSpeed;
    private double _95WriteSpeed;
    private long _totalWriteTime;

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
     */

    public void updateRead(long readBytes, long readTime) throws ArrayIndexOutOfBoundsException{

        double readSpeed = calculateSpeed(readBytes, readTime);
        _readSpeeds[_readSpeedsArrayIndex] = readSpeed;
        _readSpeedsArrayIndex++;

        _minReadSpeed = (_readRequestNum == 0) ? readSpeed : Math.min(readSpeed, _minReadSpeed);
        _maxReadSpeed = (_readRequestNum == 0) ? readSpeed : Math.max(readSpeed, _maxReadSpeed);
        _avgReadSpeed = (_readRequestNum == 0) ? readSpeed : calculateNewAvg(_readRequestNum,_avgReadSpeed, readSpeed);
        _95ReadSpeed = (_readRequestNum == 0) ? readSpeed : percentile.evaluate(_readSpeeds, 0, (_readSpeedsArrayIndex - 1));

        _totalReadTime += readTime;
        _readRequestNum ++;
        _readBytes += readBytes;
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
     */

    public void updateWrite(long writeBytes, long writeTime) throws ArrayIndexOutOfBoundsException{

        double writeSpeed = calculateSpeed(writeBytes, writeTime);
        _writeSpeeds[_writeSpeedsArrayIndex] = writeSpeed;
        _writeSpeedsArrayIndex++;


        _minWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : Math.min(writeSpeed, _minWriteSpeed);
        _maxWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : Math.max(writeSpeed, _maxWriteSpeed);
        _avgWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : calculateNewAvg(_writeRequestNum,_avgWriteSpeed, writeSpeed);
        _95WriteSpeed = (_writeRequestNum == 0) ? writeSpeed : percentile.evaluate(_writeSpeeds, 0, (_writeSpeedsArrayIndex - 1));

        _totalWriteTime += writeTime;
        _writeRequestNum ++;
        _writtenBytes += writeBytes;
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
     * @return The speed of the process in bytes per nanoseconds.
     */

    private double calculateSpeed(long bytes, long time){
        return (double) bytes / (double) time;
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

    public long getReadBytes() {
        return _readBytes;
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

    public long getWrittenBytes() {
        return _writtenBytes;
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
}