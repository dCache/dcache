package org.dcache.pool.statistics;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import com.google.common.base.MoreObjects;

import java.util.concurrent.TimeUnit;

/**
 * This class stores statistics about read and write processes of an Repository Channel
 */

public class IoStatistics {

    /**
     * The number of read requests
     */
    private int _readRequestNum = 0;

    /**
     * The number of read requests with a non negative return value
     */
    private int _nonNegativeReadRequestNum = 0;

    /**
     * The array of speeds of all read requests
     */
    private double[] _readSpeeds = new double[8192];

    /**
     * The index of the next fillable field of the _readSpeeds Array
     */
    private int _readSpeedsArrayIndex = 0;

    /**
     * The minimum value of speeds of all read requests;
     * initial value is maximum double value
     */
    private double _minReadSpeed = Double.MAX_VALUE;

    /**
     * The maximum value of speeds of all read requests
     * initial value is minimum double value
     */
    private double _maxReadSpeed = Double.MIN_VALUE;

    /**
     * The average value of speeds of all read requests
     */
    private double _avgReadSpeed = 0;

    /**
     * The 95 percentile value of speeds of all read requests
     */
    private double _95ReadSpeed = 0;

    /**
     * The total read time
     */
    private long _totalReadTime = 0;

    /**
     * The number of effective read bytes
     */
    private long _totalReadBytes = 0;

    /**
     * The number of requested read bytes
     */
    private long _totalRequestedReadBytes = 0;

    /**
     * The average value of requested read bytes
     */
    private double _avgRequestedReadBytes = 0;

    /**
     * The number of write requests
     */
    private int _writeRequestNum = 0;

    /**
     * The array of speeds of all write requests
     */
    private double[] _writeSpeeds = new double[8192];

    /**
     * The index of the next fillable field of the _writeSpeeds Array
     */
    private int _writeSpeedsArrayIndex = 0;

    /**
     * The minimum value of speeds of all write requests
     */
    private double _minWriteSpeed = Double.MAX_VALUE;

    /**
     * The maximum value of speeds of all write requests
     */
    private double _maxWriteSpeed = Double.MIN_VALUE;

    /**
     * The average value of speeds of all write requests
     */
    private double _avgWriteSpeed = 0;

    /**
     * The 95 percentile value of speeds of all write requests
     */
    private double _95WriteSpeed = 0;

    /**
     * The total write time
     */
    private long _totalWriteTime = 0;

    /**
     * The number of effective written bytes
     */
    private long _totalWrittenBytes = 0;

    /**
     * The number of requested write bytes
     */
    private long _totalRequestedWriteBytes = 0;

    /**
     * The average value of requested write bytes
     */
    private double _avgRequestedWriteBytes = 0;

    /**
     * The class which calculate the 95 percentile value of speeds of all read and write requests
     */
    private final Percentile percentile = new Percentile(0.95);

    /**
     *
     * Update all statistics depending on read requests.
     *
     * @param  readBytes
     *         The number of read bytes;
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

            _minReadSpeed = Math.min(readSpeed, _minReadSpeed);
            _maxReadSpeed = Math.max(readSpeed, _maxReadSpeed);
            _avgReadSpeed = calculateNewAvg(_nonNegativeReadRequestNum, _avgReadSpeed, readSpeed);
            _95ReadSpeed = percentile.evaluate(_readSpeeds, 0, Math.min(_readSpeeds.length, _nonNegativeReadRequestNum));

            _totalReadBytes += readBytes;

            _nonNegativeReadRequestNum++;
        }
        _totalReadTime += readTime;
        _totalRequestedReadBytes += requestedReadBytes;
        _avgRequestedReadBytes = calculateNewAvg(_readRequestNum, _avgRequestedReadBytes, requestedReadBytes);

        _readRequestNum++;
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


        _minWriteSpeed = Math.min(writeSpeed, _minWriteSpeed);
        _maxWriteSpeed = Math.max(writeSpeed, _maxWriteSpeed);
        _avgWriteSpeed = calculateNewAvg(_writeRequestNum, _avgWriteSpeed, writeSpeed);
        _95WriteSpeed = percentile.evaluate(_writeSpeeds, 0, Math.min(_writeSpeeds.length, _writeRequestNum + 1));

        _totalWriteTime += writeTime;
        _totalWrittenBytes += writeBytes;
        _totalRequestedWriteBytes += requestedWriteBytes;
        _avgRequestedWriteBytes = calculateNewAvg(_writeRequestNum, _avgRequestedWriteBytes, requestedWriteBytes);

        _writeRequestNum++;
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

    public int getReadRequestNum() {
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

    public long getTotalRequestedReadBytes() {
        return _totalRequestedReadBytes;
    }

    public double getAvgRequestedReadBytes() {
        return _avgRequestedReadBytes;
    }

    public int getNegativeReadRequestNum() { return _readRequestNum - _nonNegativeReadRequestNum; }

    public int getWriteRequestNum() {
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

    public long getTotalRequestedWriteBytes() {
        return _totalRequestedWriteBytes;
    }

    public double getAvgRequestedWriteBytes() {
        return _avgRequestedWriteBytes;
    }

    @Override
    public String toString(){

       return MoreObjects.toStringHelper(this)
                .add("number of read requests: ", _readRequestNum)
                .add("number of read requests with a negative return value: ", getNegativeReadRequestNum())
                .add("minimum speed of read requests: ", _minReadSpeed)
                .add("maximum speed of read requests: ", _maxReadSpeed)
                .add("average speed of read requests: ", _avgReadSpeed)
                .add("95 percentile speed of read requests: ", _95ReadSpeed)
                .add("total read time: ", _totalReadTime)
                .add("total read bytes: ", _totalReadBytes)
                .add("total requested read bytes: ", _totalRequestedReadBytes)
                .add("average requested read bytes: ", _totalRequestedReadBytes)
                .add("number of write requests: ", _writeRequestNum)
                .add("minimum speed of write requests: ", _minWriteSpeed)
                .add("maximum speed of write requests: ", _maxWriteSpeed)
                .add("average speed of write requests: ", _avgWriteSpeed)
                .add("95 percentile speed of write requests: ", _95WriteSpeed)
                .add("total write time: ", _totalWriteTime)
                .add("total written bytes: ", _totalWrittenBytes)
                .add("total requested write bytes: ", _totalRequestedWriteBytes)
                .add("average requested write bytes: ", _totalRequestedWriteBytes)
                .toString();
    }

}