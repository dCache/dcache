package org.dcache.pool.statistics;

import java.util.ArrayList;

public class Statistics {

    private long _readRequestNum = 0;
    private long _readBytes;
    private ArrayList<Float> _readSpeeds;
    private float _minReadSpeed;
    private float _maxReadSpeed;
    private float _avgReadSpeed;
    private float _95ReadSpeed;
    private long _totalReadTime;

    private long _writeRequestNum = 0;
    private long _writeBytes;
    private ArrayList<Float> _writeSpeeds;
    private float _minWriteSpeed;
    private float _maxWriteSpeed;
    private float _avgWriteSpeed;
    private float _95WriteSpeed;
    private long _totalWriteTime;

    public void updateRead(long readBytes, long readTime){

        float readSpeed = calculateSpeed(readBytes, readTime);
        _readSpeeds.add(readSpeed);

        _minReadSpeed = (_readRequestNum == 0) ? readSpeed : Math.min(readSpeed, _minReadSpeed);
        _maxReadSpeed = (_readRequestNum == 0) ? readSpeed : Math.max(readSpeed, _maxReadSpeed);
        _avgReadSpeed = (_readRequestNum == 0) ? readSpeed : calculateNewAvg(_readRequestNum,_avgReadSpeed, readSpeed);
        _95ReadSpeed = (_readRequestNum == 0) ? readSpeed : calculate95Percentile((_readRequestNum + 1), _readSpeeds);

        _totalReadTime += readTime;
        _readRequestNum ++;
        _readBytes += readBytes;
    }

    public void updateWrite(long writeBytes, long writeTime){
        float writeSpeed = calculateSpeed(writeBytes, writeTime);
        _writeSpeeds.add(writeSpeed);

        _minWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : Math.min(writeSpeed, _minWriteSpeed);
        _maxWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : Math.max(writeSpeed, _maxWriteSpeed);
        _avgWriteSpeed = (_writeRequestNum == 0) ? writeSpeed : calculateNewAvg(_writeRequestNum,_avgWriteSpeed, writeSpeed);
        _95WriteSpeed = (_writeRequestNum == 0) ? writeSpeed : calculate95Percentile((_writeRequestNum + 1), _writeSpeeds);

        _totalWriteTime += writeTime;
        _writeRequestNum ++;
        _writeBytes += writeBytes;
    }

    private float calculateSpeed(long bytes, long time){
        return (float) bytes / (float) time;
    }

    private float calculateNewAvg(long requestNum, float oldAvg, float newSpeed){
        return (requestNum * oldAvg + newSpeed) / (requestNum + 1);
    }

    private float calculate95Percentile(long requestNum, ArrayList<Float> speeds){
        float index = (float) 0.9 * (float) requestNum;

        if (Math.ceil(index) == index){
            return (float) 0.5 * (speeds.get((int)index) + speeds.get((int) index + 1));
        } else {
            return speeds.get((int)Math.ceil(index));
        }
    }

    public long getReadRequestNum() {
        return _readRequestNum;
    }

    public long getReadBytes() {
        return _readBytes;
    }

    public ArrayList<Float> getReadSpeeds() {
        return _readSpeeds;
    }

    public float getMinReadSpeed() {
        return _minReadSpeed;
    }

    public float getMaxReadSpeed() {
        return _maxReadSpeed;
    }

    public float getAvgReadSpeed() {
        return _avgReadSpeed;
    }

    public float get95ReadSpeed() {
        return _95ReadSpeed;
    }

    public long getTotalReadTime() {
        return _totalReadTime;
    }

    public long getWriteRequestNum() {
        return _writeRequestNum;
    }

    public long getWriteBytes() {
        return _writeBytes;
    }

    public ArrayList<Float> getWriteSpeeds() {
        return _writeSpeeds;
    }

    public float getMinWriteSpeed() {
        return _minWriteSpeed;
    }

    public float getMaxWriteSpeed() {
        return _maxWriteSpeed;
    }

    public float getAvgWriteSpeed() {
        return _avgWriteSpeed;
    }

    public float get95WriteSpeed() {
        return _95WriteSpeed;
    }

    public long getTotalWriteTime() {
        return _totalWriteTime;
    }
}
