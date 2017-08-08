package org.dcache.pool.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

public class Statistics {

    private long _readRequestNum = 0;
    private long _readBytes;
    private ArrayList<Double> _readSpeeds;
    private long _minReadSpeed;
    private long _maxReadSpeed;
    private long _avgReadSpeed;
    private long _95ReadSpeed;
    private long _totalReadTime;

    private long _writeRequestNum = 0;
    private long _writeBytes;
    private ArrayList<Double> _writeSpeeds;
    private long _minWriteSpeed;
    private long _maxWriteSpeed;
    private long _avgWriteSpeed;
    private long _95WriteSpeed;
    private long _totalWriteTime;

    public void updateRead(long readBytes, long readTime){

        long readSpeed = calculateSpeed(readBytes, readTime)
        _minReadSpeed = (_readRequestNum == 0) ? readSpeed : Math.min(readSpeed, _minReadSpeed);
        _maxReadSpeed = (_readRequestNum == 0) ? readSpeed : Math.max(readSpeed, _maxReadSpeed);
        _avgReadSpeed = (_readRequestNum == 0) ? readSpeed : (_readRequestNum * _avgReadSpeed + readSpeed) / (_readRequestNum + 1);

        _readRequestNum ++;


        _readBytes += readBytes;
        _readSpeeds.add(calculateSpeed(readBytes, readTime));
        _totalReadTime += readTime;
    }



    public void updateWrite(long writeBytes, long writeTime){
        _writeRequestNum ++;
        _writeBytes += writeBytes;
        _writeSpeeds.add(calculateSpeed(writeBytes, writeTime));
        _totalWriteTime += writeTime;
    }

    private long calculateSpeed(long readBytes, long readTime){
        return Math.round((float)readBytes / (float)readTime);
    }



    /*public Double getAvgReadSpeed(){
        return _readSpeeds.stream().collect(Collectors.groupingBy(
                d -> d.name,
                Collectors.summarizingInt(d -> d.value)))
                .forEach((name, summary) -> System.out.println(name + ": " + summary));
    }*/
}
