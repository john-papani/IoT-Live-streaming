package com.infosystem.files.utils;

import java.util.Objects;

import scala.Int;

public class eachrow {
    public int timeday;

    public int th1;
    public int th2;
    public int hvac1;
    public int hvac2;
    public int miac1;
    public int miac2;
    public int etot;
    public int mov1;
    public double w1;
    public int wtot;

    public eachrow() {
    }

    public eachrow(final int timeday, final int th1,
            final int th2, final int hvac1, final int hvac2, final int miac1, final int miac2,
            final int etot, final int mov1, final double w1, final int wtot) {
        this.timeday = timeday;
        this.th1 = th1;
        this.th2 = th2;
        this.hvac1 = hvac1;
        this.hvac2 = hvac2;
        this.miac1 = miac1;
        this.miac2 = miac2;
        this.etot = etot;
        this.mov1 = mov1;
        this.w1 = w1;
        this.wtot = wtot;

    }

    public int getDay() {
        return ((new java.util.Date((long) timeday * 1000)).getDate());
    }

    public long getTimeday() {
        return timeday;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeday, th1);
    }

    @Override
    public String toString() {
        // print some of the sensors - this is only for testing 
        final StringBuilder sb = new StringBuilder("{");
        sb.append("timestamp:").append(timeday);
        sb.append(", th1: ").append(th1);
        sb.append(", th2: ").append(th2);
        sb.append(", w1: ").append(w1);
        sb.append(", hvac1: ").append(hvac1).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
