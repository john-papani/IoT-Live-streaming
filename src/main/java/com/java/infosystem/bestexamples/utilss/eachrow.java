package com.java.infosystem.bestexamples.utilss;

import java.util.Date;
import java.util.Objects;

public class eachrow {
    // @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy//
    // hh:mm:ss:SSS")
    private int timeday;
    // private Long mpampis;

    private int temperature;
    private String description;

    private double th1;
    private double th2;
    private double hvac1;
    private double hvac2;
    private double miac1;
    private double miac2;
    private double etot;
    private double mov1;
    private double w1;
    private double wtot;

    public eachrow() {
    }

    public eachrow(final int timeday, final int temperature, final String description, final double th1,
            final double th2, final double hvac1, final double hvac2, final double miac1, final double miac2,
            final double etot, final double mov1, final double w1, final double wtot) {
        this.timeday = timeday;
        this.temperature = temperature;
        this.description = description;
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

    public int getTimeday() {
        return timeday;
    }

    public void setTimeday(final int timeday) {
        this.timeday = timeday;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(final int temperature) {
        this.temperature = temperature;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public void setTh1(final double th1) {
        this.th1 = th1;
    }

    public double getTh1() {
        return th1;
    }

    public void setTh2(final double th2) {
        this.th2 = th2;
    }

    public double getTh2() {
        return th2;
    }

    public void setHvac1(final double hvac1) {
        this.hvac1 = hvac1;
    }

    public double getHvac1() {
        return hvac1;
    }

    public void setHvac2(final double hvac2) {
        this.hvac2 = hvac2;
    }

    public double getHvac2() {
        return hvac2;
    }

    public void setMiac1(final double miac1) {
        this.miac1 = miac1;
    }

    public double getMiac1() {
        return miac1;
    }

    public void setMiac2(final double miac2) {
        this.miac2 = miac2;
    }

    public double getMiac2() {
        return miac2;
    }

    public void setEtot(final double etot) {
        this.etot = etot;
    }

    public double getEtot() {
        return etot;
    }

    public void setMov1(final double mov1) {
        this.mov1 = mov1;
    }

    public double getMov1() {
        return mov1;
    }

    public void setW1(final double w1) {
        this.w1 = w1;
    }

    public double getW1() {
        return w1;
    }

    public void setWtot(final double wtot) {
        this.wtot = wtot;
    }

    public double getWtot() {
        return wtot;
    }

    @Override
    public int hashCode() {
        // return Objects.hash(timeday, temperature, th1, th2, hvac1, hvac2);
        return Objects.hash(timeday, temperature, description);
    }

    @Override
    public String toString() {
        // final StringBuilder sb = new StringBuilder("eachrow{");
        final StringBuilder sb = new StringBuilder("{");
        sb.append("timeday=").append(timeday);
        sb.append(", temperature=").append(temperature);
        // sb.append(", th1=").append(th1);
        // sb.append(", th2=").append(th2);

        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
