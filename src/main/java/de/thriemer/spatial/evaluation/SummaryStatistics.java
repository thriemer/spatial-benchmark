package de.thriemer.spatial.evaluation;

import jakarta.persistence.Embeddable;
import jakarta.persistence.MappedSuperclass;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.Objects;


@NoArgsConstructor
@AllArgsConstructor
@MappedSuperclass
@Embeddable
public class SummaryStatistics {

    public static final String ERROR_IN_RUN = "error";

    protected String name;
    protected String unit;
    protected double avg;
    protected double first;
    protected double min;
    protected double max;
    protected double std;
    protected int sampleCount;

    public double coefficientOfVariation() {
        return std / avg;
    }

    public String name() {
        return name;
    }

    public String unit() {
        return unit;
    }

    public double avg() {
        return avg;
    }

    public double first() {
        return first;
    }

    public double min() {
        return min;
    }

    public double max() {
        return max;
    }

    public double std() {
        return std;
    }

    public double variance() {
        return Math.pow(std, 2);
    }

    public int degreesOfFreedom() {
        return sampleCount - 1;
    }

    public int sampleCount() {
        return sampleCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SummaryStatistics) obj;
        return Objects.equals(this.name, that.name) && Objects.equals(this.unit, that.unit) && Double.doubleToLongBits(this.avg) == Double.doubleToLongBits(that.avg) && Double.doubleToLongBits(this.first) == Double.doubleToLongBits(that.first) && Double.doubleToLongBits(this.min) == Double.doubleToLongBits(that.min) && Double.doubleToLongBits(this.max) == Double.doubleToLongBits(that.max) && Double.doubleToLongBits(this.std) == Double.doubleToLongBits(that.std) && this.sampleCount == that.sampleCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, unit, avg, first, min, max, std, sampleCount);
    }

    @Override
    public String toString() {
        return "SummaryStatistics[" + "name=" + name + ", " + "unit=" + unit + ", " + "avg=" + avg + ", " + "first=" + first + ", " + "min=" + min + ", " + "max=" + max + ", " + "std=" + std + ", " + "sampleCount=" + sampleCount + ']';
    }

    public double standardError() {
        return std / Math.sqrt(sampleCount);
    }
}
