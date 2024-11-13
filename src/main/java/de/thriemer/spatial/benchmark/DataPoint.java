package de.thriemer.spatial.benchmark;

import java.util.Objects;

import static de.thriemer.spatial.framework.Helper.isEqual;

public final class DataPoint {
    private final double longitude;
    private final double latitude;
    private final int id;
    private final float someFloat;
    private final String tags;

    public DataPoint(double longitude, double latitude, int id, float someFloat, String tags) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.id = id;
        this.someFloat = someFloat;
        this.tags = tags;
    }

    public double longitude() {
        return longitude;
    }

    public double latitude() {
        return latitude;
    }

    public int id() {
        return id;
    }

    public float someFloat() {
        return someFloat;
    }

    public String tags() {
        return tags;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DataPoint) obj;
        return isEqual(this.longitude, that.longitude) &&
                isEqual(this.latitude, that.latitude) &&
                this.id == that.id &&
                isEqual(this.someFloat, that.someFloat) &&
                Objects.equals(this.tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(longitude, latitude, id, someFloat, tags);
    }

    @Override
    public String toString() {
        return "DataPoint[" +
                "longitude=" + longitude + ", " +
                "latitude=" + latitude + ", " +
                "id=" + id + ", " +
                "someFloat=" + someFloat + ", " +
                "tags=" + tags + ']';
    }

}
