package Octree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public class Point implements Serializable {
    @Serial
    private static final long serialVersionUID = 1234567890123456789L; // Example UID
    private static final Logger logger = LoggerFactory.getLogger(Point.class);

    private Object x;
    private Object y;
    private Object z;
    private int ref;
    // private Object strClusteringKey; // This field was unused, removing it. If needed, can be re-added.

    public Point(Object x, Object y, Object z, int ref) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.ref = ref;
        // Logger might be too verbose for every point creation if many points are made.
        // Using trace if it's frequent, debug if less so.
        logger.trace("Point created: x={}, y={}, z={}, ref={}", x, y, z, ref);
    }

    public Object getX() {
        return x;
    }

    public void setX(Object x) {
        logger.trace("Setting x for point (ref {}): oldX={}, newX={}", this.ref, this.x, x);
        this.x = x;
    }

    public Object getY() {
        return y;
    }

    public void setY(Object y) {
        logger.trace("Setting y for point (ref {}): oldY={}, newY={}", this.ref, this.y, y);
        this.y = y;
    }

    public Object getZ() {
        return z;
    }

    public void setZ(Object z) {
        logger.trace("Setting z for point (ref {}): oldZ={}, newZ={}", this.ref, this.z, z);
        this.z = z;
    }

    public int getRef() {
        return ref;
    }

    public void setRef(int ref) {
        logger.trace("Setting ref for point (x,y,z: {},{},{}): oldRef={}, newRef={}", this.x, this.y, this.z, this.ref, ref);
        this.ref = ref;
    }

    @Override
    public String toString() {
        return "Point{" +
                "x=" + x +
                ", y=" + y +
                ", z=" + z +
                ", ref=" + ref +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return ref == point.ref &&
                Objects.equals(x, point.x) &&
                Objects.equals(y, point.y) &&
                Objects.equals(z, point.z);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, z, ref);
    }
}
