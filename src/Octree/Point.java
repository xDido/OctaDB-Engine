package Octree;

import java.io.Serializable;

public class Point implements Serializable {
    private Object x;
    private Object y;
    private Object z;
    private int ref;
    private Object strClusteringKey;


    public Point(Object x, Object y, Object z, int ref) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.ref = ref;
    }

    public void setX(Object x) {
        this.x = x;
    }

    public void setY(Object y) {
        this.y = y;
    }

    public void setZ(Object z) {
        this.z = z;
    }

    public void setRef(int ref) {
        this.ref = ref;
    }

    public Object getX() {
        return x;
    }

    public Object getY() {
        return y;
    }

    public Object getZ() {
        return z;
    }

    public int getRef() {
        return ref;
    }

    @Override
    public String toString() {
        return "Octree.Point [x=" + x + ", y=" + y + ", z=" + z + ", ref=" + ref + "]";
    }

}
