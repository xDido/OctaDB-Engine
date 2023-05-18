package Octree;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

public class Node implements Serializable {
    private ArrayList<Point> Points = new ArrayList<>();

    private ArrayList<Point> Duplicates;
    private int maxPoints;

    public Node(Object x, Object y, Object z, int PageIndex) {

        File configFile = new File("config/DBApp.config");
        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);
            maxPoints = Integer.parseInt(props.getProperty("MaximumEntriesinOctreeNode"));

        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
        Points.add(new Point(x, y, z, PageIndex));
        Duplicates = new ArrayList<Point>();

    }

    public String toString() {
        return Points.toString();
    }

    public ArrayList<Point> getPoints() {
        return Points;
    }

    public void setPoints(ArrayList<Point> points) {
        Points = points;
    }

    public ArrayList<Point> getDuplicates() {
        return Duplicates;
    }

    public void setDuplicates(ArrayList<Point> duplicates) {
        Duplicates = duplicates;
    }

    public int getMaxPoints() {
        return maxPoints;
    }

    public void setMaxPoints(int maxPoints) {
        this.maxPoints = maxPoints;
    }
}
