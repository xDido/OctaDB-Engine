package Octree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import src.DBConstants; // Import DBConstants

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

public class Node implements Serializable {
    @Serial
    private static final long serialVersionUID = 785303041728903L; // Example new UID
    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    private ArrayList<Point> Points; // Initialized in constructor
    private ArrayList<Point> Duplicates; // Initialized in constructor
    private int maxPoints;

    public Node(Object x, Object y, Object z, int PageIndex) {
        this.Points = new ArrayList<>();
        this.Duplicates = new ArrayList<>();

        File configFile = new File(DBConstants.CONFIG_FILE_PATH);
        try (FileReader reader = new FileReader(configFile)) {
            Properties props = new Properties();
            props.load(reader);
            maxPoints = Integer.parseInt(props.getProperty(DBConstants.CONFIG_MAX_ENTRIES_OCTREE_NODE));
            logger.debug("Node maxPoints set to {} from config.", maxPoints);
        } catch (FileNotFoundException e) {
            logger.error("Configuration file '{}' not found for Node.", DBConstants.CONFIG_FILE_PATH, e);
            throw new RuntimeException("Node configuration file '" + DBConstants.CONFIG_FILE_PATH + "' not found.", e);
        } catch (IOException e) {
            logger.error("IOException while reading or parsing '{}' for Node.", DBConstants.CONFIG_FILE_PATH, e);
            throw new RuntimeException("Error reading Node configuration from '" + DBConstants.CONFIG_FILE_PATH + "'.", e);
        } catch (NumberFormatException e) {
            logger.error("Invalid format for {} in '{}'.", DBConstants.CONFIG_MAX_ENTRIES_OCTREE_NODE, DBConstants.CONFIG_FILE_PATH, e);
            throw new RuntimeException("Invalid number format for " + DBConstants.CONFIG_MAX_ENTRIES_OCTREE_NODE + " in Node config.", e);
        }

        Point initialPoint = new Point(x, y, z, PageIndex);
        this.Points.add(initialPoint);
        logger.debug("Node created with initial point: {}. Max points: {}", initialPoint, maxPoints);
    }

    // Default constructor might be useful if a Node can be created empty and then configured.
    // For now, assuming a Node is always created with an initial point.
    // public Node() {
    //     this.Points = new ArrayList<>();
    //     this.Duplicates = new ArrayList<>();
    //     // maxPoints would need to be loaded or set.
    //     logger.debug("Empty Node created. maxPoints not yet set from config.");
    // }


    @Override
    public String toString() {
        return "Node{" +
                "maxPoints=" + maxPoints +
                ", pointsCount=" + (Points != null ? Points.size() : "null") +
                ", duplicatesCount=" + (Duplicates != null ? Duplicates.size() : "null") +
                // Example: First point if exists, to avoid overly long string
                ", firstPoint=" + (Points != null && !Points.isEmpty() ? Points.get(0) : "none") +
                '}';
    }

    public ArrayList<Point> getPoints() {
        return Points;
    }

    public void setPoints(ArrayList<Point> points) {
        logger.trace("Setting points for Node. New points size: {}", (points != null ? points.size() : "null"));
        Points = points;
    }

    public ArrayList<Point> getDuplicates() {
        return Duplicates;
    }

    public void setDuplicates(ArrayList<Point> duplicates) {
        logger.trace("Setting duplicates for Node. New duplicates size: {}", (duplicates != null ? duplicates.size() : "null"));
        Duplicates = duplicates;
    }

    public int getMaxPoints() {
        return maxPoints;
    }

    public void setMaxPoints(int maxPoints) {
        logger.trace("Setting maxPoints for Node to: {}", maxPoints);
        this.maxPoints = maxPoints;
    }
}
