package Octree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays; // For Arrays.toString

public class Octree implements Serializable {
    @Serial
    private static final long serialVersionUID = 682988397201384672L; // New serialVersionUID
    private static final Logger logger = LoggerFactory.getLogger(Octree.class);

    private Node root;
    private Octree[] childNodes; // Array of 8 children
    private boolean parent;

    // Bounds of this Octree node
    private Object minX, minY, minZ;
    private Object maxX, maxY, maxZ;

    // Column names this Octree is based on (relevant for the root of an index)
    private String col1, col2, col3;


    public Octree(Node p) {
        this.root = p;
        this.parent = false; // A new Octree with a specific root node is initially a leaf.
        // Min/max bounds and column names should be set externally after creation.
        logger.debug("Octree created with root node: {}", p);
    }

    public Octree() {
        this.root = null;
        this.parent = false;
        this.childNodes = null; // Explicitly null, will be new Octree[8] when it becomes a parent
        logger.debug("Empty Octree created.");
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        logger.trace("Setting root for Octree to: {}", root);
        this.root = root;
    }

    public Object getMinX() {
        return minX;
    }

    public void setMinX(Object minX) {
        logger.trace("Setting minX to: {}", minX);
        this.minX = minX;
    }

    public Object getMinY() {
        return minY;
    }

    public void setMinY(Object minY) {
        logger.trace("Setting minY to: {}", minY);
        this.minY = minY;
    }

    public Object getMinZ() {
        return minZ;
    }

    public void setMinZ(Object minZ) {
        logger.trace("Setting minZ to: {}", minZ);
        this.minZ = minZ;
    }

    public Object getMaxX() {
        return maxX;
    }

    public void setMaxX(Object maxX) {
        logger.trace("Setting maxX to: {}", maxX);
        this.maxX = maxX;
    }

    public Object getMaxY() {
        return maxY;
    }

    public void setMaxY(Object maxY) {
        logger.trace("Setting maxY to: {}", maxY);
        this.maxY = maxY;
    }

    public Object getMaxZ() {
        return maxZ;
    }

    public void setMaxZ(Object maxZ) {
        logger.trace("Setting maxZ to: {}", maxZ);
        this.maxZ = maxZ;
    }

    public String getCol1() {
        return col1;
    }

    public void setCol1(String col1) {
        logger.trace("Setting col1 to: {}", col1);
        this.col1 = col1;
    }

    public String getCol2() {
        return col2;
    }

    public void setCol2(String col2) {
        logger.trace("Setting col2 to: {}", col2);
        this.col2 = col2;
    }

    public String getCol3() {
        return col3;
    }

    public void setCol3(String col3) {
        logger.trace("Setting col3 to: {}", col3);
        this.col3 = col3;
    }

    public boolean isParent() {
        return parent;
    }

    public void setParent(boolean parent) {
        logger.trace("Setting parent status to: {}", parent);
        this.parent = parent;
        if (parent && this.childNodes == null) {
            logger.trace("Node set as parent, initializing childNodes array (if null).");
            this.childNodes = new Octree[8]; // Ensure childNodes is initialized when becoming a parent
        } else if (!parent) {
            // If it's no longer a parent, child nodes should ideally be cleared/nulled
            // to free resources and reflect state, but original logic doesn't do this.
            // For now, just setting the flag.
        }
    }

    public Octree[] getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(Octree[] childNodes) {
        logger.trace("Setting childNodes. Length: {}", (childNodes != null ? childNodes.length : "null"));
        this.childNodes = childNodes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Octree{");
        sb.append("bounds=[(").append(minX).append(",").append(minY).append(",").append(minZ).append(")-(")
                .append(maxX).append(",").append(maxY).append(",").append(maxZ).append(")]");
        sb.append(", isParent=").append(parent);
        if (col1 != null) sb.append(", col1='").append(col1).append('\'');
        if (col2 != null) sb.append(", col2='").append(col2).append('\'');
        if (col3 != null) sb.append(", col3='").append(col3).append('\'');
        sb.append(", root=").append(root != null ? root.toString().substring(0, Math.min(root.toString().length(), 50))+"..." : "null"); // Avoid overly long root string
        sb.append(", hasChildren=").append(childNodes != null && Arrays.stream(childNodes).anyMatch(java.util.Objects::nonNull));
        sb.append('}');
        return sb.toString();
    }
}
