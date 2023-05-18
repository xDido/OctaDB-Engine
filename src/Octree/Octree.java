package Octree;

import java.io.Serializable;

public class Octree implements Serializable {

    private Node root;
    private Octree[] childNodes;
    private boolean parent;
    private Object minX;
    private Object minY;
    private Object minZ;
    private Object maxX;
    private Object maxY;
    private Object maxZ;
    private String col1;
    private String col2;
    private String col3;


    public Octree(Node p) {

        this.root = p;
        //you have to initiate the 6 values of the max and min in the main method

    }

    public Octree() {

        root = null;
        parent = false;
        //you have to initiate the 6 values of the max and min in the main method
    }
    public void toStringbta3ty(int level) {
        System.out.print( "OctreeNode "+level+" {" +
                root.toString() + "} \n"+
                "Duplicates={" + root.getDuplicates() +
                "}\n");
        if(this.childNodes.length!=0)
        	{for(Octree child : this.childNodes)
        	{	if(child ==null)
        			System.out.print("null");
        		        	}}
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }
    public Object getMinX() {
        return minX;
    }

    public void setMinX(Object minX) {
        this.minX = minX;
    }

    public Object getMinY() {
        return minY;
    }

    public void setMinY(Object minY) {
        this.minY = minY;
    }

    public Object getMinZ() {
        return minZ;
    }

    public void setMinZ(Object minZ) {
        this.minZ = minZ;
    }

    public Object getMaxX() {
        return maxX;
    }

    public void setMaxX(Object maxX) {
        this.maxX = maxX;
    }

    public Object getMaxY() {
        return maxY;
    }

    public void setMaxY(Object maxY) {
        this.maxY = maxY;
    }

    public Object getMaxZ() {
        return maxZ;
    }

    public void setMaxZ(Object maxZ) {
        this.maxZ = maxZ;
    }

    public String getCol1() {
        return col1;
    }

    public void setCol1(String col1) {
        this.col1 = col1;
    }

    public String getCol2() {
        return col2;
    }

    public void setCol2(String col2) {
        this.col2 = col2;
    }

    public String getCol3() {
        return col3;
    }

    public void setCol3(String col3) {
        this.col3 = col3;
    }

    public boolean isParent() {
        return parent;
    }

    public void setParent(boolean parent) {
        this.parent = parent;
    }

    public Octree[] getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(Octree[] childNodes) {
        this.childNodes = childNodes;
    }


}
