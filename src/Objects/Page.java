package Objects;


import Exceptions.DBAppException;

import java.text.SimpleDateFormat;
import java.util.Vector;
import java.util.*;
import java.io.*;
import java.io.Serializable;

public class Page implements Serializable {

    @Serial
    private static final long serialVersionUID = 180979049375953L;
    int MaxRows;
    Vector<Hashtable<String, Object>> Tuples;

    public Page() {

        Tuples = new Vector<Hashtable<String, Object>>();
        File configFile = new File("config/DBApp.config");
        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);
            MaxRows = Integer.parseInt(props.getProperty("MaximumRowsCountinTablePage"));

        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    public boolean isFull() {
        return this.Tuples.size() >= MaxRows;
    }

    public int getMaxRows() {
        return MaxRows;
    }

    public Vector<Hashtable<String, Object>> getTuples() {
        return Tuples;
    }

    public static int compareToObject(Object Value, Object pageValue) throws DBAppException {
        int r;
        if (Value instanceof Integer) {
            int a = (int) Value;
            int b = (int) Integer.parseInt("" + pageValue);
            r = Integer.compare(a, b);
        } else if (Value instanceof Double) {
            double a = (double) Value;
            double b = Double.parseDouble((String) pageValue);
            r = Double.compare(a, b);
        } else if (Value.getClass().equals("java.lang.String")) {
            r = ((String) Value).compareTo((String) pageValue);
        } else { //date
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            try {
                Date d1 = dateFormat.parse((String) Value);
                Date d2 = dateFormat.parse((String) pageValue);
                r = d1.compareTo(d2);
            } catch (Exception e) {
                throw new DBAppException("wrong date format");
            }
        }

        return r;
    } // Value>pageValue == positive

    public int binaryOnTuples(Hashtable<String, Object> table, String primaryKey) throws DBAppException {
        Object primary = table.get(primaryKey);
        int l = 0;
        int r = this.getTuples().size() - 1;
        while (r - l > 1) {
            int m = (l + r) / 2;
            // Check if x is present at mid
            if (compareToObject(primary, this.getTuples().get(m).get(primaryKey)) == 0)
                throw new DBAppException("tuple exists");

            // If x greater, ignore left half
            if (compareToObject(primary, this.getTuples().get(m).get(primaryKey)) > 0) l = m;

                // If x is smaller, ignore right half
            else r = m;
        }
        if (r == this.getTuples().size() - 1) {
            if (compareToObject(primary, this.getTuples().get(r).get(primaryKey)) == 0)
                throw new DBAppException("tuple exists");
            if (compareToObject(primary, this.getTuples().get(r).get(primaryKey)) > 0) r += 1;
        }
        if (l == 0) {
            if (compareToObject(primary, this.getTuples().get(l).get(primaryKey)) == 0)
                throw new DBAppException("tuple exists");
            if (compareToObject(primary, this.getTuples().get(l).get(primaryKey)) < 0) r -= 1;

        }
        if(r==-1) r=0;
        return r;
    }

    public void addRow(Hashtable<String, Object> table, String PrimaryKey) throws DBAppException {
        if (this.getTuples().size() == 0) {
            this.Tuples.add(table);
            return;
        }
        int i = binaryOnTuples(table, PrimaryKey);


        if (i >= this.getTuples().size())
            this.Tuples.add(table);
        else {
            this.Tuples.add(i, table);

        }


    }


    public void serializePage(String tableName, int index) throws IOException{

        FileOutputStream fileOut = new FileOutputStream("meta/" + tableName + index + ".class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        fileOut.close();
        out.close();

    }

//    public static void main(String[] args) throws DBAppException {
//        Page x = new Page();
//        Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//        htblColNameValue.put("Name", "Myriam");
//        htblColNameValue.put("ID", 111);
//
//        Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
//        htblColNameValue2.put("Name", "Ahmed");
//        htblColNameValue2.put("ID", 11);
//
//        Hashtable<String, Object> htblColNameValue44 = new Hashtable<String, Object>();
//        htblColNameValue44.put("Name", "rest");
//        htblColNameValue44.put("ID", 30);
//        Hashtable<String, Object> htblColNameValuelast = new Hashtable<String, Object>();
//        htblColNameValuelast.put("Name", "last");
//        htblColNameValuelast.put("ID", 800);
//        Hashtable<String, Object> htblColNameValue11 = new Hashtable<String, Object>();
//        htblColNameValue11.put("Name", "first");
//        htblColNameValue11.put("ID", 1);
//
//        Hashtable<String, Object> htblColNameValue1 = new Hashtable<String, Object>();
//        htblColNameValue1.put("Name", "Nada");
//        htblColNameValue1.put("ID", 21);
//        x.addRow(htblColNameValue1, "ID");x.addRow(htblColNameValue2, "ID");
//        x.addRow(htblColNameValue, "ID");x.addRow(htblColNameValue44,"ID");
//        x.addRow(htblColNameValue11,"ID");
//        x.addRow(htblColNameValuelast,"ID");
//        System.out.println(x.getTuples().get(1).get("ID"));
//    }

}
