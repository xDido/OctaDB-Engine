import Exceptions.DBAppException;
import Objects.*;
import Octree.*;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {


    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {


        if (arrSQLTerms.length - 1 != strarrOperators.length)
            throw new DBAppException("compilation error");

        String SingleTableName = arrSQLTerms[0]._strTableName;
        Vector<Hashtable<String, Object>> result = new Vector<>();
        ArrayList<String> colNames = new ArrayList<>();
        int i = 0;
        while (i < arrSQLTerms.length) {
            SQLTerm condition = arrSQLTerms[i];
            ArrayList<String[]> tablesName = loopCSV();
            boolean valid = false;
            //check if this table and this attribute exist and the entered value is valid for the col type
            for (String[] strings : tablesName) {
                if (strings[0].equals(SingleTableName) &&
                        strings[1].equals(condition._strColumnName) &&
                        checkStringType("" + condition._objValue).equals(strings[2])) {
                    valid = true;
                    break;
                }
            }
            if (valid)
                colNames.add(condition._strColumnName);

            else  //not valid
                throw new DBAppException("error while validating!!");
            i++;
        }
        String Index = getIndexName(colNames, SingleTableName);
        if (Index.equals("linear Search") || !allOperatorsAreAND(strarrOperators)) {
            result = executeComplexQuery(SingleTableName, arrSQLTerms, strarrOperators);
        } else {
            Octree tree = ReadIndex(Index, SingleTableName);
            ArrayList<Integer> reff = new ArrayList<>();
            for (SQLTerm arrSQLTerm : arrSQLTerms) {
                ArrayList<Integer> a = getPagesByRange(arrSQLTerm, tree, SingleTableName);
                for (Integer integer : a) {
                    if (!reff.contains(integer))
                        reff.add(integer);
                }
            }
            result = performQueryWithIndex(reff, SingleTableName, arrSQLTerms, strarrOperators);

        }


        return result.iterator();
    }
    public static boolean allOperatorsAreAND(String[] operators) {
        for (String operator : operators) {
            if (!operator.equals("AND"))
                return false;
        }
        return true;

    }
    public static ArrayList<Integer> getPagesByRange(SQLTerm sqlTerm, Octree tree, String strTableName) throws DBAppException {
        ArrayList<Integer> reff = new ArrayList<>();
        Point p = new Point(null, null, null, -1);
        if (tree.getCol1().equals(sqlTerm._strColumnName))
            p.setX(sqlTerm._objValue);
        else if (tree.getCol2().equals(sqlTerm._strColumnName))
            p.setY(sqlTerm._objValue);
        else if (tree.getCol3().equals(sqlTerm._strColumnName))
            p.setZ(sqlTerm._objValue);

        switch (sqlTerm._strOperator) {

            case ">":
                reff = performRangeQuery(">", p, tree);
                break;
            case ">=":
                reff = searchOctree(p, tree, reff);
                ArrayList<Integer> Tempreff = performRangeQuery(">", p, tree);
                assert Tempreff != null;
                for (Integer integer : Tempreff)
                    if (!reff.contains(integer))
                        reff.add(integer);
                break;
            case "<":
                reff = performRangeQuery("<", p, tree);
                break;
            case "<=":
                reff = searchOctree(p, tree, reff);
                ArrayList<Integer> Tempreff1 = performRangeQuery("<", p, tree);
                assert Tempreff1 != null;
                for (Integer integer : Tempreff1)
                    if (!reff.contains(integer))
                        reff.add(integer);
            case "!=":
                ArrayList<Integer> Tempreff2 = searchOctree(p, tree, reff);
                Table t = ReadTable(strTableName);
                for (int i = 0; i < t.noOfPages; i++)
                    if (!Tempreff2.contains(i))
                        reff.add(i);
                break;

            case "=":
                reff = searchOctree(p, tree, reff);
                break;

        }
        return reff;
    }
    public static ArrayList<Integer> performRangeQuery(String operator, Point point, Octree tree) throws DBAppException {
        if (operator.equals(">")) {
            if ((point.getX() != null && compareToObject(point.getX(), tree.getMaxX()) > 0)
                    || (point.getY() != null && compareToObject(point.getY(), tree.getMaxY()) > 0) ||
                    (point.getZ() != null && compareToObject(point.getZ(), tree.getMaxZ()) > 0)) {
                //skip this node , else
                return new ArrayList<>();
            }
            if (tree.getRoot() == null) return new ArrayList<>();
            if (tree.isParent()) {
                ArrayList<Integer> ref = new ArrayList<>();
                for (int i = 0; i < tree.getChildNodes().length; i++) ref.addAll(performRangeQuery(operator, point, tree.getChildNodes()[i]));
                return ref;
            }
            //leaf node and in the range
            ArrayList<Integer> ref = new ArrayList<>();
            for (int i = 0; i < tree.getRoot().getPoints().size(); i++)
                ref.add(tree.getRoot().getPoints().get(i).getRef());
            return ref;


        }
        if (operator.equals("<")) {
            if ((point.getX() != null && compareToObject(point.getX(), tree.getMinX()) < 0)
                    || (point.getY() != null && compareToObject(point.getY(), tree.getMinY()) < 0) ||
                    (point.getZ() != null && compareToObject(point.getZ(), tree.getMinZ()) < 0)) {
                //value not in range , skip this node , else
                return new ArrayList<>();
            }
            if (tree.getRoot() == null) return new ArrayList<>();
            if (tree.isParent()) {
                ArrayList<Integer> ref = new ArrayList<>();
                for (int i = 0; i < tree.getChildNodes().length; i++) ref.addAll(performRangeQuery(operator, point, tree.getChildNodes()[i]));
                return ref;
            }
            //leaf node and in the range
            ArrayList<Integer> ref = new ArrayList<>();
            for (int i = 0; i < tree.getRoot().getPoints().size(); i++)
                ref.add(tree.getRoot().getPoints().get(i).getRef());
            return ref;

        }
        return null;
    }
    public static Vector<Hashtable<String, Object>> performQueryWithIndex(ArrayList<Integer> pageIndices, String strTableName, SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        if (strarrOperators.length == 0) {
            SQLTerm term = arrSQLTerms[0];
            return getTuplesFromIndex(pageIndices, strTableName, term._strColumnName, term._strOperator, term._objValue);
        }
        SQLTerm term = arrSQLTerms[0];
        String operator = strarrOperators[0];
        SQLTerm[] new_arrSQLTerms = new SQLTerm[arrSQLTerms.length - 1];
        String[] new_strarrOperators = new String[strarrOperators.length - 1];
        /*
        for (int i = 1; i < arrSQLTerms.length; i++) {
                    new_arrSQLTerms[i - 1] = arrSQLTerms[i];
                }
        */
        System.arraycopy(arrSQLTerms, 1, new_arrSQLTerms, 0, arrSQLTerms.length - 1);
        /*
        for (int i = 1; i < strarrOperators.length; i++) {
            new_strarrOperators[i - 1] = strarrOperators[i];
        }
*/
        System.arraycopy(strarrOperators, 1, new_strarrOperators, 0, strarrOperators.length - 1);
        Vector<Hashtable<String, Object>> semiResult = getTuplesFromIndex(pageIndices, strTableName, term._strColumnName, term._strOperator, term._objValue);
        return switch (operator) {
            case "AND" -> intersection(semiResult, executeComplexQuery(strTableName, new_arrSQLTerms, new_strarrOperators));
            case "OR" -> union(semiResult, executeComplexQuery(strTableName, new_arrSQLTerms, new_strarrOperators));
            case "XOR" -> sub(union(semiResult, executeComplexQuery(strTableName, new_arrSQLTerms, new_strarrOperators)), intersection(semiResult, executeComplexQuery(strTableName, new_arrSQLTerms, new_strarrOperators)));
            default -> throw new DBAppException("undefined operation");
        };


    }
    public static Vector<Hashtable<String, Object>> executeComplexQuery(String strTableName, SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {

        if (strarrOperators.length == 0) {
            SQLTerm term = arrSQLTerms[0];
            return getTuples(strTableName, term._strColumnName, term._strOperator, term._objValue);
        }
        SQLTerm term = arrSQLTerms[0];
        String operator = strarrOperators[0];
        SQLTerm[] new_arrSQLTerms = new SQLTerm[arrSQLTerms.length - 1];
        String[] new_strarrOperators = new String[strarrOperators.length - 1];
//        for (int i = 1; i < arrSQLTerms.length; i++) {
//            new_arrSQLTerms[i - 1] = arrSQLTerms[i];
//        }
        System.arraycopy(arrSQLTerms, 1, new_arrSQLTerms, 0, arrSQLTerms.length - 1);
//        for (int i = 1; i < strarrOperators.length; i++) {
//            new_strarrOperators[i - 1] = strarrOperators[i];
//        }
        System.arraycopy(strarrOperators, 1, new_strarrOperators, 0, strarrOperators.length - 1);
        Vector<Hashtable<String, Object>> semiResult = getTuples(strTableName, term._strColumnName, term._strOperator, term._objValue);
        return switch (operator) {
            case "AND" -> intersection(semiResult, executeComplexQuery(strTableName, new_arrSQLTerms, new_strarrOperators));
            case "OR" -> union(semiResult, executeComplexQuery(strTableName, new_arrSQLTerms, new_strarrOperators));
            case "XOR" ->
                    sub(union(semiResult, executeComplexQuery(strTableName, new_arrSQLTerms, new_strarrOperators)), intersection(semiResult, executeComplexQuery(strTableName, new_arrSQLTerms, new_strarrOperators)));
            default -> throw new DBAppException("undefined operation");
        };

    }
    public static boolean doOperation(Object tupleValue, String operator, Object SQLTermValue) throws DBAppException {

        int i = compareToObject(tupleValue, SQLTermValue);
        switch (operator) {
            case ">" -> {
                return i > 0;
            }
            case ">=" -> {
                return i >= 0;
            }
            case "<" -> {
                return i < 0;
            }
            case "<=" -> {
                return i <= 0;
            }
            case "!=" -> {
                return i != 0;
            }
            case "=" -> {
                return i == 0;
            }
        }
        throw new DBAppException("invalid operator");

    }
    public static Vector<Hashtable<String, Object>> getTuplesFromIndex(ArrayList<Integer> pageIndices, String strTableName, String key, String operator, Object value) throws DBAppException {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        for (Integer pageIndex : pageIndices) {
            Page p = ReadPage(pageIndex, strTableName);
            for (int j = 0; j < p.getTuples().size(); j++) {
                if (doOperation(p.getTuples().get(j).get(key), operator, value))
                    result.add(p.getTuples().get(j));
            }

        }
        return result;

    }
    public static Vector<Hashtable<String, Object>> getTuples(String strTableName, String key, String operator, Object value) throws DBAppException {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        Table table = ReadTable(strTableName);
        int q = table.noOfPages;
        for (int i = 0; i < q; i++) {   //i is the page id
            Page p = ReadPage(i, strTableName);   //deserialize the page
            for (int j = 0; j < p.getTuples().size(); j++) {  //j is the id of the tuple
                if (doOperation(p.getTuples().get(j).get(key), operator, value))
                    result.add(p.getTuples().get(j));
            }
            p = null;
            System.gc();
        }
        table = null;
        System.gc();

        return result;
    }
    public static Vector<Hashtable<String, Object>> sub(Vector<Hashtable<String, Object>> a, Vector<Hashtable<String, Object>> b) {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        for (Hashtable<String, Object> t : a) {
            if (!b.contains(t))
                result.add(t);
        }
        return result;
    }
    public static Vector<Hashtable<String, Object>> intersection(Vector<Hashtable<String, Object>> a, Vector<Hashtable<String, Object>> b) {
        Vector<Hashtable<String, Object>> result = new Vector<>();
        for (Hashtable<String, Object> t : a) {
            if (b.contains(t))
                result.add(t);
        }
        return result;
    }
    public static Vector<Hashtable<String, Object>> union(Vector<Hashtable<String, Object>> a, Vector<Hashtable<String, Object>> b) {
        Vector<Hashtable<String, Object>> result = new Vector<>(a);

        for (Hashtable<String, Object> t : b)
            if (!result.contains(t))
                result.add(t);

        return result;
    }
    public static String getIndexName(ArrayList<String> colNames, String strTableName) throws DBAppException {
        Table table = ReadTable(strTableName);
        for (int i = 0; i < table.Indices.size(); i++) {
            Octree tree = ReadIndex(table.Indices.get(i), strTableName);
            if (colNames.contains(tree.getCol1()) && colNames.contains(tree.getCol2()) && colNames.contains(tree.getCol3()))
                return table.Indices.get(i);
        }
        return "linear Search";
    }
    public static boolean isClusterIndexed(String strClusteringKey, String strTableName) throws DBAppException {
        Table table = ReadTable(strTableName);
        for (int i = 0; i < table.Indices.size(); i++) {
            Octree tree = ReadIndex(table.Indices.get(i), strTableName);
            if (strClusteringKey.equals(tree.getCol1()) || strClusteringKey.equals(tree.getCol2()) || strClusteringKey.equals(tree.getCol3()))
                return true;
        }
        return false;
    }
    public static void handleOverFlow(int pageIndex, Table table, String primaryKey) throws Exception {
        for (int i = pageIndex; i < table.noOfPages; i++) {
            Page p = ReadPage(i, table.getTableName());
            Hashtable<String, Object> tuple = p.getTuples().get(p.getMaxRows());
            p.getTuples().remove(p.getMaxRows());
            WritePage(i, table.getTableName(), p);
            p = null;
            if (i == table.noOfPages - 1) {
                table.addPages(table.getTableName());
            }
            Page p1;
            p1 = ReadPage(i + 1, table.getTableName());
            p1.addRow(tuple, primaryKey);

            for (int j = 0; j < table.Indices.size(); j++) {
                Octree tree = ReadIndex(table.Indices.get(j), table.getTableName());
                Point point = new Point(tuple.get(tree.getCol1()), tuple.get(tree.getCol2()), tuple.get(tree.getCol3()), i);
                updatePointRef(point, tree, i + 1, table.Indices.get(j), table.getTableName());
            }
            WritePage(i + 1, table.getTableName(), p1);
            if (p1.getTuples().size() <= p1.getMaxRows()) break;
            p1 = null;
        }

    }
    public static int compareToObject(Object Value, Object pageValue) throws DBAppException {
        int r;
        if (Value instanceof Integer) {
            int a = (int) Value;
            int b = Integer.parseInt(String.valueOf(pageValue));
            r = Integer.compare(a, b);
        } else if (Value instanceof Double) {
            double a = (double) Value;
            double b = Double.parseDouble("" + pageValue);
            r = Double.compare(a, b);
        } else if (Value instanceof String) {
            r = ((String) Value).compareTo("" + pageValue);
        } else { //date
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            try {
                Date d1 = (Date) Value;
                Date d2 = dateFormat.parse((String) pageValue);
                r = d1.compareTo(d2);
            } catch (Exception e) {
                e.printStackTrace();
                throw new DBAppException("wrong date format");
            }
        }
        return r;
    }
    private static void Writecsv(String strTableName, Hashtable<String, String> htblColNameType, String strClusteringKeyColumn, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
        boolean flag = false;
        try {
            String line;
            BufferedReader br = new BufferedReader(new FileReader("meta/Metadata.csv"));
            ArrayList<String[]> TablesName = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] r = line.split(",");
                TablesName.add(r);
            }
            for (String[] strings : TablesName) {
                if (strings[0].equals(strTableName)) {
                    flag = true;
                    break;
                }
            }

        } catch (Exception e) {
            throw new DBAppException(e);
        }
        if (flag) {
            throw new DBAppException("this Table name is used ");
        } else {
            try {
                FileWriter fileWriter = new FileWriter("meta/Metadata.csv", true);
                Enumeration<String> ColNames = htblColNameType.keys();
                while (ColNames.hasMoreElements()) {
                    String ColName = ColNames.nextElement();
                    fileWriter.append(strTableName).append(",");
                    fileWriter.append(ColName).append(",");

                    switch (htblColNameType.get(ColName)) {
                        case "java.lang.Integer", "java.lang.String", "java.lang.Double", "java.util.Date" ->
                                fileWriter.append(htblColNameType.get(ColName)).append(",");
                        default -> throw new DBAppException("Wrong Format");
                    }
                    if (ColName.equals(strClusteringKeyColumn)) fileWriter.append("True" + ",");
                    else fileWriter.append("False" + ",");
                    fileWriter.append("null" + ","); // Index Name
                    fileWriter.append("null" + ","); // Index Type
                    if (htblColNameType.get(ColName).equals("java.util.Date")) {
                        try {
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                            simpleDateFormat.parse(htblColNameMin.get(ColName));
                            simpleDateFormat.parse(htblColNameMax.get(ColName));
                        } catch (Exception e) {
                            throw new DBAppException("Wrong date format!");
                        }
                    }
                    fileWriter.append(htblColNameMin.get(ColName)).append(","); //
                    fileWriter.append(htblColNameMax.get(ColName));
                    fileWriter.append('\n');
                }
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new DBAppException("can not read the csv file");
            }

        }
    }
    public static ArrayList<String[]> loopCSV() throws DBAppException {
        try {
            String line;
            BufferedReader br = new BufferedReader(new FileReader("meta/Metadata.csv"));
            ArrayList<String[]> TablesName = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] r = line.split(",");
                TablesName.add(r);
            }

            return TablesName;
        } catch (Exception e) {
            throw new DBAppException("can not read the csv file");
        }
    }
    public static Table ReadTable(String tableName) throws DBAppException {// 000000000000
        try {
            FileInputStream fileInputStream = new FileInputStream("meta/" + tableName + ".class");
            ObjectInputStream ObjectInputStream = new ObjectInputStream(fileInputStream);
            Table Table = (Table) ObjectInputStream.readObject();
            fileInputStream.close();
            ObjectInputStream.close();
            return Table;
        } catch (Exception e) {
            throw new DBAppException("unable to deserialize , file not found ");
        }
    }
    public static void WriteTable(String tableName, Table t) throws DBAppException {
        try {
            File file = new File("meta/" + tableName + ".class");
            if (!file.exists()) file.createNewFile();

            FileOutputStream fileOutputStream = new FileOutputStream("meta/" + tableName + ".class");
            ObjectOutputStream ObjectOutputStream = new ObjectOutputStream(fileOutputStream);
            ObjectOutputStream.writeObject(t);
            ObjectOutputStream.close();
            fileOutputStream.close();
        } catch (Exception e) {
            throw new DBAppException("unable to serialize , file not found ");
        }
    }
    public static Page ReadPage(int id, String strTableName) throws DBAppException {// deserialize
        try {
            FileInputStream fileInputStream = new FileInputStream("meta/" + strTableName + id + ".class");
            ObjectInputStream ObjectInputStream = new ObjectInputStream(fileInputStream);
            Page page = (Page) ObjectInputStream.readObject();
            fileInputStream.close();
            ObjectInputStream.close();
            return page;
        } catch (Exception e) {
            throw new DBAppException("unable to deserialize , file not found ");
        }
    }
    public static void WritePage(int id, String strTableName, Page page) throws DBAppException { // serialize
        try {
            FileOutputStream fileOutputStream = new FileOutputStream("meta/" + strTableName + id + ".class");
            ObjectOutputStream ObjectOutputStream = new ObjectOutputStream(fileOutputStream);
            ObjectOutputStream.writeObject(page);
            ObjectOutputStream.close();
            fileOutputStream.close();
        } catch (Exception e) {
            throw new DBAppException("unable to serialize , file not found ");
        }
    }
    private static String checkStringType(String strClusteringKeyValue) {
        String r = "java.lang.String";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            // This will throw an exception if the date string is not in the correct format
            dateFormat.parse(strClusteringKeyValue);
            r = "java.lang.Date";
        } catch (Exception e) {
            try {
                // This will throw an exception if the date string is not in the correct format
                Integer.parseInt(strClusteringKeyValue);
                r = "java.lang.Integer";
            } catch (Exception e1) {
                try {
                    // This will throw an exception if the date string is not in the correct format
                    Double.parseDouble(strClusteringKeyValue);
                    r = "java.lang.Double";
                } catch (Exception e2) {
                }
            }
        }


        return r;
    }
    private static void checkDateFormat(String Date) throws DBAppException {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setLenient(false);
            // This will throw an exception if the date string is not in the correct format
            dateFormat.parse(Date);
        } catch (Exception e) {
            throw new DBAppException("The date is not in the correct format");
        }
    }
    private static String getPrimKeyType(String strTableName) throws DBAppException {
        //retrieve the primary key type and return it as a String ex: "int","String", "Double" and "Date"
        String type = "default";
        ArrayList<String[]> TablesName = loopCSV();
        for (String[] line : TablesName) {
            if (line[0].equals(strTableName)) {
                if (line[3].equalsIgnoreCase("True")) type = line[2];
            }
        }
        return type;
    }
    private static String getPrimKey(String strTableName) throws DBAppException {
        String prim = "";
        ArrayList<String[]> TablesName = loopCSV();
        for (String[] line : TablesName) {
            if (line[0].equals(strTableName)) {
                if (line[3].equalsIgnoreCase("True")) prim = line[1];
            }
        }
        return prim;
    }
    private static void checkColumns(ArrayList<String[]> tablesName, String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        ArrayList<String> columns = new ArrayList<>();
        for (String[] strings : tablesName) {
            if ((strings[0]).equals(strTableName)) columns.add(strings[1]);
        }
        Set<String> set = htblColNameValue.keySet();
        for (String s : set) {
            if (!columns.contains(s)) {
                throw new DBAppException("Trying to Update a column that doesn't exist in the table");
            }
        }
    }
    private static Object convertToDataType(String value) throws DBAppException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setLenient(false);
        Object obj;
        String type = checkStringType(value);
        switch (type) {
            case "java.lang.Integer" -> obj = Integer.parseInt(value);
            case "java.lang.Double" -> obj = Double.parseDouble(value);
            case "java.util.Date" -> {
                try {
                    obj = dateFormat.parse(value);
                } catch (ParseException e) {

                    throw new DBAppException(e);
                }
            }
            default -> obj = value;
        }
        return obj;
    }
    public static void insertIntoIndex(String strTableName, String indexName) throws DBAppException {

        //scan the table and add the values to the Octree.Octree
        Table table = ReadTable(strTableName);
        Octree tree = ReadIndex(indexName, strTableName);

        //loop on all the tuples in the table and insert them in the octree
        for (int i = 0; i < table.noOfPages; i++) {
            Page p = ReadPage(i, strTableName);
            for (int j = 0; j < p.getTuples().size(); j++) {
                Hashtable<String, Object> tuple = p.getTuples().get(j);
                Object x = tuple.get(tree.getCol1());
                Object y = tuple.get(tree.getCol2());
                Object z = tuple.get(tree.getCol3());
                //parse the tree
                tree = parseOctree(x, y, z, tree, i);
                WriteIndex(indexName, strTableName, tree);
            }
        }
        // WriteIndex(indexName, tree);

    }
    public static int determineOctreeIndex(Object x, Object y, Object z, Object midX, Object midY, Object midZ) throws DBAppException {
        int i;

        if (compareToObject(x, midX) < 0) {
            if (compareToObject(y, midY) < 0) {
                if (compareToObject(z, midZ) < 0) {
                    i = 0;
                } else i = 1;
            } else {
                if (compareToObject(z, midZ) < 0) {
                    i = 2;
                } else i = 3;
            }
        } else {
            if (compareToObject(y, midY) < 0) {
                if (compareToObject(z, midZ) < 0) {
                    i = 4;
                } else i = 5;
            } else {
                if (compareToObject(z, midZ) < 0) {
                    i = 6;
                } else i = 7;
            }
        }
        return i;
    }
    public static Object findMedianValue(Object min, Object max) throws DBAppException {
        Object Result = null;
        Object minValue = convertToDataType("" + min);
        Object maxValue = convertToDataType("" + max);
        if (maxValue instanceof String && minValue instanceof String) {
             // how to check string
            String s = (String) minValue;
            s.toLowerCase();
            String t = (String) maxValue;
            t.toLowerCase();
            if (s.length() < t.length()) t = t.substring(0, s.length());
            else s = s.substring(0, t.length());
            int N = s.length();

            int[] a1 = new int[N + 1];

            for (int i = 0; i < N; i++) {
                a1[i + 1] = (int) s.charAt(i) - 97 + (int) t.charAt(i) - 97;
            }

            // Iterate from right to left
            // and add carry to next position
            for (int i = N; i >= 1; i--) {
                a1[i - 1] += (int) a1[i] / 26;
                a1[i] %= 26;
            }

            // Reduce the number to find the middle
            // string by dividing each position by 2
            for (int i = 0; i <= N; i++) {

                // If current value is odd,
                // carry 26 to the next index value
                if ((a1[i] & 1) != 0) {

                    if (i + 1 <= N) {
                        a1[i + 1] += 26;
                    }
                }
                a1[i] = (int) a1[i] / 2;
            }
            StringBuilder st = new StringBuilder();
            for (int i = 1; i <= N; i++) {
                st.append((char) (a1[i] + 97));
            }
            Result = (String) st.toString();

        } else if (maxValue instanceof Date && minValue instanceof Date) {
            Result = new Date((((Date) minValue).getTime() + ((Date) maxValue).getTime()) / 2);
        } else if (maxValue instanceof Integer && minValue instanceof Integer) {
            Result = (Integer) (((Integer) minValue + (Integer) maxValue) / 2);
        } else {
            Result = (Double) (((Double) minValue + (Double) maxValue) / 2);
        }
        return Result;
    }
    public static Object[] findMinandMax(Octree tree, int i) throws DBAppException {
        // r = [minX,minY,minZ,maxX,maxY,maxZ]
        Object[] r = new Object[6];
        Object midX = findMedianValue(tree.getMinX(), tree.getMaxX());
        Object midY = findMedianValue(tree.getMinY(), tree.getMaxY());
        Object midZ = findMedianValue(tree.getMinZ(), tree.getMaxZ());

        switch (i) {
            case 0:
                r[0] = tree.getMinX();
                r[1] = tree.getMinY();
                r[2] = tree.getMinZ();
                r[3] = midX;
                r[4] = midY;
                r[5] = midZ;
                break;
            case 1:
                r[0] = tree.getMinX();
                r[1] = tree.getMinY();
                r[2] = midZ;
                r[3] = midX;
                r[4] = midY;
                r[5] = tree.getMaxZ();
                break;
            case 2:
                r[0] = tree.getMinX();
                r[1] = midY;
                r[2] = tree.getMinZ();
                r[3] = midX;
                r[4] = tree.getMaxY();
                r[5] = midZ;
                break;
            case 3:
                r[0] = tree.getMinX();
                r[1] = midY;
                r[2] = midZ;
                r[3] = midX;
                r[4] = tree.getMaxY();
                r[5] = tree.getMaxZ();
                break;
            case 4:
                r[0] = midX;
                r[1] = tree.getMinY();
                r[2] = tree.getMinZ();
                r[3] = tree.getMaxX();
                r[4] = midY;
                r[5] = midZ;
                break;
            case 5:
                r[0] = midX;
                r[1] = tree.getMinY();
                r[2] = midZ;
                r[3] = tree.getMaxX();
                r[4] = midY;
                r[5] = tree.getMaxZ();
                break;
            case 6:
                r[0] = midX;
                r[1] = midY;
                r[2] = tree.getMinZ();
                r[3] = tree.getMaxX();
                r[4] = tree.getMaxY();
                r[5] = midZ;
                break;
            case 7:
                r[0] = midX;
                r[1] = midY;
                r[2] = midZ;
                r[3] = tree.getMaxX();
                r[4] = tree.getMaxY();
                r[5] = tree.getMaxZ();
                break;

        }
        return r;

    }
    public static boolean checkDup(Point p, Node r) throws DBAppException {
        Object x = p.getX();
        Object y = p.getY();
        Object z = p.getZ();
        for (int i = 0; i < r.getPoints().size(); i++) {

            if (compareToObject(r.getPoints().get(i).getX(), x) == 0 && compareToObject(r.getPoints().get(i).getY(), y) == 0 && compareToObject(r.getPoints().get(i).getZ(), z) == 0)
                return true;

        }
        return false;

    }
    public static ArrayList<Integer> findIndexNull(Object x, Object y, Object z, Object midX, Object midY, Object midZ) throws DBAppException {
        ArrayList<Integer> index = new ArrayList<>();
        if (x == null && y != null && z != null) {
            if (compareToObject(y, midY) < 0) {
                if (compareToObject(z, midZ) < 0) {
                    //y-->minmid z-->minmid
                    index.add(0);
                    index.add(4);
                } else {//y-->minmid z-->midmax
                    index.add(1);
                    index.add(5);
                }
            } else {
                if (compareToObject(z, midZ) < 0) {
                    //y-->midmax z-->minmid
                    index.add(2);
                    index.add(6);
                } else { //y-->midmax z-->midmax
                    index.add(3);
                    index.add(7);
                }
            }
        } else if (y == null && x != null && z != null) {
            if (compareToObject(x, midX) < 0) {
                if (compareToObject(z, midZ) < 0) {
                    //x-->minmid z-->minmid
                    index.add(0);
                    index.add(2);
                } else {//x-->minmid z-->midmax
                    index.add(1);
                    index.add(3);
                }
            } else {
                if (compareToObject(z, midZ) < 0) {
                    //x-->midmax z-->minmid
                    index.add(4);
                    index.add(6);
                } else { //x-->midmax z-->midmax
                    index.add(5);
                    index.add(7);
                }
            }
        } else if (z == null && y != null && x != null) {
            if (compareToObject(x, midX) < 0) {
                if (compareToObject(y, midY) < 0) {
                    //x-->minmid y-->minmid
                    index.add(0);
                    index.add(1);
                } else {//x-->minmid y-->midmax
                    index.add(2);
                    index.add(3);
                }
            } else {
                if (compareToObject(y, midY) < 0) {
                    //x-->midmax y-->minmid
                    index.add(4);
                    index.add(5);
                } else { //x-->midmax y-->midmax
                    index.add(6);
                    index.add(7);
                }
            }

        } else if (x != null && y == null && z == null) {
            if (compareToObject(x, midX) < 0) {
                // x--> minmid
                index.add(0);
                index.add(1);
                index.add(2);
                index.add(3);
            } else {
                //x-->midmax
                index.add(4);
                index.add(5);
                index.add(6);
                index.add(7);
            }


        } else if (y != null && x == null && z == null) {
            if (compareToObject(y, midY) < 0) {
                // y--> minmid
                index.add(0);
                index.add(1);
                index.add(4);
                index.add(5);
            } else {
                //y-->midmax
                index.add(2);
                index.add(3);
                index.add(6);
                index.add(7);
            }

        } else if (z != null && y == null && x == null) {
            if (compareToObject(z, midZ) < 0) {
                // z--> minmid
                index.add(0);
                index.add(2);
                index.add(4);
                index.add(6);
            } else {
                //z-->midmax
                index.add(1);
                index.add(3);
                index.add(5);
                index.add(7);
            }

        } else if (x == null && y == null && z == null) {
            throw new DBAppException("All dimensions are null");
        } else if (x != null && y != null && z != null) {
            index.add(determineOctreeIndex(x, y, z, midX, midY, midZ));
        }
        return index;

    }
    public static ArrayList<Integer> searchOctree(Point p, Octree octree, ArrayList<Integer> reff) throws DBAppException {
        if (octree.getRoot() == null || (octree.getRoot().getPoints().size() == 0 && !octree.isParent())) {
            return reff; // Return empty result if the leaf is empty
        } else if (octree.isParent()) { // Parent node
            ArrayList<Integer> childIndices = findIndexNull(p.getX(), p.getY(), p.getZ(), findMedianValue(octree.getMinX(), octree.getMaxX()), findMedianValue(octree.getMinY(), octree.getMaxY()), findMedianValue(octree.getMinZ(), octree.getMaxZ()));
            for (int i = 0; i < childIndices.size(); i++) {
                reff = searchOctree(p, octree.getChildNodes()[childIndices.get(i)], reff); // Accumulate results from child nodes
            }
        } else { // Leaf node
            for (int i = 0; i < octree.getRoot().getPoints().size(); i++) {
                Point point = octree.getRoot().getPoints().get(i);
                if (p.getX() == null || point.getX().equals(p.getX())) {
                    if (p.getY() == null || point.getY().equals(p.getY())) {
                        if (p.getZ() == null || point.getZ().equals(p.getZ())) {
                            reff.add(point.getRef()); // Add matching point's reference to the results
                        }
                    }
                }
            }
            for (int i = 0; i < octree.getRoot().getDuplicates().size(); i++) {
                Point duplicatePoint = octree.getRoot().getDuplicates().get(i);
                if (p.getX() == null || duplicatePoint.getX().equals(p.getX())) {
                    if (p.getY() == null || duplicatePoint.getY().equals(p.getY())) {
                        if (p.getZ() == null || duplicatePoint.getZ().equals(p.getZ())) {
                            reff.add(duplicatePoint.getRef()); // Add matching duplicate point's reference to the results
                        }
                    }
                }
            }
        }
        return reff;
    }
    public static Octree deleteOctree(Point p, Octree octree) throws DBAppException {


        if (octree.getRoot() == null || (octree.getRoot().getPoints().size() == 0 && !octree.isParent())) return octree;
        else if (octree.isParent()) {
            ArrayList<Integer> childsIndext = findIndexNull(p.getX(), p.getY(), p.getZ(), findMedianValue(octree.getMinX(), octree.getMaxX()), findMedianValue(octree.getMinY(), octree.getMaxY()), findMedianValue(octree.getMinZ(), octree.getMaxZ()));
            parent:
            for (int i = 0; i < childsIndext.size(); i++) {
                octree.getChildNodes()[childsIndext.get(i)] = deleteOctree(p, octree.getChildNodes()[childsIndext.get(i)]);
            }
        } else {
            leaf:
            for (int i = 0; i < octree.getRoot().getPoints().size(); i++) {
                if (p.getX() == null) {
                    if (p.getY() == null) {
                        if (p.getZ() != null) {
                            if (octree.getRoot().getPoints().get(i).getZ().equals(p.getZ())
                                    && p.getRef() == octree.getRoot().getPoints().get(i).getRef()) {
                                ArrayList<Integer> ref = new ArrayList<>();
                                ref = searchOctree(p, octree, ref);
                                Point deleted = octree.getRoot().getPoints().remove(i);
                                if (ref.size() > 1) {
                                    for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                        if (octree.getRoot().getDuplicates().get(j).getZ().equals(deleted.getZ()) &&
                                                octree.getRoot().getDuplicates().get(j).getX().equals(deleted.getX())
                                                && octree.getRoot().getDuplicates().get(j).getY().equals(deleted.getY())) {
                                            Point pFromDup = octree.getRoot().getDuplicates().remove(j);
//                                			octree= parseOctree(pFromDup.getX(),pFromDup.getY(),pFromDup.getZ(),octree,pFromDup.getRef());
                                            octree.getRoot().getPoints().add(pFromDup);
                                            break;
                                        }
                                    }
                                }
                                return octree;
                            }
                            if (octree.getRoot().getPoints().get(i).getZ().equals(p.getZ())
                                    && p.getRef() != octree.getRoot().getPoints().get(i).getRef()) {

                                for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                    if (octree.getRoot().getDuplicates().get(j).getZ().equals(p.getZ())
                                            && p.getRef() == octree.getRoot().getDuplicates().get(j).getRef()) {
                                        octree.getRoot().getDuplicates().remove(j);
                                        break;
                                    }

                                }

                            }
                        }
                    } else if (p.getZ() == null) {
                        if (octree.getRoot().getPoints().get(i).getY().equals(p.getY())
                                && p.getRef() == octree.getRoot().getPoints().get(i).getRef()) {
                            ArrayList<Integer> ref = new ArrayList<>();
                            ref = searchOctree(p, octree, ref);
                            Point deleted = octree.getRoot().getPoints().remove(i);
                            if (ref.size() > 1) {
                                for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                    if (octree.getRoot().getDuplicates().get(j).getZ().equals(deleted.getZ()) &&
                                            octree.getRoot().getDuplicates().get(j).getX().equals(deleted.getX())
                                            && octree.getRoot().getDuplicates().get(j).getY().equals(deleted.getY())) {
                                        Point pFromDup = octree.getRoot().getDuplicates().remove(j);
//                            			octree= parseOctree(pFromDup.getX(),pFromDup.getY(),pFromDup.getZ(),octree,pFromDup.getRef());
                                        octree.getRoot().getPoints().add(pFromDup);
                                        break;
                                    }
                                }
                            }
                            return octree;
                        } else if (octree.getRoot().getPoints().get(i).getY().equals(p.getY())
                                && p.getRef() != octree.getRoot().getPoints().get(i).getRef()) {

                            for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                if (octree.getRoot().getDuplicates().get(j).getY().equals(p.getY())
                                        && p.getRef() == octree.getRoot().getDuplicates().get(j).getRef()) {
                                    octree.getRoot().getDuplicates().remove(j);
                                    break;
                                }


                            }

                        }
                    } else {
                        if (octree.getRoot().getPoints().get(i).getY().equals(p.getY())
                                && octree.getRoot().getPoints().get(i).getZ().equals(p.getZ())
                                && p.getRef() == octree.getRoot().getPoints().get(i).getRef()) {
                            ArrayList<Integer> ref = new ArrayList<>();
                            ref = searchOctree(p, octree, ref);
                            Point deleted = octree.getRoot().getPoints().remove(i);
                            if (ref.size() > 1) {
                                for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                    if (octree.getRoot().getDuplicates().get(j).getZ().equals(deleted.getZ()) &&
                                            octree.getRoot().getDuplicates().get(j).getX().equals(deleted.getX())
                                            && octree.getRoot().getDuplicates().get(j).getY().equals(deleted.getY())) {
                                        Point pFromDup = octree.getRoot().getDuplicates().remove(j);
//                            			octree= parseOctree(pFromDup.getX(),pFromDup.getY(),pFromDup.getZ(),octree,pFromDup.getRef());
                                        octree.getRoot().getPoints().add(pFromDup);
                                        break;
                                    }
                                }
                            }
                            return octree;
                        } else if (octree.getRoot().getPoints().get(i).getY().equals(p.getY())
                                && octree.getRoot().getPoints().get(i).getZ().equals(p.getZ())
                                && p.getRef() != octree.getRoot().getPoints().get(i).getRef()) {

                            for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                if (octree.getRoot().getDuplicates().get(j).getY().equals(p.getY())
                                        && octree.getRoot().getDuplicates().get(j).getZ().equals(p.getZ())
                                        && p.getRef() == octree.getRoot().getDuplicates().get(j).getRef()) {
                                    octree.getRoot().getDuplicates().remove(j);
                                    break;
                                }


                            }

                        }
                    }

                }
                if (p.getY() == null) {
                    if (p.getZ() == null) {
                        if (octree.getRoot().getPoints().get(i).getX().equals(p.getX())
                                && p.getRef() == octree.getRoot().getPoints().get(i).getRef()) {
                            ArrayList<Integer> ref = new ArrayList<>();
                            ref = searchOctree(p, octree, ref);
                            Point deleted = octree.getRoot().getPoints().remove(i);
                            if (ref.size() > 1) {
                                for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                    if (octree.getRoot().getDuplicates().get(j).getZ().equals(deleted.getZ()) &&
                                            octree.getRoot().getDuplicates().get(j).getX().equals(deleted.getX())
                                            && octree.getRoot().getDuplicates().get(j).getY().equals(deleted.getY())) {
                                        Point pFromDup = octree.getRoot().getDuplicates().remove(j);
//                            			octree= parseOctree(pFromDup.getX(),pFromDup.getY(),pFromDup.getZ(),octree,pFromDup.getRef());
                                        octree.getRoot().getPoints().add(pFromDup);
                                        break;
                                    }
                                }
                            }
                            return octree;
                        } else if (octree.getRoot().getPoints().get(i).getX().equals(p.getX())
                                && p.getRef() != octree.getRoot().getPoints().get(i).getRef()) {

                            for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                if (octree.getRoot().getDuplicates().get(j).getX().equals(p.getX())
                                        && p.getRef() == octree.getRoot().getDuplicates().get(j).getRef()) {
                                    octree.getRoot().getDuplicates().remove(j);
                                    break;
                                }


                            }

                        }
                    } else {
                        if (octree.getRoot().getPoints().get(i).getX().equals(p.getX())
                                && octree.getRoot().getPoints().get(i).getZ().equals(p.getZ())
                                && p.getRef() == octree.getRoot().getPoints().get(i).getRef()) {
                            ArrayList<Integer> ref = new ArrayList<>();
                            ref = searchOctree(p, octree, ref);
                            Point deleted = octree.getRoot().getPoints().remove(i);
                            if (ref.size() > 1) {
                                for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                    if (octree.getRoot().getDuplicates().get(j).getZ().equals(deleted.getZ()) &&
                                            octree.getRoot().getDuplicates().get(j).getX().equals(deleted.getX())
                                            && octree.getRoot().getDuplicates().get(j).getY().equals(deleted.getY())) {
                                        Point pFromDup = octree.getRoot().getDuplicates().remove(j);
//                            			octree= parseOctree(pFromDup.getX(),pFromDup.getY(),pFromDup.getZ(),octree,pFromDup.getRef());
                                        octree.getRoot().getPoints().add(pFromDup);
                                        break;
                                    }
                                }
                            }
                            return octree;
                        } else if (octree.getRoot().getPoints().get(i).getX().equals(p.getX())
                                && octree.getRoot().getPoints().get(i).getZ().equals(p.getZ())
                                && p.getRef() != octree.getRoot().getPoints().get(i).getRef()) {

                            for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                if (octree.getRoot().getDuplicates().get(j).getX().equals(p.getX())
                                        && octree.getRoot().getDuplicates().get(j).getZ().equals(p.getZ())
                                        && p.getRef() == octree.getRoot().getDuplicates().get(j).getRef()) {
                                    octree.getRoot().getDuplicates().remove(j);
                                    break;
                                }

                            }

                        }
                    }
                }
                if (p.getZ() == null) {
                    if (octree.getRoot().getPoints().get(i).getX().equals(p.getX())
                            && octree.getRoot().getPoints().get(i).getY().equals(p.getY())
                            && p.getRef() == octree.getRoot().getPoints().get(i).getRef()) {
                        ArrayList<Integer> ref = new ArrayList<>();
                        ref = searchOctree(p, octree, ref);
                        Point deleted = octree.getRoot().getPoints().remove(i);
                        if (ref.size() > 1) {
                            for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                if (octree.getRoot().getDuplicates().get(j).getZ().equals(deleted.getZ()) &&
                                        octree.getRoot().getDuplicates().get(j).getX().equals(deleted.getX())
                                        && octree.getRoot().getDuplicates().get(j).getY().equals(deleted.getY())) {
                                    Point pFromDup = octree.getRoot().getDuplicates().remove(j);
//                        			octree= parseOctree(pFromDup.getX(),pFromDup.getY(),pFromDup.getZ(),octree,pFromDup.getRef());
                                    octree.getRoot().getPoints().add(pFromDup);
                                    break;
                                }
                            }
                        }
                        return octree;
                    } else if (octree.getRoot().getPoints().get(i).getX().equals(p.getX())
                            && octree.getRoot().getPoints().get(i).getY().equals(p.getY())
                            && p.getRef() != octree.getRoot().getPoints().get(i).getRef()) {

                        for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                            if (octree.getRoot().getDuplicates().get(j).getX().equals(p.getX())
                                    && octree.getRoot().getDuplicates().get(j).getY().equals(p.getY())
                                    && p.getRef() == octree.getRoot().getDuplicates().get(j).getRef()) {
                                octree.getRoot().getDuplicates().remove(j);
                                break;
                            }


                        }

                    }
                } else {

                    if (octree.getRoot().getPoints().get(i).getX().equals(p.getX())
                            && octree.getRoot().getPoints().get(i).getY().equals(p.getY())
                            && octree.getRoot().getPoints().get(i).getZ().equals(p.getZ())
                            && p.getRef() == octree.getRoot().getPoints().get(i).getRef()) {
                        ArrayList<Integer> ref = new ArrayList<>();
                        ref = searchOctree(p, octree, ref);
                        Point deleted = octree.getRoot().getPoints().remove(i);
                        if (ref.size() > 1) {
                            for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                                if (octree.getRoot().getDuplicates().get(j).getZ().equals(deleted.getZ()) &&
                                        octree.getRoot().getDuplicates().get(j).getX().equals(deleted.getX())
                                        && octree.getRoot().getDuplicates().get(j).getY().equals(deleted.getY())) {
                                    Point pFromDup = octree.getRoot().getDuplicates().remove(j);
//                        			octree= parseOctree(pFromDup.getX(),pFromDup.getY(),pFromDup.getZ(),octree,pFromDup.getRef());
                                    octree.getRoot().getPoints().add(pFromDup);
                                    break;
                                }
                            }
                        }
                        return octree;
                    } else if (octree.getRoot().getPoints().get(i).getX().equals(p.getX())
                            && octree.getRoot().getPoints().get(i).getY().equals(p.getY())
                            && octree.getRoot().getPoints().get(i).getZ().equals(p.getZ())
                            && p.getRef() != octree.getRoot().getPoints().get(i).getRef()) {

                        for (int j = 0; j < octree.getRoot().getDuplicates().size(); j++) {
                            if (octree.getRoot().getDuplicates().get(j).getX().equals(p.getX())
                                    && octree.getRoot().getDuplicates().get(j).getY().equals(p.getY())
                                    && octree.getRoot().getDuplicates().get(j).getZ().equals(p.getZ())
                                    && p.getRef() == octree.getRoot().getDuplicates().get(j).getRef()) {
                                octree.getRoot().getDuplicates().remove(j);
                                break;
                            }


                        }

                    }

                }
            }

        }
        return octree;
    }
    public static void updatePointRef(Point oldPoint, Octree octree, int reff, String indexName, String strTableName) throws DBAppException {
        if (octree.getRoot() == null || (octree.getRoot().getPoints().size() == 0 && !octree.isParent())) return;
        else if (octree.isParent()) {
            int childsIndext = determineOctreeIndex(oldPoint.getX(), oldPoint.getY(), oldPoint.getZ(), findMedianValue(octree.getMinX(), octree.getMaxX()), findMedianValue(octree.getMinY(), octree.getMaxY()), findMedianValue(octree.getMinZ(), octree.getMaxZ()));
            //parent: won't have null values , will reach a single page
            updatePointRef(oldPoint, octree.getChildNodes()[childsIndext], reff, indexName, strTableName);

        } else {
            leaf:
            for (int i = 0; i < octree.getRoot().getPoints().size(); i++) {
                if (octree.getRoot().getPoints().get(i).getX().equals(oldPoint.getX())
                        && octree.getRoot().getPoints().get(i).getY().equals(oldPoint.getY())
                        && octree.getRoot().getPoints().get(i).getZ().equals(oldPoint.getZ())
                        && oldPoint.getRef() == octree.getRoot().getPoints().get(i).getRef()) {
                    octree.getRoot().getPoints().get(i).setRef(reff);
                    WriteIndex(indexName, strTableName, octree);
                }
            }
        }
    }
    public static Octree parseOctree(Object x, Object y, Object z, Octree tree, int PageIndex) throws DBAppException {
        if (tree.getRoot() == null) {
            tree.setRoot(new Node(x, y, z, PageIndex));
            return tree;
        } else if (tree.getRoot().getPoints().size() < tree.getRoot().getMaxPoints() && !tree.isParent()) {
            Point p = new Point(x, y, z, PageIndex);

            if (checkDup(p, tree.getRoot())) tree.getRoot().getDuplicates().add(p);
            else tree.getRoot().getPoints().add(p);

            return tree;
        } else if (tree.getRoot().getPoints().size() >= tree.getRoot().getMaxPoints() && !tree.isParent()) {
            tree.setParent(true);
            //tree.depth++ ;
            tree.setChildNodes(new Octree[8]);
            for (int j = 0; j < 8; j++) {
                Object[] minsAndMaxs = findMinandMax(tree, j);
                tree.getChildNodes()[j] = new Octree();
                tree.getChildNodes()[j].setMinX(minsAndMaxs[0]);
                tree.getChildNodes()[j].setMinY(minsAndMaxs[1]);
                tree.getChildNodes()[j].setMinZ(minsAndMaxs[2]);
                tree.getChildNodes()[j].setMaxX(minsAndMaxs[3]);
                tree.getChildNodes()[j].setMaxY(minsAndMaxs[4]);
                tree.getChildNodes()[j].setMaxZ(minsAndMaxs[5]);

            }
            Object midX = findMedianValue(tree.getMinX(), tree.getMaxX());
            Object midY = findMedianValue(tree.getMinY(), tree.getMaxY());
            Object midZ = findMedianValue(tree.getMinZ(), tree.getMaxZ());
            for (int j = 0; j < tree.getRoot().getPoints().size(); j++) {
                Object pointX = tree.getRoot().getPoints().get(j).getX();
                Object pointY = tree.getRoot().getPoints().get(j).getY();
                Object pointZ = tree.getRoot().getPoints().get(j).getZ();
                int i = determineOctreeIndex(pointX, pointY, pointZ, midX, midY, midZ);
                if (tree.getChildNodes()[i].getRoot() == null)
                    tree.getChildNodes()[i].setRoot(new Node(pointX, pointY, pointZ, tree.getRoot().getPoints().get(j).getRef()));
                else
                    tree.getChildNodes()[i].getRoot().getPoints().add(new Point(pointX, pointY, pointZ, tree.getRoot().getPoints().get(j).getRef()));


            }
            for (int d = 0; d < tree.getRoot().getDuplicates().size(); d++) {
                Object dupX = tree.getRoot().getDuplicates().get(d).getX();
                Object dupY = tree.getRoot().getDuplicates().get(d).getY();
                Object dupZ = tree.getRoot().getDuplicates().get(d).getZ();
                int i = determineOctreeIndex(dupX, dupY, dupZ, midX, midY, midZ);
                if (tree.getChildNodes()[i].getRoot() == null)
                    tree.getChildNodes()[i].setRoot(new Node(dupX, dupY, dupZ, tree.getRoot().getDuplicates().get(d).getRef()));
                else
                    tree.getChildNodes()[i].getRoot().getDuplicates().add(new Point(dupX, dupY, dupZ, tree.getRoot().getDuplicates().get(d).getRef()));

            }
            int newPointi = determineOctreeIndex(x, y, z, midX, midY, midZ);
            tree.getChildNodes()[newPointi] = parseOctree(x, y, z, tree.getChildNodes()[newPointi], PageIndex);
            tree.getRoot().setPoints(new ArrayList<>());
            return tree;


        } else { //tree.parent
            Object midX = findMedianValue(tree.getMinX(), tree.getMaxX());
            Object midY = findMedianValue(tree.getMinY(), tree.getMaxY());
            Object midZ = findMedianValue(tree.getMinZ(), tree.getMaxZ());
            int i = determineOctreeIndex(x, y, z, midX, midY, midZ);
            tree.getChildNodes()[i] = parseOctree(x, y, z, tree.getChildNodes()[i], PageIndex);
            return tree;
        }
    }
    public static void WriteIndex(String indexName, String strTableName, Octree t) throws DBAppException {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream("meta/" + indexName + "_" + strTableName + ".class");
            ObjectOutputStream ObjectOutputStream = new ObjectOutputStream(fileOutputStream);
            ObjectOutputStream.writeObject(t);
            ObjectOutputStream.close();
            fileOutputStream.close();
        } catch (Exception e) {
            throw new DBAppException("can't serialize");
        }
    }
    public static Octree ReadIndex(String indexName, String strTableName) throws DBAppException {

        try {
            FileInputStream fileInputStream = new FileInputStream("meta/" + indexName + "_" + strTableName + ".class");
            ObjectInputStream ObjectInputStream = new ObjectInputStream(fileInputStream);
            Octree t = (Octree) ObjectInputStream.readObject();
            fileInputStream.close();
            ObjectInputStream.close();
            return t;
        } catch (Exception e) {
            throw new DBAppException("unable to deserialize , file not found ");
        }
    }
    private static Hashtable<String, Object> CheckConditions(String strClusteringKeyValue, ArrayList<String[]> tablesName, String strTableName, Hashtable<String, Object> htblColNameValue, String action) throws DBAppException {
        String primaryKey = getPrimKeyType(strTableName);
        Object obj = convertToDataType(strClusteringKeyValue);
        Hashtable<String, Object> rowToBeInserted = new Hashtable<>();
        for (int i = 0; i < tablesName.size(); i++) {
            if (tablesName.get(i)[0].equals(strTableName)) {
                if (htblColNameValue.containsKey(tablesName.get(i)[1])) {
                    if (tablesName.get(i)[3].equalsIgnoreCase("True") && action.equals("Update"))
                        throw new DBAppException("You cannot update the primary key!");
                    if (htblColNameValue.get((tablesName.get(i)[1])).getClass().getName().equals(tablesName.get(i)[2]) && compareToObject(htblColNameValue.get((tablesName.get(i)[1])), tablesName.get(i)[6]) >= 0 && compareToObject(htblColNameValue.get((tablesName.get(i)[1])), tablesName.get(i)[7]) <= 0) {
                        if (action.equals("Insert"))
                            rowToBeInserted.put(tablesName.get(i)[1], htblColNameValue.get((tablesName.get(i)[1])));
                        //check if a date in the correct format if exists otherwise throw an exception message
                    } else {
                        switch (action) {
                            case "Insert" -> throw new DBAppException("insertion failed due to unacceptable value");
                            case "Update" -> throw new DBAppException("Updating failed due to unacceptable value");
                            case "Delete" -> throw new DBAppException("deletion failed due to unacceptable value or incorrect date format");
                            default -> throw new DBAppException("Unknown Command");
                        }
                    }
                } else if (action.equals("Insert")) {
                    if (tablesName.get(i)[3].equalsIgnoreCase("False")) {
                        //rowToBeInserted.put(tablesName.get(i)[1], "null");
                        throw new DBAppException("you cannot insert without a value ");
                    } else {
                        throw new DBAppException("you cannot insert without a value for the primary key");
                    }
                } else if (action.equals("Update")) {
                    if (tablesName.get(i)[3].equals("True") && tablesName.get(i)[2].equals(obj.getClass().getName())) {
                        if (compareToObject(obj, tablesName.get(i)[6]) >= 0 && compareToObject(obj, tablesName.get(i)[7]) <= 0) {
                            primaryKey = tablesName.get(i)[1];
                        } else {
                            throw new DBAppException("You are violating the range of the primary key");
                        }
                    }
                }
            }
        }
        return rowToBeInserted;
    }
    public void init() {

    } // end of init()
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
        if (htblColNameMin.size() != htblColNameType.size() || htblColNameMax.size() != htblColNameType.size())
            throw new DBAppException("incompatible values for the min and max values!");
        try {
            Writecsv(strTableName, htblColNameType, strClusteringKeyColumn, htblColNameMin, htblColNameMax);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBAppException("Cannot Create your table");
        }
        Table t = new Table(strTableName);
        try {
            WriteTable(strTableName, t);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        //check that the length of the list is exactly 3
        if (strarrColName.length != 3) throw new DBAppException("you have to create index on EXACTLY 3 columns");
        //!!!!!check if we need to write the index name as in the description
        String indexName = strarrColName[0] + strarrColName[1] + strarrColName[2] + "Index"; // to be checked if we will put the whole attribute name or part of it !
        //update the csv file
        Object minX = 0;
        Object minY = 0;
        Object minZ = 0;
        Object maxX = 0;
        Object maxY = 0;
        Object maxZ = 0;
        String col1 = "";
        String col2 = "";
        String col3 = "";
        int flag = 0;
        ArrayList<String[]> TablesName;

        try {

            TablesName = loopCSV();

            for (String[] strings : TablesName) {

                if (strings[0].equals(strTableName) && (strings[1].equals(strarrColName[0]) || strings[1].equals(strarrColName[1]) || strings[1].equals(strarrColName[2]))) {

                    if (strings[5].equals("Octree.Octree"))
                        throw new DBAppException("an index already exists on that column");

                    flag++;

                    //update if the attribute name is in the list
                    strings[4] = indexName;
                    strings[5] = "Octree.Octree";
                    //initialize the min and max
                    switch (flag) {
                        case 1 -> {
                            minX = strings[6];
                            maxX = strings[7];
                            col1 = strings[1];
                        }
                        case 2 -> {
                            minY = strings[6];
                            maxY = strings[7];
                            col2 = strings[1];
                        }
                        case 3 -> {
                            minZ = strings[6];
                            maxZ = strings[7];
                            col3 = strings[1];
                        }
                    }

                }
            }

        } catch (Exception e) {
            throw new DBAppException(e);
        }
        if (flag != 3) {
            //we have to undo any partial changes , if flag>0 then the csv file has been partially updated
            for (int i = 0; i < TablesName.size(); i++) {
                if (TablesName.get(i)[0].equals(strTableName) && (TablesName.get(i)[1].equals(strarrColName[0]) || TablesName.get(i)[1].equals(strarrColName[1]) || TablesName.get(i)[1].equals(strarrColName[2]))) {
                    TablesName.get(i)[4] = "Null";
                    TablesName.get(i)[5] = "Null";
                }
            }
            throw new DBAppException("Index creation has failed ");
        }

        try {
            File file = new File("meta/Metadata.csv");

            // Create a FileWriter object for the CSV file.
            FileWriter writer = new FileWriter(file);

            // Iterate through the rows in the CSV file.
            for (int i = 0; i < file.length(); i++) {

                // Write an empty string to the file.
                writer.write("");
            }
            for (int i = 0; i < TablesName.size(); i++) {
                for (int j = 0; j < 7; j++) {
                    writer.write(TablesName.get(i)[j] + ",");
                }
                writer.write(TablesName.get(i)[7]);
                writer.write('\n');
            }


            writer.flush();
            writer.close();

        } catch (IOException e) {
            throw new DBAppException(e);
        }


        //create an Octree.Octree and a new file to save the tree
        Octree tree = new Octree(); //root = null
        tree.setMaxX(maxX);
        tree.setMaxY(maxY);
        tree.setMaxZ(maxZ);
        tree.setMinX(minX);
        tree.setMinY(minY);
        tree.setMinZ(minZ);
        tree.setCol1(col1);
        tree.setCol2(col2);
        tree.setCol3(col3);
        WriteIndex(indexName, strTableName, tree);
        Table t = ReadTable(strTableName);
        t.Indices.add(indexName);
        insertIntoIndex(strTableName, indexName);
        WriteTable(t.getTableName(), t);
    }
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        ArrayList<String[]> TablesName = loopCSV();
        checkColumns(TablesName, strTableName, htblColNameValue);
        Hashtable<String, Object> rowToBeInserted = new Hashtable<>();
        String primaryKey = getPrimKey(strTableName);
        rowToBeInserted = CheckConditions("", TablesName, strTableName, htblColNameValue, "Insert");
        //get the table from the file
        //do we need to deserialize and serialize in order to update the data in the file of the page??
        Table tableFile = ReadTable(strTableName);
        if (tableFile.noOfPages == 0) {
            //add a new page and serialize it AND serialize table
            try {
                tableFile.addPages(strTableName);
            } catch (IOException e) {

                throw new DBAppException(e);
            }
            Page p = ReadPage(0, strTableName);   //deserialize it, but why static 0 ? Haitham
            for (int i = 0; i < tableFile.Indices.size(); i++) {
                Octree t = ReadIndex(tableFile.Indices.get(i), tableFile.getTableName());
                t = parseOctree(rowToBeInserted.get(t.getCol1()), rowToBeInserted.get(t.getCol2()), rowToBeInserted.get(t.getCol3()), t, 0);
                WriteIndex(tableFile.Indices.get(i), strTableName, t);
            }
            p.addRow(rowToBeInserted, primaryKey);  //add row         
            WritePage(0, strTableName, p);     //serialize it again !!
            //  WriteTable(strTableName, tableFile);//serialize the table
            p = null;
            tableFile = null;
            System.gc();
            return;
        } else {
            Object ValueOfKey = htblColNameValue.get(primaryKey);
            boolean done = false;
            for (int x = 0; x < tableFile.noOfPages && !done; x++) {
                int myPage = x;
                Page p = ReadPage(myPage, tableFile.getTableName());  //deserialize the pages one by one
                if (compareToObject(ValueOfKey, p.getTuples().get(0).get(primaryKey)) >= 0 && compareToObject(ValueOfKey, p.getTuples().get(p.getTuples().size() - 1).get(primaryKey)) <= 0) {
                    //after the addrow method, check for overflow
                    for (int i = 0; i < tableFile.Indices.size(); i++) {
                        Octree t = ReadIndex(tableFile.Indices.get(i), tableFile.getTableName());
                        t = parseOctree(rowToBeInserted.get(t.getCol1()), rowToBeInserted.get(t.getCol2()), rowToBeInserted.get(t.getCol3()), t, myPage);
                        WriteIndex(tableFile.Indices.get(i), strTableName, t);
                    }
                    p.addRow(rowToBeInserted, primaryKey);   //update the page
                    WritePage(myPage, tableFile.getTableName(), p);   //serialize the page
                    done = true;
                    if (p.getTuples().size() >= p.getMaxRows()) {
                        try {
                            handleOverFlow(myPage, tableFile, primaryKey);
                        } catch (Exception e) {
                            throw new DBAppException(e);
                        }
                    }
                }
                p = null;
                System.gc();
            }
            if (!done) {
                int z = 0;
                Page p = ReadPage(0, strTableName);
                if (compareToObject(ValueOfKey, p.getTuples().get(0).get(primaryKey)) < 0) {
                    p.addRow(rowToBeInserted, primaryKey);
                    WritePage(0, strTableName, p);
                } else {
                    z = tableFile.noOfPages - 1;
                    p = ReadPage(z, strTableName);
                    p.addRow(rowToBeInserted, primaryKey);
                    WritePage(z, strTableName, p);

                }
                for (int i = 0; i < tableFile.Indices.size(); i++) {
                    Octree t = ReadIndex(tableFile.Indices.get(i), tableFile.getTableName());
                    t = parseOctree(rowToBeInserted.get(t.getCol1()), rowToBeInserted.get(t.getCol2()), rowToBeInserted.get(t.getCol3()), t, z);
                    WriteIndex(tableFile.Indices.get(i), strTableName, t);
                }
                if (p.getTuples().size() > p.getMaxRows()) {
                    //there is an overflow
                    try {
                        handleOverFlow(z, tableFile, primaryKey);
                    } catch (Exception e) {

                        throw new DBAppException(e);
                    }
                }
                p = null;
                System.gc();
            }
        }
        // WriteTable(strTableName, tableFile);
        tableFile = null;
        System.gc();
    }
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        ArrayList<String[]> tablesName = loopCSV();
        String primaryKey = getPrimKey(strTableName);
        checkColumns(tablesName, strTableName, htblColNameValue);
        Object obj = convertToDataType(strClusteringKeyValue);
        String type = getPrimKeyType(strTableName);
        if (type.equals("java.util.Date")) {
            checkDateFormat(strClusteringKeyValue);
        }
        Hashtable<String, Object> check = CheckConditions(strClusteringKeyValue, tablesName, strTableName, htblColNameValue, "Update");
        // from the above comment line till here

        Hashtable<String, Object> newRow = new Hashtable<>();
        Hashtable<String, Object> oldRow = new Hashtable<>();
        int pageRef = 0;

        Table tableFile = ReadTable(strTableName);    //deserialize table
        if (tableFile.noOfPages == 0) throw new DBAppException("the table is empty");
        int pageIndex;
        boolean found = false;
        ArrayList<Integer> reff = new ArrayList<>();
        boolean isClusterIndexed = isClusterIndexed(primaryKey, strTableName);
        if (isClusterIndexed) {
            String indexName = getIndexName(new ArrayList<>(Collections.singletonList(primaryKey)), strTableName);
            Octree t = ReadIndex(indexName, strTableName);
            Point newPoint = new Point(null, null, null, -1);
            Point oldPoint = new Point(null, null, null, -1);
            if (t.getCol1().equals(primaryKey))
                newPoint.setX(obj);
            else if (t.getCol2().equals(primaryKey))
                newPoint.setY(obj);
            else if (t.getCol3().equals(primaryKey))
                newPoint.setZ(obj);
            reff = searchOctree(newPoint, t, reff);
            for (int i : reff) {
                Page page = ReadPage(i, strTableName);
                found = false;
                int l = 0;
                int r = page.getTuples().size() - 1;
                while (l <= r) {
                    int m = l + (r - l) / 2;
                    // Check if x is present at mid
                    if (compareToObject(obj, page.getTuples().get(m).get(primaryKey)) == 0) {
                        pageRef = i;
                        oldPoint.setX(page.getTuples().get(m).get(t.getCol1()));
                        oldPoint.setY(page.getTuples().get(m).get(t.getCol2()));
                        oldPoint.setZ(page.getTuples().get(m).get(t.getCol3()));
                        oldPoint.setRef(i);
                        found = true;
                        t = deleteOctree(oldPoint, t);
                        WriteIndex(tableFile.Indices.get(i), strTableName, t);
                        for (String key : htblColNameValue.keySet()) {
                            if (t.getCol1().equals(key))
                                newPoint.setX(htblColNameValue.get(key));
                            else if (t.getCol2().equals(key))
                                newPoint.setY(htblColNameValue.get(key));
                            else if (t.getCol3().equals(key))
                                newPoint.setZ(htblColNameValue.get(key));
                            //any keys have index ? if yes, update the index 3
                            page.getTuples().get(m).put(key, htblColNameValue.get(key)); // update happened
                        }
                        newRow = page.getTuples().get(m);
                        Octree octree = ReadIndex(tableFile.Indices.get(i), strTableName);
                        t = parseOctree(newPoint.getX(), newPoint.getY(), newPoint.getZ(), octree, i);
                        WriteIndex(indexName, strTableName, t);
                        WritePage(i, strTableName, page);

                        break;
                    }
                    // If x greater, ignore left half
                    if (compareToObject(obj, page.getTuples().get(m).get(primaryKey)) > 0) l = m + 1;
                        // If x is smaller, ignore right half
                    else r = m - 1;
                }
                page = null;
                System.gc();
                if (found) break;
            }
        } else {
            Point point = new Point(null, null, null, -1);
            for (int x = 0; x < tableFile.noOfPages; x++) {
                pageIndex = x;
                //.getTuples().get(p.getTuples().size() - 1).get(primaryKey)
                Page p = ReadPage(pageIndex, strTableName);    //deserialize page by page
                if (compareToObject(obj, p.getTuples().get(0).get(primaryKey)) >= 0 && compareToObject(obj, p.getTuples().get(p.getTuples().size() - 1).get(primaryKey)) <= 0) {
                    found = false;
                    int l = 0;
                    int r = p.getTuples().size() - 1;
                    int i = 0;
                    while (l <= r) {
                        int m = l + (r - l) / 2;
                        // Check if x is present at mid
                        if (compareToObject(obj, p.getTuples().get(m).get(primaryKey)) == 0) {
                            found = true;
                            pageRef = x;
                            oldRow.putAll(p.getTuples().get(m));

                            for (String key : htblColNameValue.keySet()) {
                                p.getTuples().get(m).put(key, htblColNameValue.get(key)); // update happened
                            }
                            newRow.putAll(p.getTuples().get(m));
                            WritePage(pageIndex, strTableName, p);
                            break;
                        }
                        // If x greater, ignore left half
                        if (compareToObject(obj, p.getTuples().get(m).get(primaryKey)) > 0) l = m + 1;
                            // If x is smaller, ignore right half
                        else r = m - 1;
                        i++;
                    }
                }
                p = null;
                System.gc();
            }
            //update all indices with columns present in the hashTable
            for (int i = 0; i < tableFile.Indices.size(); i++) {
                Octree tree = ReadIndex(tableFile.Indices.get(i), strTableName);
                if (htblColNameValue.containsKey(tree.getCol1()) &&
                        htblColNameValue.containsKey(tree.getCol2()) &&
                        htblColNameValue.containsKey(tree.getCol3())) {//get oldRow and newRow
                    point.setX(oldRow.get(tree.getCol1()));
                    point.setY(oldRow.get(tree.getCol2()));
                    point.setZ(oldRow.get(tree.getCol3()));
                    point.setRef(pageRef);
                    tree = deleteOctree(point, tree);
                    WriteIndex(tableFile.Indices.get(i), strTableName, tree);
                    Point p2 = new Point(htblColNameValue.get(tree.getCol1()), htblColNameValue.get(tree.getCol2()), htblColNameValue.get(tree.getCol3()), pageRef);
                    Octree octree = ReadIndex(tableFile.Indices.get(i), strTableName);
                    tree = parseOctree(p2.getX(), p2.getY(), p2.getZ(), octree, pageRef);
                    WriteIndex(tableFile.Indices.get(i), strTableName, tree);
                }

            }


        }
        if (!found) throw new DBAppException("primary key not found");
        tableFile = null;
        System.gc();
    }
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        ArrayList<String[]> tablesName = loopCSV();
        Table table = ReadTable(strTableName);   //deserialize the table
        checkColumns(tablesName, strTableName, htblColNameValue);
        Hashtable<String, Object> check = CheckConditions("", tablesName, strTableName, htblColNameValue, "Delete");
        String primaryKey = getPrimKey(strTableName);
        ArrayList<Integer> pagesNo = new ArrayList<>();
        Point point = new Point(null, null, null, -1);
        int index;
        if (table.Indices.size() != 0) {

            Octree tree = new Octree();
            for (index = 0; index < table.Indices.size(); index++) {
                tree = ReadIndex(table.Indices.get(index), strTableName);

                if (htblColNameValue.containsKey(tree.getCol1()))
                    point.setX(htblColNameValue.get(tree.getCol1()));
                if (htblColNameValue.containsKey(tree.getCol2()))
                    point.setY(htblColNameValue.get(tree.getCol2()));
                if (htblColNameValue.containsKey(tree.getCol3()))
                    point.setZ(htblColNameValue.get(tree.getCol3()));
                if (point.getX() != null || point.getY() != null || point.getZ() != null)
                    break;
                WriteIndex(table.Indices.get(index), strTableName, tree);
            }
            if (point.getX() != null || point.getY() != null || point.getZ() != null) {
                //if the tree helped and now we have page numbers
                pagesNo = searchOctree(point, tree, pagesNo);
//            	int pagesSize= pagesNo.size();

                for (int i = 0; i < pagesNo.size(); i++) {
                    boolean match = false;
                    int myPage = pagesNo.get(i);
                    //i is the page id
                    Page p = ReadPage(myPage, strTableName);   //deserialize the page
                    for (int j = 0; j < p.getTuples().size(); j++) {  //j is the id of the tuple
                        match = false;
                        for (String key : htblColNameValue.keySet()) {
                            if (!p.getTuples().get(j).get(key).equals(htblColNameValue.get(key))) {
                                match = false;
                                break;
                            } else {
                                match = true;
                            }
                        }
                        if (match) {
                            try {
                                //delete the tuple
                                p.getTuples().removeElementAt(j);    //delete from table
                                WritePage(myPage, strTableName, p);
                                point.setRef(myPage);
                                tree = deleteOctree(point, tree);          //delete from octree
                                WriteIndex(table.Indices.get(index), strTableName, tree);
                                if (p.getTuples().size() == 0) {     //delete empty page
                                    table.deletePage(myPage);
                                    File file = new File("meta/" + table.getTableName() + myPage + ".class");
                                    file.delete();
                                }
                                j--;
                            } catch (IOException e) {
                                throw new DBAppException(e);
                            }
                        }
                    }
                    p = null;
                    System.gc();

                }

            }
        }
        //ELSE if it doesn't have index or the inputs don't contain a column from the index
        if ((table.Indices.size() != 0) || (point.getX() == null && point.getY() == null && point.getZ() == null)) {
            if (htblColNameValue.containsKey(getPrimKey(strTableName))) {
                int q = table.noOfPages;
                int pageIndex;
                int x = 0;
                for (x = 0; x < q; x++) {
                    pageIndex = x;
                    Page p = ReadPage(pageIndex, strTableName);   //deserialize the page
                    if (compareToObject(htblColNameValue.get(primaryKey), p.getTuples().get(0).get(primaryKey)) >= 0 && compareToObject(htblColNameValue.get(primaryKey), p.getTuples().get(p.getTuples().size() - 1).get(primaryKey)) <= 0) {

                        int l = 0;
                        int r = p.getTuples().size() - 1;
                        while (l <= r) {
                            int m = l + (r - l) / 2;
                            // Check if x is present at mid
                            if (compareToObject(htblColNameValue.get(primaryKey), p.getTuples().get(m).get(primaryKey)) == 0) {

                                for (String key : htblColNameValue.keySet()) {
                                    if (!p.getTuples().get(m).get(key).equals(htblColNameValue.get(key)))
                                        throw new DBAppException("no tuples are found with these values");
                                }
                                try {
                                    //delete the tuple
                                    if (table.Indices.size() != 0) {

                                        Octree tree = new Octree();
                                        for (index = 0; index < table.Indices.size(); index++) {
                                            tree = ReadIndex(table.Indices.get(index), strTableName);

                                            point.setX(p.getTuples().get(m).get(tree.getCol1()));
                                            point.setY(p.getTuples().get(m).get(tree.getCol2()));
                                            point.setZ(p.getTuples().get(m).get(tree.getCol3()));
                                            ArrayList<Integer> ref = new ArrayList<>();
                                            ref = searchOctree(point, tree, ref);
                                            point.setRef(ref.get(0));
                                            tree = deleteOctree(point, tree);
                                            WriteIndex(table.Indices.get(index), strTableName, tree);
                                        }
                                    }
                                    p.getTuples().removeElementAt(m);
                                    WritePage(pageIndex, strTableName, p);
                                    if (p.getTuples().size() == 0) {
                                        table.deletePage(pageIndex);
                                        File file = new File("meta/" + table.getTableName() + pageIndex + ".class");
                                        file.delete();

                                        //delete the page file

                                    }
                                } catch (IOException e) {
                                    throw new DBAppException(e);
                                }
                                p = null;
                                System.gc();
                            }
                            // If x greater, ignore left half
                            assert p != null;
                            if (compareToObject(htblColNameValue.get(primaryKey), p.getTuples().get(m).get(primaryKey)) > 0)
                                l = m + 1;
                                // If x is smaller, ignore right half
                            else r = m - 1;
                        }

                    }
                } // Binary Search

                table = null;
                System.gc();
            } else {
                boolean match = false;
                int q = table.noOfPages;
                for (int i = 0; i < q; i++) {   //i is the page id
                    Page p = ReadPage(i, strTableName);   //deserialize the page
                    for (int j = 0; j < p.getTuples().size(); j++) {  //j is the id of the tuple
                        match = false;
                        for (String key : htblColNameValue.keySet()) {
                            if (!p.getTuples().get(j).get(key).equals(htblColNameValue.get(key))) {
                                match = false;
                                break;
                            } else {
                                match = true;
                            }
                        }
                        if (match) {
                            try {
                                //delete from octree
                                if (table.Indices.size() != 0) {
                                    Octree tree = new Octree();
                                    for (index = 0; index < table.Indices.size(); index++) {
                                        tree = ReadIndex(table.Indices.get(index), strTableName);
                                        point.setX(p.getTuples().get(j).get(tree.getCol1()));
                                        point.setY(p.getTuples().get(j).get(tree.getCol2()));
                                        point.setZ(p.getTuples().get(j).get(tree.getCol3()));
                                        ArrayList<Integer> ref = new ArrayList<>();
                                        ref = searchOctree(point, tree, ref);
                                        point.setRef(ref.get(0));
                                        tree = deleteOctree(point, tree);
                                        WriteIndex(table.Indices.get(index), strTableName, tree);
                                    }
                                }
                                //delete the tuple
                                p.getTuples().removeElementAt(j);
                                WritePage(i, strTableName, p);
                                if (p.getTuples().size() == 0) {
                                    table.deletePage(i);
                                    File file = new File("meta/" + table.getTableName() + i + ".class");
                                    file.delete();
                                }
                                j--;
                            } catch (IOException e) {
                                throw new DBAppException(e);
                            }
                        }
                    }
                    p = null;
                    System.gc();
                }
                table = null;
                System.gc();
            }

        }


    }
}
