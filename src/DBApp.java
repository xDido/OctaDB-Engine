import Exceptions.DBAppException;
import Objects.*;
import Octree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import src.DBConstants; // Added import

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

    private static final Logger logger = LoggerFactory.getLogger(DBApp.class);

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        logger.info("Attempting to select from table with terms: {} and operators: {}", Arrays.toString(arrSQLTerms), Arrays.toString(strarrOperators));

        if (arrSQLTerms == null || arrSQLTerms.length == 0) {
            logger.error("SQL terms array is null or empty.");
            throw new DBAppException("SQL terms cannot be null or empty.");
        }
        if (strarrOperators == null && arrSQLTerms.length > 1) {
            logger.error("Operators array is null but multiple SQL terms provided.");
            throw new DBAppException("Operators array cannot be null when multiple SQL terms are provided.");
        }
        if (arrSQLTerms.length - 1 != (strarrOperators != null ? strarrOperators.length : 0)) {
            logger.error("Mismatch between number of SQL terms and operators. Terms: {}, Operators: {}", arrSQLTerms.length, (strarrOperators != null ? strarrOperators.length : 0));
            throw new DBAppException("Compilation error: Number of terms and operators does not match.");
        }

        String singleTableName = arrSQLTerms[0].getTableName();
        logger.debug("Target table for select: {}", singleTableName);

        Vector<Hashtable<String, Object>> result;
        ArrayList<String> colNamesForIndexCheck = new ArrayList<>();

        logger.debug("Validating SQL terms...");
        for (SQLTerm term : arrSQLTerms) {
            if (!singleTableName.equals(term.getTableName())) {
                logger.error("All SQL terms must belong to the same table. Expected: {}, Found: {}", singleTableName, term.getTableName());
                throw new DBAppException("All SQL terms in a select query must belong to the same table.");
            }
            ArrayList<String[]> tableMetadata = loopCSV();
            boolean termIsValid = false;
            for (String[] columnMetadata : tableMetadata) {
                if (columnMetadata[0].equals(singleTableName) &&
                    columnMetadata[1].equals(term.getColumnName()) &&
                    checkStringType("" + term.getValue()).equals(columnMetadata[2])) {
                    termIsValid = true;
                    break;
                }
            }
            if (termIsValid) {
                colNamesForIndexCheck.add(term.getColumnName());
                logger.trace("Term validated: Table: {}, Column: {}, Operator: {}, Value: {}", term.getTableName(), term.getColumnName(), term.getOperator(), term.getValue());
            } else {
                logger.error("Invalid term detected: Table: {}, Column: {}, Value: {}. Type mismatch or column not found.", term.getTableName(), term.getColumnName(), term.getValue());
                throw new DBAppException("Error while validating SQL term: Table=" + term.getTableName() + ", Column=" + term.getColumnName() + ". Type mismatch or column/table not found.");
            }
        }
        logger.debug("All SQL terms validated successfully. Columns for index check: {}", colNamesForIndexCheck);

        String indexToUse = getIndexName(colNamesForIndexCheck, singleTableName);
        boolean allANDs = allOperatorsAreAND(strarrOperators != null ? strarrOperators : new String[0]);
        logger.debug("Index found: '{}'. All operators are AND: {}", indexToUse, allANDs);

        if (DBConstants.LINEAR_SEARCH_INDICATOR.equals(indexToUse) || !allANDs) {
            logger.info("Performing select using linear scan (executeComplexQuery). Index: '{}', All ANDs: {}", indexToUse, allANDs);
            result = executeComplexQuery(singleTableName, arrSQLTerms, strarrOperators != null ? strarrOperators : new String[0]);
        } else {
            logger.info("Performing select using Octree index '{}' (performQueryWithIndex).", indexToUse);
            Octree tree = ReadIndex(indexToUse, singleTableName);
            ArrayList<Integer> potentialPagesFromIndex = new ArrayList<>();
            Set<Integer> uniquePages = new HashSet<>();
            for (SQLTerm term : arrSQLTerms) {
                if (term.getColumnName().equals(tree.getCol1()) || term.getColumnName().equals(tree.getCol2()) || term.getColumnName().equals(tree.getCol3())) {
                    logger.debug("Getting pages by range for term: {} on index {}", term, indexToUse);
                    ArrayList<Integer> pagesForTerm = getPagesByRange(term, tree, singleTableName);
                    logger.trace("Pages for term {}: {}", term, pagesForTerm);
                    uniquePages.addAll(pagesForTerm);
                } else {
                    logger.warn("Term column '{}' is not part of the selected Octree index '{}'. This specific term won't use the index for page filtering.", term.getColumnName(), indexToUse);
                }
            }
            potentialPagesFromIndex.addAll(uniquePages);
            logger.debug("Consolidated potential pages from index '{}': {}", indexToUse, potentialPagesFromIndex);
            if (potentialPagesFromIndex.isEmpty() && arrSQLTerms.length > 0) {
                 logger.info("Index search on '{}' yielded no potential pages. Result set will be empty.", indexToUse);
                 result = new Vector<>();
            } else {
                result = performQueryWithIndex(potentialPagesFromIndex, singleTableName, arrSQLTerms, strarrOperators != null ? strarrOperators : new String[0]);
            }
        }

        logger.info("Select query execution completed. Number of results: {}", result.size());
        return result.iterator();
    }

    public static boolean allOperatorsAreAND(String[] operators) {
        if (operators == null || operators.length == 0) {
            return true;
        }
        for (String operator : operators) {
            if (!DBConstants.OPERATOR_AND.equals(operator))
                return false;
        }
        return true;
    }

    public static ArrayList<Integer> getPagesByRange(SQLTerm sqlTerm, Octree tree, String strTableName) throws DBAppException {
        ArrayList<Integer> reff = new ArrayList<>();
        Point p = new Point(null, null, null, -1);
        if (tree.getCol1().equals(sqlTerm.getColumnName()))
            p.setX(sqlTerm.getValue());
        else if (tree.getCol2().equals(sqlTerm.getColumnName()))
            p.setY(sqlTerm.getValue());
        else if (tree.getCol3().equals(sqlTerm.getColumnName()))
            p.setZ(sqlTerm.getValue());

        switch (sqlTerm.getOperator()) {
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
                break;
            case "!=":
                ArrayList<Integer> Tempreff2 = searchOctree(p, tree, reff);
                Table t = ReadTable(strTableName);
                ArrayList<Integer> allPages = new ArrayList<>();
                for (int i = 0; i < t.noOfPages; i++) {
                    allPages.add(i);
                }
                reff.clear();
                for(Integer pageNum : allPages){
                    if(!Tempreff2.contains(pageNum)){
                        reff.add(pageNum);
                    }
                }
                break;
            case "=":
                reff = searchOctree(p, tree, reff);
                break;
            default:
                logger.warn("Unsupported operator '{}' in getPagesByRange.", sqlTerm.getOperator());
        }
        return reff;
    }

    public static ArrayList<Integer> performRangeQuery(String operator, Point point, Octree tree) throws DBAppException {
        if (operator.equals(">")) {
            if ((point.getX() != null && compareToObject(point.getX(), tree.getMaxX()) > 0)
                    || (point.getY() != null && compareToObject(point.getY(), tree.getMaxY()) > 0) ||
                    (point.getZ() != null && compareToObject(point.getZ(), tree.getMaxZ()) > 0)) {
                return new ArrayList<>();
            }
            if (tree.getRoot() == null) return new ArrayList<>();
            if (tree.isParent()) {
                ArrayList<Integer> ref = new ArrayList<>();
                for (int i = 0; i < tree.getChildNodes().length; i++) ref.addAll(performRangeQuery(operator, point, tree.getChildNodes()[i]));
                return ref;
            }
            ArrayList<Integer> ref = new ArrayList<>();
            for (int i = 0; i < tree.getRoot().getPoints().size(); i++)
                ref.add(tree.getRoot().getPoints().get(i).getRef());
            return ref;
        }
        if (operator.equals("<")) {
            if ((point.getX() != null && compareToObject(point.getX(), tree.getMinX()) < 0)
                    || (point.getY() != null && compareToObject(point.getY(), tree.getMinY()) < 0) ||
                    (point.getZ() != null && compareToObject(point.getZ(), tree.getMinZ()) < 0)) {
                return new ArrayList<>();
            }
            if (tree.getRoot() == null) return new ArrayList<>();
            if (tree.isParent()) {
                ArrayList<Integer> ref = new ArrayList<>();
                for (int i = 0; i < tree.getChildNodes().length; i++) ref.addAll(performRangeQuery(operator, point, tree.getChildNodes()[i]));
                return ref;
            }
            ArrayList<Integer> ref = new ArrayList<>();
            for (int i = 0; i < tree.getRoot().getPoints().size(); i++)
                ref.add(tree.getRoot().getPoints().get(i).getRef());
            return ref;
        }
        return null;
    }

    public static Vector<Hashtable<String, Object>> performQueryWithIndex(ArrayList<Integer> pageIndices, String strTableName, SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        logger.debug("Performing query with index. Pages: {}, Table: {}, Terms: {}, Operators: {}", pageIndices, strTableName, Arrays.toString(arrSQLTerms), Arrays.toString(strarrOperators));
        if (arrSQLTerms.length == 0) return new Vector<>();
        if (pageIndices.isEmpty() && arrSQLTerms.length > 0) return new Vector<>();

        if (strarrOperators.length == 0) {
            SQLTerm term = arrSQLTerms[0];
            logger.trace("Single term or base case for performQueryWithIndex: {}", term);
            return getTuplesFromIndex(pageIndices, strTableName, term.getColumnName(), term.getOperator(), term.getValue());
        }

        SQLTerm term1 = arrSQLTerms[0];
        String operator = strarrOperators[0];

        SQLTerm[] remainingTerms = Arrays.copyOfRange(arrSQLTerms, 1, arrSQLTerms.length);
        String[] remainingOperators = Arrays.copyOfRange(strarrOperators, 1, strarrOperators.length);

        logger.trace("Processing term: {} with operator: {}", term1, operator);
        Vector<Hashtable<String, Object>> result1 = getTuplesFromIndex(pageIndices, strTableName, term1.getColumnName(), term1.getOperator(), term1.getValue());

        if (result1.isEmpty() && DBConstants.OPERATOR_AND.equalsIgnoreCase(operator)) {
            logger.debug("Intermediate result for term {} is empty and operator is AND, returning empty set.", term1);
            return new Vector<>();
        }

        Vector<Hashtable<String, Object>> result2 = performQueryWithIndex(pageIndices, strTableName, remainingTerms, remainingOperators);

         if (result2.isEmpty() && DBConstants.OPERATOR_AND.equalsIgnoreCase(operator)) {
            logger.debug("Recursive call result is empty and operator is AND, returning empty set.");
            return new Vector<>();
        }

        logger.debug("Combining results. Result1 size: {}, Result2 size: {}, Operator: {}", result1.size(), result2.size(), operator);
        return switch (operator.toUpperCase()) {
            case DBConstants.OPERATOR_AND -> intersection(result1, result2);
            case DBConstants.OPERATOR_OR -> union(result1, result2);
            case DBConstants.OPERATOR_XOR -> sub(union(result1, result2), intersection(result1, result2));
            default -> {
                logger.error("Undefined operator in performQueryWithIndex: {}", operator);
                throw new DBAppException("Undefined operation: " + operator);
            }
        };
    }

    public static Vector<Hashtable<String, Object>> executeComplexQuery(String strTableName, SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        logger.debug("Executing complex query (linear scan). Table: {}, Terms: {}, Operators: {}", strTableName, Arrays.toString(arrSQLTerms), Arrays.toString(strarrOperators));
        if (arrSQLTerms.length == 0) return new Vector<>();

        if (strarrOperators.length == 0) {
            SQLTerm term = arrSQLTerms[0];
            logger.trace("Single term or base case for executeComplexQuery: {}", term);
            return getTuples(strTableName, term.getColumnName(), term.getOperator(), term.getValue());
        }

        SQLTerm term1 = arrSQLTerms[0];
        String operator = strarrOperators[0];

        SQLTerm[] remainingTerms = Arrays.copyOfRange(arrSQLTerms, 1, arrSQLTerms.length);
        String[] remainingOperators = Arrays.copyOfRange(strarrOperators, 1, strarrOperators.length);

        logger.trace("Processing term: {} with operator: {}", term1, operator);
        Vector<Hashtable<String, Object>> result1 = getTuples(strTableName, term1.getColumnName(), term1.getOperator(), term1.getValue());

        if (result1.isEmpty() && DBConstants.OPERATOR_AND.equalsIgnoreCase(operator)) {
            logger.debug("Intermediate result for term {} is empty and operator is AND, returning empty set.", term1);
            return new Vector<>();
        }

        Vector<Hashtable<String, Object>> result2 = executeComplexQuery(strTableName, remainingTerms, remainingOperators);

        if (result2.isEmpty() && DBConstants.OPERATOR_AND.equalsIgnoreCase(operator)) {
            logger.debug("Recursive call result is empty and operator is AND, returning empty set.");
            return new Vector<>();
        }

        logger.debug("Combining results. Result1 size: {}, Result2 size: {}, Operator: {}", result1.size(), result2.size(), operator);
        return switch (operator.toUpperCase()) {
            case DBConstants.OPERATOR_AND -> intersection(result1, result2);
            case DBConstants.OPERATOR_OR -> union(result1, result2);
            case DBConstants.OPERATOR_XOR -> sub(union(result1, result2), intersection(result1, result2));
            default -> {
                logger.error("Undefined operator in executeComplexQuery: {}", operator);
                throw new DBAppException("Undefined operation: " + operator);
            }
        };
    }

    public static boolean doOperation(Object tupleValue, String operator, Object SQLTermValue) throws DBAppException {
        int i = compareToObject(tupleValue, SQLTermValue);
        return switch (operator) {
            case ">" -> i > 0;
            case ">=" -> i >= 0;
            case "<" -> i < 0;
            case "<=" -> i <= 0;
            case "!=" -> i != 0;
            case "=" -> i == 0;
            default -> {
                logger.error("Invalid operator in doOperation: {}", operator);
                throw new DBAppException("Invalid operator: " + operator);
            }
        };
    }

    public static Vector<Hashtable<String, Object>> getTuplesFromIndex(ArrayList<Integer> pageIndices, String strTableName, String key, String operator, Object value) throws DBAppException {
        logger.debug("Getting tuples from index. Pages: {}, Table: {}, Key: {}, Operator: {}, Value: {}", pageIndices, strTableName, key, operator, value);
        Vector<Hashtable<String, Object>> result = new Vector<>();
        for (Integer pageIndex : pageIndices) {
            Page p = ReadPage(pageIndex, strTableName);
            for (int j = 0; j < p.getTuples().size(); j++) {
                if (doOperation(p.getTuples().get(j).get(key), operator, value))
                    result.add(p.getTuples().get(j));
            }
        }
        logger.trace("getTuplesFromIndex result size: {}", result.size());
        return result;
    }

    public static Vector<Hashtable<String, Object>> getTuples(String strTableName, String key, String operator, Object value) throws DBAppException {
        logger.debug("Getting tuples via linear scan. Table: {}, Key: {}, Operator: {}, Value: {}", strTableName, key, operator, value);
        Vector<Hashtable<String, Object>> result = new Vector<>();
        Table table = ReadTable(strTableName);
        for (int i = 0; i < table.noOfPages; i++) {
            Page p = ReadPage(i, strTableName);
            for (int j = 0; j < p.getTuples().size(); j++) {
                if (doOperation(p.getTuples().get(j).get(key), operator, value))
                    result.add(p.getTuples().get(j));
            }
        }
        logger.trace("getTuples (linear scan) result size: {}", result.size());
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
        logger.debug("Searching for index in table '{}' for columns: {}", strTableName, colNames);
        Table table = ReadTable(strTableName);
        for (int i = 0; i < table.Indices.size(); i++) {
            Octree tree = ReadIndex(table.Indices.get(i), strTableName);
            if (colNames.contains(tree.getCol1()) && colNames.contains(tree.getCol2()) && colNames.contains(tree.getCol3())) {
                logger.info("Found matching index '{}' for table '{}' based on columns: {}, {}, {}", table.Indices.get(i), strTableName, tree.getCol1(), tree.getCol2(), tree.getCol3());
                return table.Indices.get(i);
            }
        }
        logger.info("No suitable Octree index found for columns {} in table '{}'. Defaulting to linear search.", colNames, strTableName);
        return DBConstants.LINEAR_SEARCH_INDICATOR;
    }

    public static boolean isClusterIndexed(String strClusteringKey, String strTableName) throws DBAppException {
        Table table = ReadTable(strTableName);
        for (int i = 0; i < table.Indices.size(); i++) {
            Octree tree = ReadIndex(table.Indices.get(i), strTableName);
            if (strClusteringKey.equals(tree.getCol1()) || strClusteringKey.equals(tree.getCol2()) || strClusteringKey.equals(tree.getCol3())) {
                logger.debug("Clustering key '{}' is part of index '{}' in table '{}'.", strClusteringKey, table.Indices.get(i), strTableName);
                return true;
            }
        }
        logger.debug("Clustering key '{}' is not part of any index in table '{}'.", strClusteringKey, strTableName);
        return false;
    }

    public static void handleOverFlow(int pageIndex, Table table, String primaryKey) throws Exception {
        logger.info("Handling overflow for page {} in table '{}'. Primary Key: {}", pageIndex, table.getTableName(), primaryKey);
        for (int i = pageIndex; i < table.noOfPages; i++) {
            Page p = ReadPage(i, table.getTableName());
            if(p.getTuples().size() <= p.getMaxRows()){
                 logger.debug("Page {} in table '{}' is no longer overflowing (size {}). Stopping overflow handling for this chain.", i, table.getTableName(), p.getTuples().size());
                 break;
            }
            Hashtable<String, Object> tuple = p.getTuples().get(p.getMaxRows());
            logger.trace("Moving tuple {} from page {} to next page.", tuple, i);
            p.getTuples().remove(p.getMaxRows());
            WritePage(i, table.getTableName(), p);

            int nextPageIdx = i + 1;
            if (i == table.noOfPages - 1) {
                logger.debug("Last page is overflowing. Adding a new page to table '{}'.", table.getTableName());
                table.addPages(table.getTableName());
                 WriteTable(table.getTableName(), table);
            }

            Page p1 = ReadPage(nextPageIdx, table.getTableName());
            p1.addRow(tuple, primaryKey);
            logger.trace("Added tuple {} to page {}.", tuple, nextPageIdx);

            for (int j = 0; j < table.Indices.size(); j++) {
                String indexName = table.Indices.get(j);
                Octree tree = ReadIndex(indexName, table.getTableName());
                Point pointToUpdate = new Point(tuple.get(tree.getCol1()), tuple.get(tree.getCol2()), tuple.get(tree.getCol3()), i);
                logger.trace("Updating index '{}': Point {} ref from {} to {}", indexName, pointToUpdate, i, nextPageIdx);
                updatePointRef(pointToUpdate, tree, nextPageIdx, indexName, table.getTableName());
            }
            WritePage(nextPageIdx, table.getTableName(), p1);
            if (p1.getTuples().size() <= p1.getMaxRows()){
                 logger.debug("Overflow handled for page chain starting at {}. Next page {} now has size {}.", pageIndex, nextPageIdx, p1.getTuples().size());
                 break;
            }
        }
        logger.info("Overflow handling completed for chain starting at page {} in table '{}'.", pageIndex, table.getTableName());
    }

    public static int compareToObject(Object Value, Object pageValue) throws DBAppException {
        int r;
        if (Value instanceof Integer) {
            int a = (int) Value;
            int b = Integer.parseInt(String.valueOf(pageValue));
            r = Integer.compare(a, b);
        } else if (Value instanceof Double) {
            double a = (double) Value;
            double b = Double.parseDouble(String.valueOf(pageValue));
            r = Double.compare(a, b);
        } else if (Value instanceof String) {
            r = ((String) Value).compareTo(String.valueOf(pageValue));
        } else if (Value instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(DBConstants.DATE_FORMAT_YYYY_MM_DD);
            dateFormat.setLenient(false);
            try {
                Date d1 = (Date) Value;
                Date d2;
                if (pageValue instanceof Date) {
                    d2 = (Date) pageValue;
                } else {
                    d2 = dateFormat.parse(String.valueOf(pageValue));
                }
                r = d1.compareTo(d2);
            } catch (ParseException e) {
                logger.error("ParseException during date comparison. Value: {}, PageValue: {}", Value, pageValue, e);
                throw new DBAppException("Wrong date format during comparison: " + e.getMessage());
            }
        } else {
            logger.error("Unsupported type for comparison: Value type = {}, PageValue type = {}", Value.getClass().getName(), pageValue.getClass().getName());
            throw new DBAppException("Unsupported data type for comparison: " + Value.getClass().getName());
        }
        return r;
    }

    private static void Writecsv(String strTableName, Hashtable<String, String> htblColNameType, String strClusteringKeyColumn, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
        logger.info("Attempting to write/append metadata to {} for table '{}'. Clustering Key: '{}'. Columns: {}", DBConstants.METADATA_FILE_PATH, strTableName, strClusteringKeyColumn, htblColNameType.keySet());
        String csvFilePath = DBConstants.METADATA_FILE_PATH;
        boolean tableExists = false;

        try {
            File metaDir = new File(DBConstants.META_DIR);
            if (!metaDir.exists()) {
                if (metaDir.mkdirs()) {
                    logger.info("Created directory: {}", metaDir.getAbsolutePath());
                } else {
                    logger.error("Failed to create directory: {}", metaDir.getAbsolutePath());
                    throw new DBAppException("Failed to create metadata directory: " + metaDir.getAbsolutePath());
                }
            } else if (!metaDir.isDirectory()) {
                logger.error("Path 'meta' exists but is not a directory.");
                throw new DBAppException("'meta' is not a directory.");
            }

            ArrayList<String[]> currentMetadata = loopCSV();
            for (String[] row : currentMetadata) {
                if (row.length > 0 && row[0].equals(strTableName)) {
                    tableExists = true;
                    break;
                }
            }
        } catch (DBAppException e) {
            logger.warn("DBAppException while pre-checking metadata in Writecsv for table '{}': {}. This might be ignorable if the CSV file doesn't exist yet or is empty.", strTableName, e.getMessage());
        }


        if (tableExists) {
            logger.error("Table name '{}' already exists in {}. Cannot add new table metadata.", strTableName, csvFilePath);
            throw new DBAppException("Table name '" + strTableName + "' already exists in metadata.");
        } else {
            logger.debug("Table name '{}' does not exist in CSV or CSV is new/empty. Proceeding to append new metadata.", strTableName);

            try (FileWriter fileWriter = new FileWriter(csvFilePath, true)) {
                Enumeration<String> colNames = htblColNameType.keys();
                while (colNames.hasMoreElements()) {
                    String colName = colNames.nextElement();
                    logger.trace("Appending metadata for column: {}", colName);

                    fileWriter.append(strTableName).append(",");
                    fileWriter.append(colName).append(",");

                    String colType = htblColNameType.get(colName);
                    switch (colType) {
                        case DBConstants.JAVA_LANG_INTEGER, DBConstants.JAVA_LANG_STRING, DBConstants.JAVA_LANG_DOUBLE, DBConstants.JAVA_UTIL_DATE:
                            fileWriter.append(colType).append(",");
                            break;
                        default:
                            logger.error("Invalid column type '{}' for column '{}' in table '{}'.", colType, colName, strTableName);
                            throw new DBAppException("Wrong data type format for column " + colName + ": " + colType);
                    }

                    fileWriter.append(Boolean.toString(colName.equals(strClusteringKeyColumn))).append(",");
                    fileWriter.append(DBConstants.CSV_NULL_STRING).append(",");
                    fileWriter.append(DBConstants.CSV_NULL_STRING).append(",");

                    String minVal = htblColNameMin.get(colName);
                    String maxVal = htblColNameMax.get(colName);

                    if (DBConstants.JAVA_UTIL_DATE.equals(colType)) {
                        try {
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DBConstants.DATE_FORMAT_YYYY_MM_DD);
                            simpleDateFormat.setLenient(false);
                            simpleDateFormat.parse(minVal);
                            simpleDateFormat.parse(maxVal);
                            logger.trace("Date formats for min/max validated for column {}", colName);
                        } catch (ParseException e) {
                            logger.error("Wrong date format for min ('{}') or max ('{}') for date column '{}' in table '{}'.", minVal, maxVal, colName, strTableName, e);
                            throw new DBAppException("Wrong date format for min/max values of date column " + colName + ": " + e.getMessage());
                        }
                    }
                    fileWriter.append(minVal).append(",");
                    fileWriter.append(maxVal);
                    fileWriter.append('\n');
                    logger.trace("Appended line for column {} to {}.", colName, csvFilePath);
                }
                logger.info("Successfully appended metadata for table '{}' to {}.", strTableName, csvFilePath);
            } catch (IOException e) {
                logger.error("IOException while appending to metadata file {}: {}", csvFilePath, e.getMessage(), e);
                throw new DBAppException("Cannot write to metadata file " + csvFilePath + ": " + e.getMessage());
            }
        }
    }

    public static ArrayList<String[]> loopCSV() throws DBAppException {
        String csvFilePath = DBConstants.METADATA_FILE_PATH;
        logger.debug("Attempting to read and parse CSV metadata from: {}", csvFilePath);
        ArrayList<String[]> tableEntries = new ArrayList<>();

        File csvFile = new File(csvFilePath);
        if (!csvFile.exists()) {
            logger.warn("Metadata file {} does not exist. Returning empty list.", csvFilePath);
            return tableEntries;
        }
        if (csvFile.length() == 0 && csvFile.isFile()) {
            logger.info("Metadata file {} is empty. Returning empty list.", csvFilePath);
            return tableEntries;
        }

        try (FileReader fileReader = new FileReader(csvFile);
             BufferedReader br = new BufferedReader(fileReader)) {
            String line;
            int lineNum = 0;
            while ((line = br.readLine()) != null) {
                lineNum++;
                if (line.trim().isEmpty()) {
                    logger.trace("Skipping empty line {} in {}", lineNum, csvFilePath);
                    continue;
                }
                String[] r = line.split(",", -1);
                if (r.length != 8) {
                     logger.warn("Malformed line {} in {}: Expected 8 columns, found {}. Line: '{}'", lineNum, csvFilePath, r.length, line);
                }
                tableEntries.add(r);
                logger.trace("Parsed line {} from {}: {}", lineNum, csvFilePath, Arrays.toString(r));
            }
            logger.info("Successfully read and parsed {} data lines from {}.", tableEntries.size(), csvFilePath);
            return tableEntries;
        } catch (FileNotFoundException e) {
            logger.warn("Metadata file {} not found during read (should have been caught by exists check). Returning empty list.", csvFilePath, e);
            return tableEntries;
        } catch (IOException e) {
            logger.error("IOException while reading metadata file {}: {}", csvFilePath, e.getMessage(), e);
            throw new DBAppException("Cannot read metadata file " + csvFilePath + ": " + e.getMessage());
        }
    }

    public static Table ReadTable(String tableName) throws DBAppException {
        String filePath = DBConstants.META_DIR + tableName + DBConstants.DATA_FILE_EXTENSION;
        logger.debug("Attempting to deserialize Table object from: {}", filePath);
        try (FileInputStream fileInputStream = new FileInputStream(filePath);
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            Table table = (Table) objectInputStream.readObject();
            logger.info("Successfully deserialized Table '{}' from {}", tableName, filePath);
            return table;
        } catch (FileNotFoundException e) {
            logger.error("File not found for table deserialization: {}", filePath, e);
            throw new DBAppException("Unable to deserialize table '" + tableName + "': File not found at " + filePath);
        } catch (IOException e) {
            logger.error("IOException during table deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to deserialize table '" + tableName + "' due to IOException: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException during table deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to deserialize table '" + tableName + "': Class not found - " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected exception during table deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("An unexpected error occurred while deserializing table '" + tableName + "': " + e.getMessage());
        }
    }

    public static void WriteTable(String tableName, Table t) throws DBAppException {
        String filePath = DBConstants.META_DIR + tableName + DBConstants.DATA_FILE_EXTENSION;
        logger.debug("Attempting to serialize Table object '{}' to: {}", tableName, filePath);
        File file = new File(filePath);
        try {
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                if (parentDir.mkdirs()) {
                    logger.info("Created parent directory for table file: {}", parentDir.getAbsolutePath());
                } else {
                    logger.error("Failed to create parent directory for table file: {}", parentDir.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            logger.error("Exception while ensuring parent directory for {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Error preparing file path for table '" + tableName + "': " + e.getMessage());
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(file);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(t);
            logger.info("Successfully serialized Table '{}' to {}", tableName, filePath);
        } catch (IOException e) {
            logger.error("IOException during table serialization to {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to serialize table '" + tableName + "' due to IOException: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected exception during table serialization to {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("An unexpected error occurred while serializing table '" + tableName + "': " + e.getMessage());
        }
    }

    public static Page ReadPage(int id, String strTableName) throws DBAppException {
        String filePath = DBConstants.META_DIR + strTableName + id + DBConstants.DATA_FILE_EXTENSION;
        logger.debug("Attempting to deserialize Page object: {}", filePath);
        try (FileInputStream fileInputStream = new FileInputStream(filePath);
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            Page page = (Page) objectInputStream.readObject();
            logger.info("Successfully deserialized Page {} for table '{}' from {}", id, strTableName, filePath);
            return page;
        } catch (FileNotFoundException e) {
            logger.error("File not found for page deserialization: {}", filePath, e);
            throw new DBAppException("Unable to deserialize page " + id + " for table '" + strTableName + "': File not found at " + filePath);
        } catch (IOException e) {
            logger.error("IOException during page deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to deserialize page " + id + " for table '" + strTableName + "' due to IOException: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException during page deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to deserialize page " + id + " for table '" + strTableName + "': Class not found - " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected exception during page deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("An unexpected error occurred while deserializing page " + id + " for table '" + strTableName + "': " + e.getMessage());
        }
    }

    public static void WritePage(int id, String strTableName, Page page) throws DBAppException {
        String filePath = DBConstants.META_DIR + strTableName + id + DBConstants.DATA_FILE_EXTENSION;
        logger.debug("Attempting to serialize Page {} for table '{}' to: {}", id, strTableName, filePath);
        File file = new File(filePath);
        try {
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                if (parentDir.mkdirs()) {
                    logger.info("Created parent directory for page file: {}", parentDir.getAbsolutePath());
                } else {
                    logger.warn("Failed to create parent directory for page file: {}", parentDir.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            logger.error("Exception while ensuring parent directory for {}: {}", filePath, e.getMessage(), e);
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(file);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(page);
            logger.info("Successfully serialized Page {} for table '{}' to {}", id, strTableName, filePath);
        } catch (IOException e) {
            logger.error("IOException during page serialization to {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to serialize page " + id + " for table '" + strTableName + "' due to IOException: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected exception during page serialization to {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("An unexpected error occurred while serializing page " + id + " for table '" + strTableName + "': " + e.getMessage());
        }
    }

    private static String checkStringType(String strClusteringKeyValue) {
        logger.trace("Checking string type for value: '{}'", strClusteringKeyValue);
        String r = DBConstants.JAVA_LANG_STRING;
        SimpleDateFormat dateFormat = new SimpleDateFormat(DBConstants.DATE_FORMAT_YYYY_MM_DD);
        dateFormat.setLenient(false);
        try {
            dateFormat.parse(strClusteringKeyValue);
            r = DBConstants.JAVA_UTIL_DATE;
        } catch (ParseException e) {
            try {
                Integer.parseInt(strClusteringKeyValue);
                r = DBConstants.JAVA_LANG_INTEGER;
            } catch (NumberFormatException e1) {
                try {
                    Double.parseDouble(strClusteringKeyValue);
                    r = DBConstants.JAVA_LANG_DOUBLE;
                } catch (NumberFormatException e2) {
                }
            }
        }
        logger.trace("String type for '{}' determined as: {}", strClusteringKeyValue, r);
        return r;
    }

    private static void checkDateFormat(String Date) throws DBAppException {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(DBConstants.DATE_FORMAT_YYYY_MM_DD);
            dateFormat.setLenient(false);
            dateFormat.parse(Date);
        } catch (ParseException e) {
            logger.warn("Date format check failed for date string: '{}'", Date, e);
            throw new DBAppException("The date is not in the correct format yyyy-MM-dd: " + Date);
        }
    }

    private static String getPrimKeyType(String strTableName) throws DBAppException {
        String type = DBConstants.DEFAULT_PK_TYPE;
        ArrayList<String[]> TablesName = loopCSV();
        for (String[] line : TablesName) {
            if (line.length > 3 && line[0].equals(strTableName)) {
                if (Boolean.parseBoolean(line[3])) {
                     type = line[2];
                     break;
                }
            }
        }
        if(DBConstants.DEFAULT_PK_TYPE.equals(type)){
            logger.warn("Primary key type not found for table '{}', returning 'default'.", strTableName);
        }
        return type;
    }

    private static String getPrimKey(String strTableName) throws DBAppException {
        String prim = "";
        ArrayList<String[]> TablesName = loopCSV();
        for (String[] line : TablesName) {
             if (line.length > 3 && line[0].equals(strTableName)) {
                if (Boolean.parseBoolean(line[3])) {
                    prim = line[1];
                    break;
                }
            }
        }
         if(prim.isEmpty()){
            logger.warn("Primary key column name not found for table '{}'.", strTableName);
        }
        return prim;
    }

    private static void checkColumns(ArrayList<String[]> tablesName, String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        ArrayList<String> columnsInTable = new ArrayList<>();
        for (String[] metadataEntry : tablesName) {
            if (metadataEntry.length > 1 && metadataEntry[0].equals(strTableName)) {
                columnsInTable.add(metadataEntry[1]);
            }
        }
        if(columnsInTable.isEmpty()){
            logger.error("No metadata found for table '{}' during column check.", strTableName);
            throw new DBAppException("Table '" + strTableName + "' not found in metadata.");
        }

        Set<String> columnsInHashtable = htblColNameValue.keySet();
        for (String s : columnsInHashtable) {
            if (!columnsInTable.contains(s)) {
                logger.error("Column '{}' in input Hashtable does not exist in table '{}'. Existing columns: {}", s, strTableName, columnsInTable);
                throw new DBAppException("Column '" + s + "' does not exist in table '" + strTableName + "'.");
            }
        }
    }

    private static Object convertToDataType(String value) throws DBAppException {
        if (value == null || DBConstants.CSV_NULL_STRING.equalsIgnoreCase(value.trim())) {
            return null;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(DBConstants.DATE_FORMAT_YYYY_MM_DD);
        dateFormat.setLenient(false);
        Object obj;
        String type = checkStringType(value);
        logger.trace("Converting value '{}' to inferred type '{}'", value, type);
        switch (type) {
            case DBConstants.JAVA_LANG_INTEGER -> obj = Integer.parseInt(value);
            case DBConstants.JAVA_LANG_DOUBLE -> obj = Double.parseDouble(value);
            case DBConstants.JAVA_UTIL_DATE -> {
                try {
                    obj = dateFormat.parse(value);
                } catch (ParseException e) {
                    logger.error("ParseException while converting value '{}' to Date.", value, e);
                    throw new DBAppException("Failed to parse date string '" + value + "': " + e.getMessage());
                }
            }
            default -> obj = value;
        }
        logger.trace("Converted value '{}' to object: {} (Actual type: {})", value, obj, (obj != null ? obj.getClass().getName() : "null"));
        return obj;
    }

    public static void insertIntoIndex(String strTableName, String indexName) throws DBAppException {
        logger.info("Populating index '{}' for table '{}'.", indexName, strTableName);
        Table table = ReadTable(strTableName);
        Octree tree = ReadIndex(indexName, strTableName);

        int tuplesProcessed = 0;
        for (int i = 0; i < table.noOfPages; i++) {
            Page p = ReadPage(i, strTableName);
            for (int j = 0; j < p.getTuples().size(); j++) {
                Hashtable<String, Object> tuple = p.getTuples().get(j);
                Object x = tuple.get(tree.getCol1());
                Object y = tuple.get(tree.getCol2());
                Object z = tuple.get(tree.getCol3());

                if (x == null || y == null || z == null) {
                    logger.warn("Tuple in table '{}', page {}, at internal index {} has null value for one or more indexed columns ({}, {}, {}). Skipping for index '{}'. Values: X={}, Y={}, Z={}",
                                strTableName, i, j, tree.getCol1(), tree.getCol2(), tree.getCol3(), indexName, x, y, z);
                    continue;
                }

                logger.trace("Parsing into Octree for index '{}': X={}, Y={}, Z={}, PageRef={}", indexName, x,y,z,i);
                tree = parseOctree(x, y, z, tree, i);
                tuplesProcessed++;
            }
        }
        WriteIndex(indexName, strTableName, tree);
        logger.info("Finished populating index '{}' for table '{}'. Processed {} tuples.", indexName, strTableName, tuplesProcessed);
    }

    public static int determineOctreeIndex(Object x, Object y, Object z, Object midX, Object midY, Object midZ) throws DBAppException {
        int i;
        if (compareToObject(x, midX) < 0) {
            if (compareToObject(y, midY) < 0) {
                i = (compareToObject(z, midZ) < 0) ? 0 : 1;
            } else {
                i = (compareToObject(z, midZ) < 0) ? 2 : 3;
            }
        } else {
            if (compareToObject(y, midY) < 0) {
                i = (compareToObject(z, midZ) < 0) ? 4 : 5;
            } else {
                i = (compareToObject(z, midZ) < 0) ? 6 : 7;
            }
        }
        return i;
    }

    public static Object findMedianValue(Object min, Object max) throws DBAppException {
        logger.trace("Finding median value between min='{}' (type: {}) and max='{}' (type: {})",
            min, (min != null ? min.getClass().getName() : "null"),
            max, (max != null ? max.getClass().getName() : "null"));

        Object Result = null;
        Object minValue = convertToDataType(String.valueOf(min));
        Object maxValue = convertToDataType(String.valueOf(max));

        if (maxValue instanceof String && minValue instanceof String) {
            String s = ((String) minValue).toLowerCase();
            String t = ((String) maxValue).toLowerCase();

            if (s.compareTo(t) > 0) {
                String temp = s; s = t; t = temp;
            }

            int N = Math.min(s.length(), t.length());
            StringBuilder midString = new StringBuilder();

            for(int k=0; k<N; k++){
                midString.append((char)(((s.charAt(k) + t.charAt(k))/2)));
            }

            if (s.length() < t.length() && N == s.length()) {
                 for(int k=N; k < t.length(); k++) {
                    midString.append((char)(t.charAt(k)/2 + 'a'/2));
                 }
            } else if (t.length() < s.length() && N == t.length()) {
                 for(int k=N; k < s.length(); k++) {
                    midString.append((char)(s.charAt(k)/2 + 'a'/2));
                 }
            }
            Result = midString.toString();

        } else if (maxValue instanceof Date && minValue instanceof Date) {
            Result = new Date((((Date) minValue).getTime() + ((Date) maxValue).getTime()) / 2);
        } else if (maxValue instanceof Integer && minValue instanceof Integer) {
            Result = (((Integer) minValue + (Integer) maxValue) / 2);
        } else if (maxValue instanceof Double && minValue instanceof Double) {
            Result = (((Double) minValue + (Double) maxValue) / 2.0);
        } else {
             logger.error("Unsupported types for median calculation: min={}, max={}",
                (minValue != null ? minValue.getClass().getName() : "null"),
                (maxValue != null ? maxValue.getClass().getName() : "null"));
            throw new DBAppException("Cannot find median for unsupported types or type mismatch.");
        }
        logger.trace("Median calculated: {}", Result);
        return Result;
    }

    public static Object[] findMinandMax(Octree tree, int i) throws DBAppException {
        Object[] r = new Object[6];
        Object midX = findMedianValue(tree.getMinX(), tree.getMaxX());
        Object midY = findMedianValue(tree.getMinY(), tree.getMaxY());
        Object midZ = findMedianValue(tree.getMinZ(), tree.getMaxZ());

        switch (i) {
            case 0: r[0] = tree.getMinX(); r[1] = tree.getMinY(); r[2] = tree.getMinZ(); r[3] = midX; r[4] = midY; r[5] = midZ; break;
            case 1: r[0] = tree.getMinX(); r[1] = tree.getMinY(); r[2] = midZ; r[3] = midX; r[4] = midY; r[5] = tree.getMaxZ(); break;
            case 2: r[0] = tree.getMinX(); r[1] = midY; r[2] = tree.getMinZ(); r[3] = midX; r[4] = tree.getMaxY(); r[5] = midZ; break;
            case 3: r[0] = tree.getMinX(); r[1] = midY; r[2] = midZ; r[3] = midX; r[4] = tree.getMaxY(); r[5] = tree.getMaxZ(); break;
            case 4: r[0] = midX; r[1] = tree.getMinY(); r[2] = tree.getMinZ(); r[3] = tree.getMaxX(); r[4] = midY; r[5] = midZ; break;
            case 5: r[0] = midX; r[1] = tree.getMinY(); r[2] = midZ; r[3] = tree.getMaxX(); r[4] = midY; r[5] = tree.getMaxZ(); break;
            case 6: r[0] = midX; r[1] = midY; r[2] = tree.getMinZ(); r[3] = tree.getMaxX(); r[4] = tree.getMaxY(); r[5] = midZ; break;
            case 7: r[0] = midX; r[1] = midY; r[2] = midZ; r[3] = tree.getMaxX(); r[4] = tree.getMaxY(); r[5] = tree.getMaxZ(); break;
        }
        return r;
    }

    public static boolean checkDup(Point p, Node r) throws DBAppException {
        Object x = p.getX();
        Object y = p.getY();
        Object z = p.getZ();
        for (int i = 0; i < r.getPoints().size(); i++) {
            Point existingPoint = r.getPoints().get(i);
            if (compareToObject(existingPoint.getX(), x) == 0 &&
                compareToObject(existingPoint.getY(), y) == 0 &&
                compareToObject(existingPoint.getZ(), z) == 0)
                return true;
        }
        return false;
    }

    public static ArrayList<Integer> findIndexNull(Object x, Object y, Object z, Object midX, Object midY, Object midZ) throws DBAppException {
        ArrayList<Integer> index = new ArrayList<>();
        logger.trace("findIndexNull called with X={}, Y={}, Z={}, midX={}, midY={}, midZ={}", x,y,z,midX,midY,midZ);

        if (x == null && y != null && z != null) {
            if (compareToObject(y, midY) < 0) {
                index.add((compareToObject(z, midZ) < 0) ? 0 : 1);
                index.add((compareToObject(z, midZ) < 0) ? 4 : 5);
            } else {
                index.add((compareToObject(z, midZ) < 0) ? 2 : 3);
                index.add((compareToObject(z, midZ) < 0) ? 6 : 7);
            }
        } else if (y == null && x != null && z != null) {
            if (compareToObject(x, midX) < 0) {
                index.add((compareToObject(z, midZ) < 0) ? 0 : 1);
                index.add((compareToObject(z, midZ) < 0) ? 2 : 3);
            } else {
                index.add((compareToObject(z, midZ) < 0) ? 4 : 5);
                index.add((compareToObject(z, midZ) < 0) ? 6 : 7);
            }
        } else if (z == null && y != null && x != null) {
            if (compareToObject(x, midX) < 0) {
                index.add((compareToObject(y, midY) < 0) ? 0 : 2);
                index.add((compareToObject(y, midY) < 0) ? 1 : 3);
            } else {
                index.add((compareToObject(y, midY) < 0) ? 4 : 6);
                index.add((compareToObject(y, midY) < 0) ? 5 : 7);
            }
        } else if (x != null && y == null && z == null) {
            index.addAll(Arrays.asList(0,1,2,3,4,5,6,7));
            index.removeIf(idx -> (compareToObject(x, midX) < 0 && idx > 3));
            index.removeIf(idx -> (compareToObject(x, midX) >= 0 && idx < 4));
        } else if (y != null && x == null && z == null) {
            index.addAll(Arrays.asList(0,1,2,3,4,5,6,7));
            index.removeIf(idx -> (compareToObject(y, midY) < 0 && (idx == 2 || idx == 3 || idx == 6 || idx == 7) ));
            index.removeIf(idx -> (compareToObject(y, midY) >= 0 && (idx == 0 || idx == 1 || idx == 4 || idx == 5) ));
        } else if (z != null && x == null && y == null) {
            index.addAll(Arrays.asList(0,1,2,3,4,5,6,7));
            index.removeIf(idx -> (compareToObject(z, midZ) < 0 && idx % 2 != 0 ));
            index.removeIf(idx -> (compareToObject(z, midZ) >= 0 && idx % 2 == 0 ));
        } else if (x == null && y == null && z == null) {
            logger.error("All dimensions are null in findIndexNull. This should not happen for a valid point insertion/search in Octree.");
            throw new DBAppException("All dimensions are null, cannot determine Octree index.");
        } else {
            index.add(determineOctreeIndex(x, y, z, midX, midY, midZ));
        }
        logger.trace("findIndexNull determined indices: {}", index);
        return index;
    }

    public static ArrayList<Integer> searchOctree(Point p, Octree octree, ArrayList<Integer> reff) throws DBAppException {
        logger.trace("Searching Octree. Point: {}, Current Octree Min:({},{},{}), Max:({},{},{}). Refs found so far: {}",
            p, octree.getMinX(), octree.getMinY(), octree.getMinZ(), octree.getMaxX(), octree.getMaxY(), octree.getMaxZ(), reff.size());

        if (octree.getRoot() == null || (octree.getRoot().getPoints().isEmpty() && octree.getRoot().getDuplicates().isEmpty() && !octree.isParent())) {
            logger.trace("Octree node is empty or leaf without points/duplicates. Returning current refs.");
            return reff;
        }

        if (octree.isParent()) {
            logger.trace("Octree node is a parent. Finding child indices to traverse.");
            ArrayList<Integer> childIndices = findIndexNull(p.getX(), p.getY(), p.getZ(),
                                                            findMedianValue(octree.getMinX(), octree.getMaxX()),
                                                            findMedianValue(octree.getMinY(), octree.getMaxY()),
                                                            findMedianValue(octree.getMinZ(), octree.getMaxZ()));
            logger.trace("Child indices to search: {}", childIndices);
            for (int childIdx : childIndices) {
                if(octree.getChildNodes()[childIdx] != null){
                    searchOctree(p, octree.getChildNodes()[childIdx], reff);
                } else {
                    logger.trace("Child node at index {} is null. Skipping.", childIdx);
                }
            }
        } else {
            logger.trace("Octree node is a leaf. Checking points and duplicates.");
            for (Point pointInNode : octree.getRoot().getPoints()) {
                boolean match = true;
                if (p.getX() != null && !p.getX().equals(pointInNode.getX())) match = false;
                if (match && p.getY() != null && !p.getY().equals(pointInNode.getY())) match = false;
                if (match && p.getZ() != null && !p.getZ().equals(pointInNode.getZ())) match = false;

                if (match) {
                    if(!reff.contains(pointInNode.getRef())) {
                       reff.add(pointInNode.getRef());
                       logger.trace("Found matching point's ref: {}. Added to results.", pointInNode.getRef());
                    }
                }
            }
            for (Point duplicatePoint : octree.getRoot().getDuplicates()) {
                 boolean match = true;
                if (p.getX() != null && !p.getX().equals(duplicatePoint.getX())) match = false;
                if (match && p.getY() != null && !p.getY().equals(duplicatePoint.getY())) match = false;
                if (match && p.getZ() != null && !p.getZ().equals(duplicatePoint.getZ())) match = false;

                if (match) {
                     if(!reff.contains(duplicatePoint.getRef())) {
                        reff.add(duplicatePoint.getRef());
                        logger.trace("Found matching duplicate point's ref: {}. Added to results.", duplicatePoint.getRef());
                     }
                }
            }
        }
        logger.trace("Returning from searchOctree. Total refs: {}", reff.size());
        return reff;
    }

    public static Octree deleteOctree(Point p, Octree octree) throws DBAppException {
        logger.trace("Attempting to delete point {} from Octree node. Current node min:({},{},{}), max:({},{},{})",
                    p, octree.getMinX(), octree.getMinY(), octree.getMinZ(), octree.getMaxX(), octree.getMaxY(), octree.getMaxZ());

        if (octree.getRoot() == null || (octree.getRoot().getPoints().isEmpty() && octree.getRoot().getDuplicates().isEmpty() && !octree.isParent())) {
            logger.trace("Deletion target Octree node is empty or leaf without points/duplicates. No action.");
            return octree;
        }

        if (octree.isParent()) {
            logger.trace("Deletion target is a parent node. Finding child indices for point {}.", p);
            ArrayList<Integer> childIndices = findIndexNull(p.getX(), p.getY(), p.getZ(),
                                                            findMedianValue(octree.getMinX(), octree.getMaxX()),
                                                            findMedianValue(octree.getMinY(), octree.getMaxY()),
                                                            findMedianValue(octree.getMinZ(), octree.getMaxZ()));
            logger.trace("Child indices for deletion: {}", childIndices);
            for (int childIdx : childIndices) {
                 if(octree.getChildNodes()[childIdx] != null){
                    octree.getChildNodes()[childIdx] = deleteOctree(p, octree.getChildNodes()[childIdx]);
                } else {
                     logger.trace("Child node at index {} for deletion is null. Skipping.", childIdx);
                }
            }
        } else {
            logger.trace("Deletion target is a leaf node. Searching for point {} with ref {}.", p, p.getRef());
            boolean removedFromPoints = false;
            Iterator<Point> pointIterator = octree.getRoot().getPoints().iterator();
            while(pointIterator.hasNext()){
                Point currentPoint = pointIterator.next();
                boolean xMatch = (p.getX() == null || (currentPoint.getX() != null && currentPoint.getX().equals(p.getX())));
                boolean yMatch = (p.getY() == null || (currentPoint.getY() != null && currentPoint.getY().equals(p.getY())));
                boolean zMatch = (p.getZ() == null || (currentPoint.getZ() != null && currentPoint.getZ().equals(p.getZ())));
                boolean refMatch = (p.getRef() == -1 || currentPoint.getRef() == p.getRef());

                if(xMatch && yMatch && zMatch && refMatch){
                    logger.debug("Found and removed point {} from node's points list.", currentPoint);
                    pointIterator.remove();
                    removedFromPoints = true;
                    Iterator<Point> dupIterator = octree.getRoot().getDuplicates().iterator();
                    while(dupIterator.hasNext()){
                        Point dup = dupIterator.next();
                        if( (currentPoint.getX() != null && currentPoint.getX().equals(dup.getX())) &&
                            (currentPoint.getY() != null && currentPoint.getY().equals(dup.getY())) &&
                            (currentPoint.getZ() != null && currentPoint.getZ().equals(dup.getZ())) ) {
                            octree.getRoot().getPoints().add(dup);
                            dupIterator.remove();
                            logger.debug("Promoted duplicate point {} to main points list.", dup);
                            break;
                        }
                    }
                    break;
                }
            }

            if (!removedFromPoints) {
                 if(p.getRef() != -1) {
                    Iterator<Point> dupIterator = octree.getRoot().getDuplicates().iterator();
                    while(dupIterator.hasNext()){
                        Point dup = dupIterator.next();
                        boolean xMatch = (p.getX() == null || (dup.getX() != null && dup.getX().equals(p.getX())));
                        boolean yMatch = (p.getY() == null || (dup.getY() != null && dup.getY().equals(p.getY())));
                        boolean zMatch = (p.getZ() == null || (dup.getZ() != null && dup.getZ().equals(p.getZ())));
                        boolean refMatch = (dup.getRef() == p.getRef());

                        if(xMatch && yMatch && zMatch && refMatch){
                            logger.debug("Found and removed point {} from node's duplicates list.", dup);
                            dupIterator.remove();
                            break;
                        }
                    }
                 }
            }
        }
        return octree;
    }

    public static void updatePointRef(Point oldPoint, Octree octree, int newRef, String indexName, String strTableName) throws DBAppException {
        logger.trace("Updating point ref in Octree. OldPoint: {}, NewRef: {}, Index: {}, Table: {}", oldPoint, newRef, indexName, strTableName);

        if (octree.getRoot() == null || (octree.getRoot().getPoints().isEmpty() && octree.getRoot().getDuplicates().isEmpty() && !octree.isParent())) {
            logger.trace("UpdatePointRef: Octree node is empty. Cannot update point.");
            return;
        }

        if (octree.isParent()) {
            logger.trace("UpdatePointRef: Parent node. Determining child index for oldPoint: {}", oldPoint);
            if(oldPoint.getX() == null || oldPoint.getY() == null || oldPoint.getZ() == null) {
                 logger.warn("UpdatePointRef called with null dimension in oldPoint {}. Cannot reliably find in parent node. Aborting update for this path.", oldPoint);
                 return;
            }
            int childIdx = determineOctreeIndex(oldPoint.getX(), oldPoint.getY(), oldPoint.getZ(),
                                                findMedianValue(octree.getMinX(), octree.getMaxX()),
                                                findMedianValue(octree.getMinY(), octree.getMaxY()),
                                                findMedianValue(octree.getMinZ(), octree.getMaxZ()));
            logger.trace("UpdatePointRef: Traversing to child index {}.", childIdx);
            if(octree.getChildNodes()[childIdx] != null) {
                updatePointRef(oldPoint, octree.getChildNodes()[childIdx], newRef, indexName, strTableName);
            } else {
                 logger.trace("UpdatePointRef: Child node at index {} is null. Point not found in this path.", childIdx);
            }
        } else {
            logger.trace("UpdatePointRef: Leaf node. Searching for oldPoint: {} with oldRef: {}", oldPoint, oldPoint.getRef());
            boolean updated = false;
            for (Point currentPoint : octree.getRoot().getPoints()) {
                if (currentPoint.getX().equals(oldPoint.getX()) &&
                    currentPoint.getY().equals(oldPoint.getY()) &&
                    currentPoint.getZ().equals(oldPoint.getZ()) &&
                    currentPoint.getRef() == oldPoint.getRef()) {
                    logger.debug("Found point in main list. Updating ref from {} to {}. Point: {}", currentPoint.getRef(), newRef, currentPoint);
                    currentPoint.setRef(newRef);
                    updated = true;
                    break;
                }
            }
            if (!updated) {
                for (Point currentPoint : octree.getRoot().getDuplicates()) {
                    if (currentPoint.getX().equals(oldPoint.getX()) &&
                        currentPoint.getY().equals(oldPoint.getY()) &&
                        currentPoint.getZ().equals(oldPoint.getZ()) &&
                        currentPoint.getRef() == oldPoint.getRef()) {
                        logger.debug("Found point in duplicates list. Updating ref from {} to {}. Point: {}", currentPoint.getRef(), newRef, currentPoint);
                        currentPoint.setRef(newRef);
                        updated = true;
                        break;
                    }
                }
            }
            if(updated){
                 logger.info("Point ref updated successfully in index '{}' for table '{}'. Old ref: {}, New ref: {}", indexName, strTableName, oldPoint.getRef(), newRef);
                 WriteIndex(indexName, strTableName, octree);
            } else {
                 logger.warn("Point {} with ref {} not found in leaf node for ref update.", oldPoint, oldPoint.getRef());
            }
        }
    }

    public static Octree parseOctree(Object x, Object y, Object z, Octree tree, int PageIndex) throws DBAppException {
        if (x == null || y == null || z == null) {
            logger.error("Attempted to parse Octree with null dimension(s): X={}, Y={}, Z={}. This is typically not allowed for point insertion.", x,y,z);
            throw new DBAppException("Cannot insert point with null dimensions into Octree.");
        }
        logger.trace("Parsing into Octree: X={}, Y={}, Z={}, PageIndex={}, CurrentTreeDepth/State (approx): MaxPoints={}, IsParent={}",
            x,y,z,PageIndex, (tree.getRoot() != null ? tree.getRoot().getMaxPoints() : "N/A"), tree.isParent());

        if (tree.getRoot() == null) {
            logger.trace("Tree root is null. Creating new root node.");
            tree.setRoot(new Node(x, y, z, PageIndex));
            return tree;
        }

        Node currentNode = tree.getRoot();
        if (!tree.isParent()) {
            if (currentNode.getPoints().size() < currentNode.getMaxPoints()) {
                Point p = new Point(x, y, z, PageIndex);
                if (checkDup(p, currentNode)) {
                    logger.trace("Duplicate point detected. Adding to duplicates list: {}", p);
                    currentNode.getDuplicates().add(p);
                } else {
                    logger.trace("Adding point to leaf node: {}", p);
                    currentNode.getPoints().add(p);
                }
                return tree;
            } else {
                logger.trace("Leaf node is full ({} points). Splitting node.", currentNode.getPoints().size());
                tree.setParent(true);
                tree.setChildNodes(new Octree[8]);

                Object midX = findMedianValue(tree.getMinX(), tree.getMaxX());
                Object midY = findMedianValue(tree.getMinY(), tree.getMaxY());
                Object midZ = findMedianValue(tree.getMinZ(), tree.getMaxZ());
                logger.trace("Calculated midpoints for split: midX={}, midY={}, midZ={}", midX, midY, midZ);

                for (int j = 0; j < 8; j++) {
                    Object[] minsAndMaxs = findMinandMax(tree, j);
                    tree.getChildNodes()[j] = new Octree();
                    tree.getChildNodes()[j].setMinX(minsAndMaxs[0]); tree.getChildNodes()[j].setMinY(minsAndMaxs[1]); tree.getChildNodes()[j].setMinZ(minsAndMaxs[2]);
                    tree.getChildNodes()[j].setMaxX(minsAndMaxs[3]); tree.getChildNodes()[j].setMaxY(minsAndMaxs[4]); tree.getChildNodes()[j].setMaxZ(minsAndMaxs[5]);
                     logger.trace("Initialized child Octree {} with bounds: Min=({},{},{}), Max=({},{},{})",
                                j, minsAndMaxs[0],minsAndMaxs[1],minsAndMaxs[2], minsAndMaxs[3],minsAndMaxs[4],minsAndMaxs[5]);
                }

                ArrayList<Point> allPointsToRedistribute = new ArrayList<>(currentNode.getPoints());
                allPointsToRedistribute.addAll(currentNode.getDuplicates());

                currentNode.getPoints().clear();
                currentNode.getDuplicates().clear();

                for (Point existingPoint : allPointsToRedistribute) {
                    logger.trace("Redistributing existing point: {}", existingPoint);
                    int childIndex = determineOctreeIndex(existingPoint.getX(), existingPoint.getY(), existingPoint.getZ(), midX, midY, midZ);
                    tree.getChildNodes()[childIndex] = parseOctree(existingPoint.getX(), existingPoint.getY(), existingPoint.getZ(), tree.getChildNodes()[childIndex], existingPoint.getRef());
                }

                logger.trace("Inserting new point {} after split.", new Point(x,y,z,PageIndex));
                int newPointChildIndex = determineOctreeIndex(x, y, z, midX, midY, midZ);
                tree.getChildNodes()[newPointChildIndex] = parseOctree(x, y, z, tree.getChildNodes()[newPointChildIndex], PageIndex);

                return tree;
            }
        } else {
            logger.trace("Node is already a parent. Determining child index for new point.");
            Object midX = findMedianValue(tree.getMinX(), tree.getMaxX());
            Object midY = findMedianValue(tree.getMinY(), tree.getMaxY());
            Object midZ = findMedianValue(tree.getMinZ(), tree.getMaxZ());
            int childIndex = determineOctreeIndex(x, y, z, midX, midY, midZ);
            logger.trace("Traversing to child index {} for new point.", childIndex);

            if(tree.getChildNodes()[childIndex] == null) {
                 logger.warn("Parent node's child at index {} is null. Initializing it before parsing.", childIndex);
                 Object[] minsAndMaxs = findMinandMax(tree, childIndex);
                 tree.getChildNodes()[childIndex] = new Octree();
                 tree.getChildNodes()[childIndex].setMinX(minsAndMaxs[0]); tree.getChildNodes()[childIndex].setMinY(minsAndMaxs[1]); tree.getChildNodes()[childIndex].setMinZ(minsAndMaxs[2]);
                 tree.getChildNodes()[childIndex].setMaxX(minsAndMaxs[3]); tree.getChildNodes()[childIndex].setMaxY(minsAndMaxs[4]); tree.getChildNodes()[childIndex].setMaxZ(minsAndMaxs[5]);
            }
            tree.getChildNodes()[childIndex] = parseOctree(x, y, z, tree.getChildNodes()[childIndex], PageIndex);
            return tree;
        }
    }

    public static void WriteIndex(String indexName, String strTableName, Octree t) throws DBAppException {
        String filePath = DBConstants.META_DIR + indexName + "_" + strTableName + DBConstants.DATA_FILE_EXTENSION;
        logger.debug("Attempting to serialize Octree index '{}' for table '{}' to: {}", indexName, strTableName, filePath);
        File file = new File(filePath);
        try {
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                if (parentDir.mkdirs()) {
                    logger.info("Created parent directory for index file: {}", parentDir.getAbsolutePath());
                } else {
                    logger.warn("Failed to create parent directory for index file: {}", parentDir.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            logger.error("Exception while ensuring parent directory for {}: {}", filePath, e.getMessage(), e);
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(file);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(t);
            logger.info("Successfully serialized Octree index '{}' for table '{}' to {}", indexName, strTableName, filePath);
        } catch (IOException e) {
            logger.error("IOException during Octree index serialization to {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to serialize Octree index '" + indexName + "' for table '" + strTableName + "' due to IOException: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected exception during Octree index serialization to {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("An unexpected error occurred while serializing Octree index '" + indexName + "' for table '" + strTableName + "': " + e.getMessage());
        }
    }

    public static Octree ReadIndex(String indexName, String strTableName) throws DBAppException {
        String filePath = DBConstants.META_DIR + indexName + "_" + strTableName + DBConstants.DATA_FILE_EXTENSION;
        logger.debug("Attempting to deserialize Octree index '{}' for table '{}' from: {}", indexName, strTableName, filePath);
        try (FileInputStream fileInputStream = new FileInputStream(filePath);
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            Octree t = (Octree) objectInputStream.readObject();
            logger.info("Successfully deserialized Octree index '{}' for table '{}' from {}", indexName, strTableName, filePath);
            return t;
        } catch (FileNotFoundException e) {
            logger.error("File not found for Octree index deserialization: {}", filePath, e);
            throw new DBAppException("Unable to deserialize Octree index '" + indexName + "' for table '" + strTableName + "': File not found at " + filePath);
        } catch (IOException e) {
            logger.error("IOException during Octree index deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to deserialize Octree index '" + indexName + "' for table '" + strTableName + "' due to IOException: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException during Octree index deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("Unable to deserialize Octree index '" + indexName + "' for table '" + strTableName + "': Class not found - " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected exception during Octree index deserialization from {}: {}", filePath, e.getMessage(), e);
            throw new DBAppException("An unexpected error occurred while deserializing Octree index '" + indexName + "' for table '" + strTableName + "': " + e.getMessage());
        }
    }

    private static Hashtable<String, Object> CheckConditions(String strClusteringKeyValue, ArrayList<String[]> tablesName, String strTableName, Hashtable<String, Object> htblColNameValue, String action) throws DBAppException {
        logger.trace("Checking conditions for action '{}' on table '{}'. ClusteringKey Value: '{}', ColumnValues: {}", action, strTableName, strClusteringKeyValue, htblColNameValue);
        String primaryKeyColumnName = getPrimKey(strTableName);
        Object objClusteringKeyValueTyped = null;
        if (strClusteringKeyValue != null && !strClusteringKeyValue.isEmpty()) {
             objClusteringKeyValueTyped = convertToDataType(strClusteringKeyValue);
        }

        Hashtable<String, Object> rowToBeInserted = new Hashtable<>();

        for (String[] metadataLine : tablesName) {
            if (metadataLine.length < 8) {
                logger.warn("Skipping malformed metadata line (not enough columns): {}", Arrays.toString(metadataLine));
                continue;
            }
            if (metadataLine[0].equals(strTableName)) {
                String currentColumnName = metadataLine[1];
                String currentColumnType = metadataLine[2];
                boolean isClusteringKey = Boolean.parseBoolean(metadataLine[3]);
                String minValStr = metadataLine[6];
                String maxValStr = metadataLine[7];

                if (htblColNameValue.containsKey(currentColumnName)) {
                    Object inputValue = htblColNameValue.get(currentColumnName);
                    if (inputValue == null) {
                        logger.error("Null value provided for column '{}' in action '{}' for table '{}'. This is generally not allowed.", currentColumnName, action, strTableName);
                        throw new DBAppException("Null value provided for column " + currentColumnName);
                    }

                    if (!inputValue.getClass().getName().equals(currentColumnType)) {
                        logger.error("Type mismatch for column '{}'. Expected: {}, Actual: {}. Value: {}", currentColumnName, currentColumnType, inputValue.getClass().getName(), inputValue);
                        throw new DBAppException("Type mismatch for column " + currentColumnName + ". Expected " + currentColumnType);
                    }

                    if (compareToObject(inputValue, convertToDataType(minValStr)) < 0 || compareToObject(inputValue, convertToDataType(maxValStr)) > 0) {
                        logger.error("Value for column '{}' is out of range [{}, {}]. Value: {}", currentColumnName, minValStr, maxValStr, inputValue);
                        throw new DBAppException("Value for column " + currentColumnName + " is out of range.");
                    }

                    if (DBConstants.ACTION_INSERT.equals(action)) {
                        rowToBeInserted.put(currentColumnName, inputValue);
                    } else if (DBConstants.ACTION_UPDATE.equals(action) && isClusteringKey) {
                         logger.error("Attempt to update primary key column '{}' in table '{}' is disallowed.", currentColumnName, strTableName);
                         throw new DBAppException("You cannot update the primary key!");
                    }
                } else if (DBConstants.ACTION_INSERT.equals(action)) {
                    if (isClusteringKey) {
                        logger.error("Primary key '{}' must be provided for insert operation in table '{}'.", currentColumnName, strTableName);
                        throw new DBAppException("Primary key column '" + currentColumnName + "' value must be provided for insert.");
                    } else {
                        logger.error("Column '{}' must be provided for insert operation in table '{}' (assuming non-nullable).", currentColumnName, strTableName);
                        throw new DBAppException("Value for column '" + currentColumnName + "' must be provided for insert.");
                    }
                }
            }
        }

        if (DBConstants.ACTION_UPDATE.equals(action) && objClusteringKeyValueTyped != null && !primaryKeyColumnName.isEmpty()) {
            for (String[] metadataLine : tablesName) {
                 if (metadataLine.length > 7 && metadataLine[0].equals(strTableName) && metadataLine[1].equals(primaryKeyColumnName)) {
                    String minValStr = metadataLine[6];
                    String maxValStr = metadataLine[7];
                    if (compareToObject(objClusteringKeyValueTyped, convertToDataType(minValStr)) < 0 ||
                        compareToObject(objClusteringKeyValueTyped, convertToDataType(maxValStr)) > 0) {
                        logger.error("Clustering key value '{}' for update is out of range [{}, {}] for PK column '{}'.", strClusteringKeyValue, minValStr, maxValStr, primaryKeyColumnName);
                        throw new DBAppException("Clustering key value " + strClusteringKeyValue + " for update is out of range for primary key " + primaryKeyColumnName);
                    }
                    break;
                 }
            }
        }
        if (DBConstants.ACTION_INSERT.equals(action)) {
             logger.debug("Validated row for insertion: {}", rowToBeInserted);
             return rowToBeInserted;
        }
        return new Hashtable<>();
    }

    public void init() {
        logger.info("DBApp initialized.");
    }

    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
        logger.info("Attempting to create table '{}' with Clustering Key '{}'. Column Types: {}, Column Mins: {}, Column Maxs: {}",
                strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);

        if (htblColNameMin.size() != htblColNameType.size() || htblColNameMax.size() != htblColNameType.size()) {
            logger.error("Mismatch in number of columns for types, mins, or maxs. Types: {}, Mins: {}, Maxs: {}", htblColNameType.size(), htblColNameMin.size(), htblColNameMax.size());
            throw new DBAppException("Incompatible number of entries for column types, min values, and max values.");
        }
        if(!htblColNameType.containsKey(strClusteringKeyColumn)){
            logger.error("Clustering key column '{}' not found in the provided column types for table '{}'.", strClusteringKeyColumn, strTableName);
            throw new DBAppException("Clustering key column '" + strClusteringKeyColumn + "' must be defined in column types.");
        }

        try {
            logger.debug("Writing metadata to CSV for table '{}'.", strTableName);
            Writecsv(strTableName, htblColNameType, strClusteringKeyColumn, htblColNameMin, htblColNameMax);
            logger.debug("Metadata CSV updated successfully for table '{}'.", strTableName);
        } catch (DBAppException e) {
            logger.error("DBAppException while writing CSV metadata for table '{}': {}", strTableName, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected exception while writing CSV metadata for table '{}'", strTableName, e);
            throw new DBAppException("Cannot create table '" + strTableName + "' due to metadata writing error: " + e.getMessage());
        }

        Table t = new Table(strTableName);
        t.setStrClusteringKey(strClusteringKeyColumn); // Set the clustering key in the Table object
        logger.debug("Table object created for '{}'. Clustering key set to '{}'.", strTableName, strClusteringKeyColumn);
        try {
            logger.debug("Writing table object for '{}' to disk.", strTableName);
            WriteTable(strTableName, t);
            logger.info("Table '{}' created successfully.", strTableName);
        } catch (DBAppException e) {
            logger.error("DBAppException while serializing table object for '{}': {}", strTableName, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected exception while serializing table object for '{}'", strTableName, e);
            throw new DBAppException("Cannot serialize table object for '" + strTableName + "': " + e.getMessage());
        }
    }

    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        logger.info("Attempting to create index on table '{}' for columns: {}", strTableName, Arrays.toString(strarrColName));

        if (strarrColName == null || strarrColName.length != 3) {
            logger.error("Index creation requires exactly 3 column names. Provided: {}", (strarrColName != null ? Arrays.toString(strarrColName) : "null"));
            throw new DBAppException("You have to create index on EXACTLY 3 columns.");
        }

        String[] sortedColNamesForName = Arrays.copyOf(strarrColName, strarrColName.length);
        Arrays.sort(sortedColNamesForName);
        String indexName = sortedColNamesForName[0] + sortedColNamesForName[1] + sortedColNamesForName[2] + "Index";
        logger.debug("Generated index name (from sorted column names): {}", indexName);


        Object minX = null, minY = null, minZ = null;
        Object maxX = null, maxY = null, maxZ = null;
        String col1 = strarrColName[0];
        String col2 = strarrColName[1];
        String col3 = strarrColName[2];

        ArrayList<String[]> tableMetadataEntries;

        try {
            tableMetadataEntries = loopCSV();
            logger.debug("Read {} lines from metadata CSV.", tableMetadataEntries.size());

            Set<String> specifiedColsSet = new HashSet<>(Arrays.asList(strarrColName));
            if (specifiedColsSet.size() != 3) {
                logger.error("Duplicate column names provided for index creation: {}", Arrays.toString(strarrColName));
                throw new DBAppException("Column names for index must be unique.");
            }

            int columnsValidatedCount = 0;
            for (String colName : strarrColName) {
                boolean colFoundInMeta = false;
                for (String[] metadataLine : tableMetadataEntries) {
                    if (metadataLine.length > 5 && metadataLine[0].equals(strTableName) && metadataLine[1].equals(colName)) {
                        colFoundInMeta = true;
                        logger.trace("Processing metadata line for table '{}', column '{}': {}", strTableName, colName, Arrays.toString(metadataLine));

                        if (DBConstants.INDEX_TYPE_OCTREE.equals(metadataLine[5])) {
                            if (indexName.equals(metadataLine[4])) {
                                logger.warn("Column '{}' in table '{}' is already part of the index '{}' being created. This might be a re-creation attempt.", colName, strTableName, indexName);
                            } else {
                                logger.error("Column '{}' in table '{}' is already part of a different Octree index: '{}'. Cannot create new index '{}'.", colName, strTableName, metadataLine[4], indexName);
                                throw new DBAppException("Column '" + colName + "' is already part of another Octree index ('" + metadataLine[4] + "').");
                            }
                        }
                        columnsValidatedCount++;
                        break;
                    }
                }
                if (!colFoundInMeta) {
                    logger.error("Column '{}' specified for index not found in metadata for table '{}'.", colName, strTableName);
                    throw new DBAppException("Column '" + colName + "' not found in metadata for table '" + strTableName + "'.");
                }
            }

            if (columnsValidatedCount != 3) {
                logger.error("Failed to validate all 3 specified columns for index creation on table '{}'. Validated: {}", strTableName, columnsValidatedCount);
                throw new DBAppException("Not all 3 specified columns could be validated against metadata for table " + strTableName + ".");
            }

            minX = getColumnMinMax(tableMetadataEntries, strTableName, col1, true, indexName);
            maxX = getColumnMinMax(tableMetadataEntries, strTableName, col1, false, indexName);
            minY = getColumnMinMax(tableMetadataEntries, strTableName, col2, true, indexName);
            maxY = getColumnMinMax(tableMetadataEntries, strTableName, col2, false, indexName);
            minZ = getColumnMinMax(tableMetadataEntries, strTableName, col3, true, indexName);
            maxZ = getColumnMinMax(tableMetadataEntries, strTableName, col3, false, indexName);

            logger.debug("Successfully validated columns for index '{}': Col1='{}' ({}-{}), Col2='{}' ({}-{}), Col3='{}' ({}-{})",
                    indexName, col1, minX, maxX, col2, minY, maxY, col3, minZ, maxZ);

            boolean metadataChanged = false;
            for (String[] metadataLine : tableMetadataEntries) {
                 if (metadataLine.length > 5 && metadataLine[0].equals(strTableName) &&
                    (metadataLine[1].equals(col1) || metadataLine[1].equals(col2) || metadataLine[1].equals(col3))) {
                    if (!indexName.equals(metadataLine[4]) || !DBConstants.INDEX_TYPE_OCTREE.equals(metadataLine[5])) {
                        metadataLine[4] = indexName;
                        metadataLine[5] = DBConstants.INDEX_TYPE_OCTREE;
                        metadataChanged = true;
                        logger.trace("Updated in-memory metadata for table '{}', column '{}' to use index '{}'", strTableName, metadataLine[1], indexName);
                    }
                }
            }

            Table tableForIndexCheck = ReadTable(strTableName);
            if (!metadataChanged && Table.isIndexStored(tableForIndexCheck, indexName)) {
                logger.info("Index '{}' already exists and metadata is up-to-date. Checking index file existence.", indexName);
                File indexFile = new File(DBConstants.META_DIR + indexName + "_" + strTableName + DBConstants.DATA_FILE_EXTENSION);
                if (!indexFile.exists()) {
                    logger.warn("Index '{}' metadata exists, but index file not found. Recreating and populating index file.", indexName);
                } else {
                     logger.info("Index file for '{}' also exists. No changes needed.", indexName);
                    return;
                }
            }


        } catch (DBAppException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected exception while processing CSV metadata for index creation on table '{}'", strTableName, e);
            throw new DBAppException("Error processing CSV metadata for index creation on table '" + strTableName + "': " + e.getMessage());
        }

        try {
            File csvFile = new File(DBConstants.METADATA_FILE_PATH);
            logger.warn("Rewriting entire metadata file '{}' for index creation on table '{}'.", DBConstants.METADATA_FILE_PATH, strTableName);

            try (FileWriter writer = new FileWriter(csvFile, false)) {
                for (String[] metadataLine : tableMetadataEntries) {
                    writer.write(String.join(",", metadataLine) + "\n");
                }
                writer.flush();
                logger.info("Successfully rewrote metadata file '{}'.", DBConstants.METADATA_FILE_PATH);
            }
        } catch (IOException e) {
            logger.error("IOException while rewriting metadata file '{}' for table '{}'", DBConstants.METADATA_FILE_PATH, strTableName, e);
            throw new DBAppException("Failed to rewrite " + DBConstants.METADATA_FILE_PATH + " for index creation: " + e.getMessage());
        }

        Octree tree = new Octree();
        tree.setMaxX(maxX); tree.setMaxY(maxY); tree.setMaxZ(maxZ);
        tree.setMinX(minX); tree.setMinY(minY); tree.setMinZ(minZ);
        tree.setCol1(col1); tree.setCol2(col2); tree.setCol3(col3);
        logger.debug("Octree object configured for index '{}' on table '{}'. Dimensions: {} ({}-{}), {} ({}-{}), {} ({}-{})",
                indexName, strTableName, col1, minX, maxX, col2, minY, maxY, col3, minZ, maxZ);


        WriteIndex(indexName, strTableName, tree);
        logger.debug("Initial empty Octree index '{}' written to disk for table '{}'.", indexName, strTableName);

        Table table = ReadTable(strTableName);
        if (!table.Indices.contains(indexName)) {
            table.Indices.add(indexName);
            logger.debug("Added index name '{}' to table '{}' object's list of indices.", indexName, strTableName);
        }

        logger.info("Populating index '{}' for table '{}' by scanning table data...", indexName, strTableName);
        insertIntoIndex(strTableName, indexName);
        logger.info("Index '{}' populated successfully.", indexName);

        WriteTable(table.getTableName(), table);
        logger.info("Index '{}' on table '{}' created and populated successfully.", indexName, strTableName);
    }

    private Object getColumnMinMax(ArrayList<String[]> metadataEntries, String tableName, String columnName, boolean getMin, String indexBeingCreated) throws DBAppException {
        for (String[] line : metadataEntries) {
            if (line.length > 7 && line[0].equals(tableName) && line[1].equals(columnName)) {
                try {
                    String valueStr = getMin ? line[6] : line[7];
                    if (DBConstants.CSV_NULL_STRING.equalsIgnoreCase(valueStr)) {
                        logger.error("Min/Max value for column '{}' in table '{}' is 'null' in metadata.", columnName, tableName);
                        throw new DBAppException("Min/Max value for column '" + columnName + "' cannot be null for index creation.");
                    }
                    return valueStr;
                } catch (ArrayIndexOutOfBoundsException e) {
                    logger.error("Metadata line for {}.{} is malformed (not enough columns for min/max): {}", tableName, columnName, Arrays.toString(line), e);
                    throw new DBAppException("Malformed metadata for column " + tableName + "." + columnName + " when retrieving min/max values.");
                }
            }
        }
        logger.error("Metadata not found for {}.{} to retrieve min/max.", tableName, columnName);
        throw new DBAppException("Metadata not found for " + tableName + "." + columnName + " to retrieve min/max for index creation.");
    }


    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        logger.info("Attempting to insert into table '{}': {}", strTableName, htblColNameValue);

        ArrayList<String[]> tableMetadata = loopCSV();
        checkColumns(tableMetadata, strTableName, htblColNameValue);
        String primaryKey = getPrimKey(strTableName);
        logger.debug("Primary key for table '{}' is '{}'", strTableName, primaryKey);
        if (primaryKey.isEmpty()) {
            throw new DBAppException("Primary key not found for table " + strTableName + ". Cannot insert.");
        }
        if (!htblColNameValue.containsKey(primaryKey)) {
             throw new DBAppException("Primary key column '" + primaryKey + "' must be provided in values for insert.");
        }


        Hashtable<String, Object> rowToBeInserted = CheckConditions(htblColNameValue.get(primaryKey).toString(), tableMetadata, strTableName, htblColNameValue, DBConstants.ACTION_INSERT);
        logger.debug("Validated row to be inserted: {}", rowToBeInserted);

        Table tableFile = ReadTable(strTableName);
        if (tableFile.noOfPages == 0) {
            logger.info("Table '{}' is empty. Creating first page.", strTableName);
            try {
                tableFile.addPages(strTableName);
                logger.debug("Added first page to table '{}'. Total pages: {}", strTableName, tableFile.noOfPages);
                 WriteTable(strTableName, tableFile);
            } catch (IOException e) { // addPages in Table.java might throw DBAppException if WritePage/WriteTable fails
                logger.error("IOException while adding first page to table '{}'", strTableName, e);
                throw new DBAppException("Error creating the first page for table " + strTableName + ": " + e.getMessage());
            } catch (DBAppException e) { // Catch DBAppException from addPages
                 logger.error("DBAppException while adding first page to table '{}'", strTableName, e);
                throw e;
            }


            Page p = ReadPage(0, strTableName);
            logger.debug("Updating indices for new row in page 0 of table '{}'", strTableName);
            for (int i = 0; i < tableFile.Indices.size(); i++) {
                String indexName = tableFile.Indices.get(i);
                Octree tree = ReadIndex(indexName, tableFile.getTableName());
                logger.trace("Parsing Octree for index '{}' with values: X={}, Y={}, Z={}, PageIndex=0",
                        indexName, rowToBeInserted.get(tree.getCol1()), rowToBeInserted.get(tree.getCol2()), rowToBeInserted.get(tree.getCol3()));
                tree = parseOctree(rowToBeInserted.get(tree.getCol1()), rowToBeInserted.get(tree.getCol2()), rowToBeInserted.get(tree.getCol3()), tree, 0);
                WriteIndex(indexName, strTableName, tree);
            }

            p.addRow(rowToBeInserted, primaryKey);
            logger.debug("Added row to page 0. Page size: {}", p.getTuples().size());
            WritePage(0, strTableName, p);
            logger.info("Successfully inserted row into new page 0 of table '{}'", strTableName);
            return;
        } else {
            Object valueOfKey = htblColNameValue.get(primaryKey);
            logger.debug("Value of clustering key for insertion: {}", valueOfKey);
            boolean done = false;

            for (int x = 0; x < tableFile.noOfPages && !done; x++) {
                int myPageIdx = x;
                Page p = ReadPage(myPageIdx, tableFile.getTableName());
                if (p.getTuples().isEmpty()) {
                    logger.warn("Page {} in table {} is empty. This might indicate an issue or prior deletions.", myPageIdx, strTableName);
                    if (x < tableFile.noOfPages -1) continue;
                }

                logger.trace("Checking page {} for insertion. Page min key: {}, max key: {}", myPageIdx,
                             (p.getTuples().isEmpty() ? "N/A" : p.getTuples().get(0).get(primaryKey)),
                             (p.getTuples().isEmpty() ? "N/A" : p.getTuples().get(p.getTuples().size() - 1).get(primaryKey)));

                boolean fitsInPage = !p.getTuples().isEmpty() &&
                                     compareToObject(valueOfKey, p.getTuples().get(0).get(primaryKey)) >= 0 &&
                                     compareToObject(valueOfKey, p.getTuples().get(p.getTuples().size() - 1).get(primaryKey)) <= 0;

                boolean insertHere = fitsInPage;

                if (!insertHere && x == 0 && !p.getTuples().isEmpty() && compareToObject(valueOfKey, p.getTuples().get(0).get(primaryKey)) < 0) {
                    insertHere = true;
                } else if (!insertHere && x == tableFile.noOfPages - 1 && !p.getTuples().isEmpty() && compareToObject(valueOfKey, p.getTuples().get(p.getTuples().size()-1).get(primaryKey)) > 0) {
                    insertHere = true;
                } else if (p.getTuples().isEmpty() && x == tableFile.noOfPages -1) {
                    insertHere = true;
                }


                if (insertHere) {
                    logger.info("Target page for insertion in table '{}' is {}.", strTableName, myPageIdx);

                    logger.debug("Updating indices for new row in page {} of table '{}'", myPageIdx, strTableName);
                    for (int i = 0; i < tableFile.Indices.size(); i++) {
                        String indexName = tableFile.Indices.get(i);
                        Octree t = ReadIndex(indexName, tableFile.getTableName());
                        logger.trace("Parsing Octree for index '{}' with values: X={}, Y={}, Z={}, PageIndex={}",
                                indexName, rowToBeInserted.get(t.getCol1()), rowToBeInserted.get(t.getCol2()), rowToBeInserted.get(t.getCol3()), myPageIdx);
                        t = parseOctree(rowToBeInserted.get(t.getCol1()), rowToBeInserted.get(t.getCol2()), rowToBeInserted.get(t.getCol3()), t, myPageIdx);
                        WriteIndex(indexName, strTableName, t);
                    }

                    p.addRow(rowToBeInserted, primaryKey);
                    logger.debug("Added row to page {}. Page size: {}", myPageIdx, p.getTuples().size());
                    WritePage(myPageIdx, tableFile.getTableName(), p);
                    done = true;

                    if (p.getTuples().size() > p.getMaxRows()) {
                        logger.info("Page {} is full (size {}). Handling overflow.", myPageIdx, p.getTuples().size());
                        try {
                            handleOverFlow(myPageIdx, tableFile, primaryKey);
                        } catch (Exception e) {
                            logger.error("Exception during overflow handling for page {} in table '{}'", myPageIdx, strTableName, e);
                            throw new DBAppException("Error handling overflow for page " + myPageIdx + " in table " + strTableName + ": " + e.getMessage());
                        }
                    }
                    logger.info("Successfully inserted row into page {} of table '{}'", myPageIdx, strTableName);
                }
            }

            if (!done) {
                logger.warn("!done after loop. This might indicate an edge case or that a new page should have been created for value {} in table {}.", valueOfKey, strTableName);
                int targetPageIdx;
                Page p;
                // Ensure page 0 exists and is not empty before trying to get a tuple from it
                Page firstPage = (tableFile.noOfPages > 0) ? ReadPage(0, strTableName) : null;

                if (firstPage != null && !firstPage.getTuples().isEmpty() && compareToObject(valueOfKey, firstPage.getTuples().get(0).get(primaryKey)) < 0) {
                    targetPageIdx = 0;
                    p = firstPage;
                } else {
                    targetPageIdx = tableFile.noOfPages - 1;
                     if (targetPageIdx < 0) {
                         logger.error("No pages found to insert into for table '{}' in !done block. This is unexpected.", strTableName);
                         throw new DBAppException("Cannot insert, no pages found in table " + strTableName + " and insertion logic failed.");
                     }
                    p = ReadPage(targetPageIdx, strTableName);
                }
                logger.info("Fallback: Inserting into page {} for table '{}'", targetPageIdx, strTableName);

                logger.debug("Updating indices for new row in page {} of table '{}'", targetPageIdx, strTableName);
                 for (int i = 0; i < tableFile.Indices.size(); i++) {
                    String indexName = tableFile.Indices.get(i);
                    Octree t = ReadIndex(indexName, tableFile.getTableName());
                     logger.trace("Parsing Octree for index '{}' with values: X={}, Y={}, Z={}, PageIndex={}",
                                indexName, rowToBeInserted.get(t.getCol1()), rowToBeInserted.get(t.getCol2()), rowToBeInserted.get(t.getCol3()), targetPageIdx);
                    t = parseOctree(rowToBeInserted.get(t.getCol1()), rowToBeInserted.get(t.getCol2()), rowToBeInserted.get(t.getCol3()), t, targetPageIdx);
                    WriteIndex(indexName, strTableName, t);
                }

                p.addRow(rowToBeInserted, primaryKey);
                logger.debug("Added row to page {}. Page size: {}", targetPageIdx, p.getTuples().size());
                WritePage(targetPageIdx, strTableName, p);

                if (p.getTuples().size() > p.getMaxRows()) {
                    logger.info("Page {} is full (size {}). Handling overflow.", targetPageIdx, p.getTuples().size());
                    try {
                        handleOverFlow(targetPageIdx, tableFile, primaryKey);
                    } catch (Exception e) {
                        logger.error("Exception during overflow handling for page {} in table '{}'", targetPageIdx, strTableName, e);
                        throw new DBAppException("Error handling overflow for page " + targetPageIdx + " in table " + strTableName + ": " + e.getMessage());
                    }
                }
                logger.info("Successfully inserted row into page {} of table '{}' (fallback/edge case handling)", targetPageIdx, strTableName);
            }
        }
        logger.info("Insertion process completed for table '{}'", strTableName);
    }

    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        logger.info("Attempting to update table '{}' with clustering key value '{}' and new data: {}", strTableName, strClusteringKeyValue, htblColNameValue);

        ArrayList<String[]> tableMetadata = loopCSV();
        String primaryKey = getPrimKey(strTableName);
        logger.debug("Primary key for table '{}' is '{}'. Clustering key value for update: {}", strTableName, primaryKey, strClusteringKeyValue);
         if (primaryKey.isEmpty()) {
            throw new DBAppException("Primary key not found for table " + strTableName + ". Cannot update.");
        }


        checkColumns(tableMetadata, strTableName, htblColNameValue);

        Object objClusteringKeyValue = convertToDataType(strClusteringKeyValue);
        String clusteringKeyType = getPrimKeyType(strTableName);

        if (DBConstants.JAVA_UTIL_DATE.equals(clusteringKeyType) && strClusteringKeyValue !=null) {
            checkDateFormat(strClusteringKeyValue);
        }

        if(htblColNameValue.containsKey(primaryKey)){
            logger.error("Attempt to update primary key column '{}' directly via htblColNameValue is disallowed.", primaryKey);
            throw new DBAppException("Primary key column '" + primaryKey + "' cannot be included in the new values for an update operation.");
        }


        CheckConditions(strClusteringKeyValue, tableMetadata, strTableName, htblColNameValue, DBConstants.ACTION_UPDATE);
        logger.debug("Initial checks passed for update operation on table '{}'", strTableName);

        Hashtable<String, Object> newRowValues = new Hashtable<>();
        Hashtable<String, Object> oldRowValues = new Hashtable<>();
        int pageRefOfUpdate = -1;

        Table tableFile = ReadTable(strTableName);
        if (tableFile.noOfPages == 0) {
            logger.warn("Update operation on empty table '{}'. No action taken.", strTableName);
            throw new DBAppException("Cannot update: the table '" + strTableName + "' is empty.");
        }

        boolean found = false;
        boolean isClusterColIndexed = isClusterIndexed(primaryKey, strTableName);
        logger.debug("Is clustering key '{}' part of an index? {}", primaryKey, isClusterColIndexed);

        if (isClusterColIndexed) {
            ArrayList<String> pkList = new ArrayList<>();
            pkList.add(primaryKey);
            String indexName = getIndexName(pkList, strTableName);

            logger.debug("Attempting to use index for update. Index name for primary key column: {}", indexName);

            if (!DBConstants.LINEAR_SEARCH_INDICATOR.equals(indexName)) {
                Octree tree = ReadIndex(indexName, strTableName);
                Point searchPoint = new Point(null, null, null, -1);
                Point oldDataPoint = new Point(null, null, null, -1);
                Point newDataPoint = new Point(null, null, null, -1);

                if (tree.getCol1().equals(primaryKey)) searchPoint.setX(objClusteringKeyValue);
                else if (tree.getCol2().equals(primaryKey)) searchPoint.setY(objClusteringKeyValue);
                else if (tree.getCol3().equals(primaryKey)) searchPoint.setZ(objClusteringKeyValue);
                else {
                    logger.warn("Index '{}' was identified for PK '{}', but PK is not col1, col2, or col3 of the Octree. Proceeding with caution or potential inefficiency.", indexName, primaryKey);
                }

                ArrayList<Integer> potentialPages = new ArrayList<>();
                potentialPages = searchOctree(searchPoint, tree, potentialPages);
                logger.debug("Index search for key '{}' in index '{}' returned potential pages: {}", objClusteringKeyValue, indexName, potentialPages);

                pageLoop:
                for (int pageIdx : potentialPages) {
                    Page page = ReadPage(pageIdx, strTableName);
                    int l = 0;
                    int r = page.getTuples().size() - 1;
                    while (l <= r) {
                        int m = l + (r - l) / 2;
                        Hashtable<String, Object> currentTuple = page.getTuples().get(m);
                        if (compareToObject(objClusteringKeyValue, currentTuple.get(primaryKey)) == 0) {
                            logger.info("Found tuple to update in page {} at index {} via indexed search.", pageIdx, m);
                            oldRowValues.putAll(currentTuple);
                            pageRefOfUpdate = pageIdx;

                            oldDataPoint.setX(currentTuple.get(tree.getCol1()));
                            oldDataPoint.setY(currentTuple.get(tree.getCol2()));
                            oldDataPoint.setZ(currentTuple.get(tree.getCol3()));
                            oldDataPoint.setRef(pageIdx);

                            logger.debug("Deleting old entry from Octree '{}': {}", indexName, oldDataPoint);
                            Octree tempTree = ReadIndex(indexName, strTableName);
                            tempTree = deleteOctree(oldDataPoint, tempTree);

                            for (String keyToUpdate : htblColNameValue.keySet()) {
                                currentTuple.put(keyToUpdate, htblColNameValue.get(keyToUpdate));
                            }
                            logger.debug("Tuple updated to: {}", currentTuple);
                            newRowValues.putAll(currentTuple);

                            newDataPoint.setX(currentTuple.get(tree.getCol1()));
                            newDataPoint.setY(currentTuple.get(tree.getCol2()));
                            newDataPoint.setZ(currentTuple.get(tree.getCol3()));
                            newDataPoint.setRef(pageIdx);

                            logger.debug("Inserting new entry into Octree '{}': {}", indexName, newDataPoint);
                            tempTree = parseOctree(newDataPoint.getX(), newDataPoint.getY(), newDataPoint.getZ(), tempTree, pageIdx);
                            WriteIndex(indexName, strTableName, tempTree);

                            WritePage(pageIdx, strTableName, page);
                            found = true;
                            break pageLoop;
                        }
                        if (compareToObject(objClusteringKeyValue, currentTuple.get(primaryKey)) > 0) l = m + 1;
                        else r = m - 1;
                    }
                }
            } else {
                 logger.info("No specific index found for primary key column, or getIndexName returned 'linear Search'. Will proceed to linear scan if needed.");
            }
        }

        if (!found) {
            logger.info("Tuple not found via index or index not applicable/available. Performing linear scan of pages for table '{}'.", strTableName);
            Point updatePointOld = new Point(null, null, null, -1);
            Point updatePointNew = new Point(null, null, null, -1);

            pageScanLoop:
            for (int pageIdx = 0; pageIdx < tableFile.noOfPages; pageIdx++) {
                Page p = ReadPage(pageIdx, strTableName);
                if(p.getTuples().isEmpty()) continue;

                if (compareToObject(objClusteringKeyValue, p.getTuples().get(0).get(primaryKey)) >= 0 &&
                    compareToObject(objClusteringKeyValue, p.getTuples().get(p.getTuples().size() - 1).get(primaryKey)) <= 0) {

                    logger.trace("Scanning page {} for update target.", pageIdx);
                    int l = 0;
                    int r = p.getTuples().size() - 1;
                    while (l <= r) {
                        int m = l + (r - l) / 2;
                        Hashtable<String, Object> currentTuple = p.getTuples().get(m);
                        if (compareToObject(objClusteringKeyValue, currentTuple.get(primaryKey)) == 0) {
                            logger.info("Found tuple to update in page {} at index {} via linear scan.", pageIdx, m);
                            oldRowValues.putAll(currentTuple);
                            pageRefOfUpdate = pageIdx;

                            for (String keyToUpdate : htblColNameValue.keySet()) {
                                currentTuple.put(keyToUpdate, htblColNameValue.get(keyToUpdate));
                            }
                            logger.debug("Tuple updated to: {}", currentTuple);
                            newRowValues.putAll(currentTuple);
                            WritePage(pageIdx, strTableName, p);
                            found = true;
                            break pageScanLoop;
                        }
                        if (compareToObject(objClusteringKeyValue, currentTuple.get(primaryKey)) > 0) l = m + 1;
                        else r = m - 1;
                    }
                }
            }

            if (found) {
                logger.debug("Updating all relevant indices for table '{}' after linear scan update.", strTableName);
                for (String currentIdxName : tableFile.Indices) {
                    Octree treeToUpdate = ReadIndex(currentIdxName, strTableName);
                    boolean isRelevant = false;
                    for(String updatedKey : htblColNameValue.keySet()){
                        if(updatedKey.equals(treeToUpdate.getCol1()) || updatedKey.equals(treeToUpdate.getCol2()) || updatedKey.equals(treeToUpdate.getCol3())){
                            isRelevant = true; break;
                        }
                    }
                    if (!isRelevant && (oldRowValues.containsKey(treeToUpdate.getCol1()) || oldRowValues.containsKey(treeToUpdate.getCol2()) || oldRowValues.containsKey(treeToUpdate.getCol3()))) {
                        isRelevant = true;
                    }

                    if (isRelevant) {
                        logger.info("Updating index '{}' as it is relevant to the updated columns.", currentIdxName);

                        updatePointOld.setX(oldRowValues.get(treeToUpdate.getCol1()));
                        updatePointOld.setY(oldRowValues.get(treeToUpdate.getCol2()));
                        updatePointOld.setZ(oldRowValues.get(treeToUpdate.getCol3()));
                        updatePointOld.setRef(pageRefOfUpdate);

                        logger.debug("Deleting old entry from Octree '{}': {}", currentIdxName, updatePointOld);
                        treeToUpdate = deleteOctree(updatePointOld, treeToUpdate);

                        updatePointNew.setX(newRowValues.get(treeToUpdate.getCol1()));
                        updatePointNew.setY(newRowValues.get(treeToUpdate.getCol2()));
                        updatePointNew.setZ(newRowValues.get(treeToUpdate.getCol3()));
                        updatePointNew.setRef(pageRefOfUpdate);

                        logger.debug("Inserting new entry into Octree '{}': {}", currentIdxName, updatePointNew);
                        treeToUpdate = parseOctree(updatePointNew.getX(), updatePointNew.getY(), updatePointNew.getZ(), treeToUpdate, pageRefOfUpdate);
                        WriteIndex(currentIdxName, strTableName, treeToUpdate);
                    }
                }
            }
        }

        if (!found) {
            logger.warn("Update failed: Clustering key value '{}' not found in table '{}'.", strClusteringKeyValue, strTableName);
            throw new DBAppException("Primary key '" + strClusteringKeyValue + "' not found in table '" + strTableName + "' for update.");
        }

        logger.info("Update process completed for table '{}'. Clustering key: '{}'", strTableName, strClusteringKeyValue);
    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        logger.info("Attempting to delete from table '{}' with criteria: {}", strTableName, htblColNameValue);

        ArrayList<String[]> tablesNameMetaData = loopCSV();
        Table table = ReadTable(strTableName);
        checkColumns(tablesNameMetaData, strTableName, htblColNameValue);

        CheckConditions("", tablesNameMetaData, strTableName, htblColNameValue, DBConstants.ACTION_DELETE);
        logger.debug("Initial checks passed for delete operation on table '{}'", strTableName);

        String primaryKey = getPrimKey(strTableName);
        int deletedCount = 0;

        ArrayList<Integer> pagesToScan = new ArrayList<>();
        boolean useIndexForPageFiltering = false;

        if (!table.Indices.isEmpty() && !htblColNameValue.isEmpty()) {
            for (String indexName : table.Indices) {
                Octree tempTree = ReadIndex(indexName, strTableName);
                Point searchCriteriaPoint = new Point(null, null, null, -1);
                boolean criteriaMatchIndexCols = false;

                if (htblColNameValue.containsKey(tempTree.getCol1())) {
                    searchCriteriaPoint.setX(htblColNameValue.get(tempTree.getCol1()));
                    criteriaMatchIndexCols = true;
                }
                if (htblColNameValue.containsKey(tempTree.getCol2())) {
                    searchCriteriaPoint.setY(htblColNameValue.get(tempTree.getCol2()));
                    criteriaMatchIndexCols = true;
                }
                if (htblColNameValue.containsKey(tempTree.getCol3())) {
                    searchCriteriaPoint.setZ(htblColNameValue.get(tempTree.getCol3()));
                    criteriaMatchIndexCols = true;
                }

                if (criteriaMatchIndexCols) {
                    logger.info("Using index '{}' to narrow down pages for delete operation.", indexName);
                    pagesToScan = searchOctree(searchCriteriaPoint, tempTree, new ArrayList<>());
                    useIndexForPageFiltering = true;
                    logger.debug("Index search on '{}' identified potential pages: {}", indexName, pagesToScan);
                    break;
                }
            }
        }

        if (!useIndexForPageFiltering) {
            logger.info("No suitable index for pre-filtering pages or no criteria provided for index use. Scanning relevant/all pages.");
            if (htblColNameValue.containsKey(primaryKey) && !primaryKey.isEmpty()) {
                Object pkValue = htblColNameValue.get(primaryKey);
                logger.debug("Primary key provided in delete criteria. Attempting to narrow down page scan based on PK range.");
                for (int i = 0; i < table.noOfPages; i++) {
                    Page p = ReadPage(i, strTableName);
                    if (!p.getTuples().isEmpty() &&
                        compareToObject(pkValue, p.getTuples().get(0).get(primaryKey)) >= 0 &&
                        compareToObject(pkValue, p.getTuples().get(p.getTuples().size() - 1).get(primaryKey)) <= 0) {
                        pagesToScan.add(i);
                        logger.trace("Added page {} to scan based on PK range.", i);
                    }
                }
                if(pagesToScan.isEmpty() && table.noOfPages > 0 && pkValue != null){
                     logger.info("Primary key value {} is outside the range of all pages. No records will be deleted if PK is the sole criteria or all criteria must match.", pkValue);
                }
            } else {
                logger.debug("No primary key in delete criteria or no PK defined. Will scan all pages.");
                for (int i = 0; i < table.noOfPages; i++) {
                    pagesToScan.add(i);
                }
            }
        }

        logger.debug("Final list of pages to scan for deletion: {}", pagesToScan);
        Set<Hashtable<String,Object>> distinctTuplesDeleted = new HashSet<>();

        for (int pageIdx : pagesToScan) {
            Page p = ReadPage(pageIdx, strTableName);
            if (p.getTuples().isEmpty()) {
                logger.trace("Skipping empty page {}.", pageIdx);
                continue;
            }

            logger.trace("Scanning page {} for tuples to delete.", pageIdx);
            Vector<Hashtable<String, Object>> tuplesInPageToRemove = new Vector<>();

            for (Hashtable<String, Object> currentTuple : p.getTuples()) {
                boolean match = true;
                if (htblColNameValue.isEmpty()){
                    match = true;
                } else {
                    for (String key : htblColNameValue.keySet()) {
                        if (!currentTuple.containsKey(key) || !Objects.equals(currentTuple.get(key), htblColNameValue.get(key))) {
                            match = false;
                            break;
                        }
                    }
                }

                if (match) {
                    logger.info("Found matching tuple for deletion in page {}: {}", pageIdx, currentTuple);
                    tuplesInPageToRemove.add(currentTuple);
                    distinctTuplesDeleted.add(currentTuple);

                    for (String indexNameToUpdate : table.Indices) {
                        Octree treeToUpdate = ReadIndex(indexNameToUpdate, strTableName);
                        Point pointToDelete = new Point(
                                currentTuple.get(treeToUpdate.getCol1()),
                                currentTuple.get(treeToUpdate.getCol2()),
                                currentTuple.get(treeToUpdate.getCol3()),
                                pageIdx
                        );
                        logger.debug("Deleting point {} from index '{}'", pointToDelete, indexNameToUpdate);
                        treeToUpdate = deleteOctree(pointToDelete, treeToUpdate);
                        WriteIndex(indexNameToUpdate, strTableName, treeToUpdate);
                    }
                }
            }

            if (!tuplesInPageToRemove.isEmpty()) {
                p.getTuples().removeAll(tuplesInPageToRemove);
                WritePage(pageIdx, strTableName, p);
                logger.debug("{} tuples removed from page {}. Current page size: {}", tuplesInPageToRemove.size(), pageIdx, p.getTuples().size());

                if (p.getTuples().isEmpty()) {
                    logger.info("Page {} is now empty after deletions. Deleting page file and updating table metadata.", pageIdx);
                    try {
                        File pageFile = new File(DBConstants.META_DIR + table.getTableName() + pageIdx + DBConstants.DATA_FILE_EXTENSION);
                        if (pageFile.delete()) {
                            logger.debug("Successfully deleted page file: {}", pageFile.getPath());
                        } else {
                            logger.warn("Failed to delete page file: {}", pageFile.getPath());
                        }
                        Table currentTableState = ReadTable(strTableName);
                        currentTableState.deletePage(pageIdx);
                        WriteTable(strTableName, currentTableState);

                    } catch (IOException e) {
                        logger.error("IOException while deleting empty page {} or updating table metadata for table '{}'", pageIdx, strTableName, e);
                    }
                }
            }
        }
        deletedCount = distinctTuplesDeleted.size();
        if (deletedCount > 0) {
            logger.info("Successfully deleted {} distinct tuple(s) from table '{}'.", deletedCount, strTableName);
        } else {
            logger.info("No tuples found matching the criteria in table '{}'. No deletions performed.", strTableName);
        }
    }
}
