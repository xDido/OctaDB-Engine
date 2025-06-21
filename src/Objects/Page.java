package Objects;

import Exceptions.DBAppException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import src.DBApp; // Assuming DBApp is in src package and contains the static compareToObject
import src.DBConstants; // Import DBConstants

import java.io.*;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {

    @Serial
    private static final long serialVersionUID = 180979049375953L;
    private static final Logger logger = LoggerFactory.getLogger(Page.class);

    int MaxRows;
    Vector<Hashtable<String, Object>> Tuples;

    public Page() {
        Tuples = new Vector<>(); // Diamond operator
        File configFile = new File(DBConstants.CONFIG_FILE_PATH);
        try (FileReader reader = new FileReader(configFile)) {
            Properties props = new Properties();
            props.load(reader);
            MaxRows = Integer.parseInt(props.getProperty(DBConstants.CONFIG_MAX_ROWS_PER_PAGE));
            logger.debug("Page initialized. MaxRows set to {} from config.", MaxRows);
        } catch (FileNotFoundException e) {
            logger.error("Configuration file '{}' not found.", DBConstants.CONFIG_FILE_PATH, e);
            throw new RuntimeException("Configuration file '" + DBConstants.CONFIG_FILE_PATH + "' not found.", e);
        } catch (IOException e) {
            logger.error("IOException while reading or parsing '{}'.", DBConstants.CONFIG_FILE_PATH, e);
            throw new RuntimeException("Error reading configuration file '" + DBConstants.CONFIG_FILE_PATH + "'.", e);
        } catch (NumberFormatException e) {
            logger.error("Invalid format for {} in '{}'.", DBConstants.CONFIG_MAX_ROWS_PER_PAGE, DBConstants.CONFIG_FILE_PATH, e);
            throw new RuntimeException("Invalid number format for " + DBConstants.CONFIG_MAX_ROWS_PER_PAGE + " in config.", e);
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

    // compareToObject method removed from here. DBApp.compareToObject should be used.

    public int binarySearchForInsertionPoint(Hashtable<String, Object> tuple, String primaryKeyColumn) throws DBAppException {
        logger.trace("Binary searching for insertion point for tuple with PK value: {} in a page with {} tuples.", tuple.get(primaryKeyColumn), Tuples.size());
        Object primaryValueToInsert = tuple.get(primaryKeyColumn);
        int low = 0;
        int high = Tuples.size() - 1;
        int insertionPoint = Tuples.size(); // Default to end if larger than all

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Object midTuplePrimaryValue = Tuples.get(mid).get(primaryKeyColumn);
            int comparison = DBApp.compareToObject(primaryValueToInsert, midTuplePrimaryValue);

            if (comparison == 0) {
                logger.warn("Attempting to insert a tuple with a duplicate primary key value: {}. PK column: {}", primaryValueToInsert, primaryKeyColumn);
                throw new DBAppException("Tuple with primary key " + primaryValueToInsert + " already exists in this page.");
            }

            if (comparison < 0) { // primaryValueToInsert < midTuplePrimaryValue
                insertionPoint = mid; // Potential insertion point
                high = mid - 1;
            } else { // primaryValueToInsert > midTuplePrimaryValue
                low = mid + 1;
            }
        }
        logger.trace("Binary search determined insertion point: {}", insertionPoint);
        return insertionPoint;
    }


    public void addRow(Hashtable<String, Object> table, String PrimaryKey) throws DBAppException {
        logger.debug("Attempting to add row to page. Row: {}, PK: {}", table, PrimaryKey);
        if (this.Tuples.isEmpty()) {
            this.Tuples.add(table);
            logger.info("Added row to empty page. New size: 1. Row: {}", table);
            return;
        }

        int insertionIndex = binarySearchForInsertionPoint(table, PrimaryKey);

        this.Tuples.add(insertionIndex, table);
        logger.info("Added row at index {}. New page size: {}. Row: {}", insertionIndex, this.Tuples.size(), table);
    }

    // serializePage method removed as DBApp.WritePage handles this.

    @Override
    public String toString() {
        return "Page{" +
                "MaxRows=" + MaxRows +
                ", CurrentTuples=" + (Tuples != null ? Tuples.size() : "null") +
                // Optionally, list some tuple summaries if not too verbose
                '}';
    }
}
