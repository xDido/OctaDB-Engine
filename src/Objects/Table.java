package Objects;

import Exceptions.DBAppException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import src.DBApp; // For DBApp.WritePage and DBApp.WriteTable

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List; // Changed from java.util.* to specific imports

public class Table implements Serializable {
    @Serial
    private static final long serialVersionUID = 455199201728903L;
    private static final Logger logger = LoggerFactory.getLogger(Table.class);

    private String tableName;
    private String strClusteringKey; // Should be set during table creation via metadata
    public int noOfPages = 0;
    public ArrayList<String> Indices; // Consider making this private with getters/setters

    public Table(String tableName) {
        this.tableName = tableName;
        this.Indices = new ArrayList<>();
        // strClusteringKey should be initialized, perhaps by reading metadata or passed in.
        // For now, it remains potentially null until set.
        logger.info("Table object created for tableName: '{}'", tableName);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        // Consider logging if table name changes, as it affects file paths.
        logger.warn("Table name changed from '{}' to '{}'. This might have implications for file storage if not handled carefully.", this.tableName, tableName);
        this.tableName = tableName;
    }
    
    public String getStrClusteringKey() {
        return strClusteringKey;
    }

    public void setStrClusteringKey(String strClusteringKey) {
        // This should ideally be set once during table creation.
        logger.debug("Setting clustering key for table '{}' to '{}'", tableName, strClusteringKey);
        this.strClusteringKey = strClusteringKey;
    }
    
    public List<String> getIndices(){
        return new ArrayList<>(this.Indices); // Return a copy
    }
    
    public void addIndex(String indexName) {
        if (!this.Indices.contains(indexName)) {
            this.Indices.add(indexName);
            logger.info("Added index '{}' to table '{}'", indexName, this.tableName);
        } else {
            logger.warn("Index '{}' already exists for table '{}'. Not adding again.", indexName, this.tableName);
        }
    }
    
    public void removeIndex(String indexName) {
        if (this.Indices.remove(indexName)) {
            logger.info("Removed index '{}' from table '{}'", indexName, this.tableName);
        } else {
            logger.warn("Index '{}' not found for table '{}'. Could not remove.", indexName, this.tableName);
        }
    }


    // serializeTable method removed, DBApp.WriteTable should be used.

    public void addPages(String tableNameFromParameter) throws DBAppException { // Changed to DBAppException for consistency
        // tableNameFromParameter seems redundant if this.tableName is correctly set and used.
        if (!this.tableName.equals(tableNameFromParameter)){
            logger.warn("addPages called with parameter tableName '{}' which differs from this.tableName '{}'. Using this.tableName.", tableNameFromParameter, this.tableName);
        }
        Page newPage = new Page(); // Page constructor loads MaxRows from config
        noOfPages++;
        int newPageIndex = this.noOfPages - 1;
        logger.info("Adding new page (ID: {}) to table '{}'. Total pages will be {}.", newPageIndex, this.tableName, this.noOfPages);

        DBApp.WritePage(newPageIndex, this.tableName, newPage); // Use DBApp static method
        logger.debug("New page {} for table '{}' serialized.", newPageIndex, this.tableName);

        DBApp.WriteTable(this.tableName, this); // Use DBApp static method to serialize this table instance
        logger.debug("Table '{}' re-serialized after adding page.", this.tableName);
    }

    public void deletePage(int pageIndex) throws DBAppException { // Changed to DBAppException
        // This method only updates the Table object's state (noOfPages).
        // Actual file deletion and re-arranging/re-linking pages if necessary is more complex
        // and seems to be handled partially in DBApp.deleteFromTable.
        // For now, this just decrements count and re-serializes the table object.
        // A robust implementation would need to handle shifting page numbers or managing a list of free pages.
        if (pageIndex < 0 || pageIndex >= noOfPages) {
            logger.error("Invalid pageIndex {} for deletion from table '{}'. Current noOfPages: {}", pageIndex, tableName, noOfPages);
            throw new DBAppException("Invalid page index " + pageIndex + " for table " + tableName);
        }

        // This simple decrement assumes pages are always compacted or that page numbers are not reused directly.
        // If page numbers are absolute and can have gaps, a different mechanism for page management is needed.
        // For now, mirroring the implication of the original simple decrement.
        noOfPages--;
        logger.info("Decremented page count for table '{}' due to deletion of page (conceptual index {}). New count: {}. Table object will be re-serialized.", tableName, pageIndex, noOfPages);

        // Note: The actual deletion of the page *file* should happen in DBApp.deleteFromTable
        // This method is more about updating the Table object's metadata about its pages.
        DBApp.WriteTable(this.tableName, this);
        logger.debug("Table '{}' re-serialized after (conceptual) page deletion.", this.tableName);
    }

    /**
     * Checks if a given index name is already registered in this table's list of indices.
     * This is a helper method that was identified as useful during DBApp.createIndex refactoring.
     * @param table The table object to check.
     * @param indexName The name of the index to check for.
     * @return true if the index is stored, false otherwise.
     */
    public static boolean isIndexStored(Table table, String indexName) {
        if (table == null || indexName == null || table.Indices == null) {
            return false;
        }
        return table.Indices.contains(indexName);
    }

    @Override
    public String toString() {
        return "Table{" +
                "tableName='" + tableName + '\'' +
                ", strClusteringKey='" + strClusteringKey + '\'' +
                ", noOfPages=" + noOfPages +
                ", Indices=" + (Indices != null ? Indices.toString() : "null") +
                '}';
    }
}
