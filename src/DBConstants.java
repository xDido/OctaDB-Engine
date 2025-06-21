package src; // Assuming DBApp and other classes will be in src or have access to it. Adjust if needed.

public final class DBConstants {

    private DBConstants() {
        // Prevent instantiation
    }

    // Data Types
    public static final String JAVA_LANG_INTEGER = "java.lang.Integer";
    public static final String JAVA_LANG_STRING = "java.lang.String";
    public static final String JAVA_LANG_DOUBLE = "java.lang.Double";
    public static final String JAVA_UTIL_DATE = "java.util.Date";

    // Indexing
    public static final String INDEX_TYPE_OCTREE = "Octree.Octree"; // As used in metadata CSV
    public static final String LINEAR_SEARCH_INDICATOR = "linear Search";


    // File Paths and Names
    public static final String METADATA_FILE_PATH = "meta/Metadata.csv";
    public static final String CONFIG_FILE_PATH = "config/DBApp.config";
    public static final String DATA_FILE_EXTENSION = ".class"; // For table, page, index files
    public static final String META_DIR = "meta/";


    // Logical Operators (for SQL queries)
    public static final String OPERATOR_AND = "AND";
    public static final String OPERATOR_OR = "OR";
    public static final String OPERATOR_XOR = "XOR";

    // CSV Content Literals
    public static final String CSV_NULL_STRING = "null";
    public static final String CSV_TRUE_STRING = "True";
    public static final String CSV_FALSE_STRING = "False";

    // Date Format
    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";

    // Configuration Keys (example, more can be added if found)
    public static final String CONFIG_MAX_ROWS_PER_PAGE = "MaximumRowsCountinTablePage";
    public static final String CONFIG_MAX_ENTRIES_OCTREE_NODE = "MaximumEntriesinOctreeNode";

    // Actions (used in CheckConditions)
    public static final String ACTION_INSERT = "Insert";
    public static final String ACTION_UPDATE = "Update";
    public static final String ACTION_DELETE = "Delete";

    // Default value for primary key type if not found (as in getPrimKeyType)
    public static final String DEFAULT_PK_TYPE = "default";

}
