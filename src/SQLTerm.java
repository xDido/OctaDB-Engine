import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects; // For Objects.toString

public class SQLTerm implements Serializable {

    @Serial
    private static final long serialVersionUID = 987654321098765432L; // Example UID
    private static final Logger logger = LoggerFactory.getLogger(SQLTerm.class);

    private String _strTableName;
    private String _strColumnName;
    private String _strOperator;
    private Object _objValue;

    public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
        this._strTableName = strTableName;
        this._strColumnName = strColumnName;
        this._strOperator = strOperator;
        this._objValue = objValue;
        logger.debug("SQLTerm created: Table='{}', Column='{}', Operator='{}', Value='{}'",
                strTableName, strColumnName, strOperator, objValue);
    }

    public SQLTerm() {
        // This constructor is present in the original code.
        // It might be used by some frameworks or for deserialization if fields were public or had setters.
        // Since fields are now private and there are no setters, its direct utility is limited
        // unless specific deserialization libraries handle it.
        super();
        logger.debug("SQLTerm created using default constructor. Fields will be null until set (if setters are added).");
    }

    // Getters
    public String getTableName() {
        return _strTableName;
    }

    public String getColumnName() {
        return _strColumnName;
    }

    public String getOperator() {
        return _strOperator;
    }

    public Object getValue() {
        return _objValue;
    }

    @Override
    public String toString() {
        return "SQLTerm{" +
                "tableName='" + _strTableName + '\'' +
                ", columnName='" + _strColumnName + '\'' +
                ", operator='" + _strOperator + '\'' +
                ", value=" + (_objValue != null ? Objects.toString(_objValue) : "null") +
                '}';
    }

    // Optional: equals and hashCode if SQLTerm objects need to be compared by value or stored in Sets/Maps.
    // For now, focusing on the primary refactoring goals.
    // If these are needed, they would compare all four fields.
}
