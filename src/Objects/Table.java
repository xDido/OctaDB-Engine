package Objects;
import java.io.*;
import java.util.*;
public class Table implements Serializable{
    @Serial
    private static final long serialVersionUID = 455199201728903L;
    private String tableName ;
    private String strClusteringKey;
    public int noOfPages =0 ;
    public ArrayList<String> Indices ;
    public Table(String tableName){
        this.tableName=tableName;
        this.Indices = new ArrayList<>();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void serializeTable(String tableName) throws IOException{
        FileOutputStream fileOut = new FileOutputStream("meta/" + tableName + ".class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        fileOut.close();
        out.close();
    }

    public void addPages(String table) throws IOException{
        Page page = new Page();
        noOfPages++ ;
        page.serializePage( table , this.noOfPages-1); 
        /////////check!!!
        this.serializeTable(table);
    }
    
    public String getStrClusteringKey() {
        return strClusteringKey;
    }
   
    
   
    public void deletePage(int PageIndex) throws IOException {
    	this.noOfPages-- ;
    	this.serializeTable(this.tableName);
    }
    
    
    
    
    
    
    
    
}
