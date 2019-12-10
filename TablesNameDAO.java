import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.tiptech.tipqadatamodel.util.ConnectionStats;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import oracle.jdbc.OracleTypes;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.collections.map.MultiKeyMap;
import org.hibernate.SessionFactory;

import org.hibernate.internal.SessionImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository(value = "TablesNameDAO")
@Transactional
public class TablesNameDAO {

    @Autowired
    private SessionFactory sessionFactory;
    //@Autowired
    //private TablesNameDAO tablesNameDAO;

    public List<String> getAllTablesName(String fieldName) {

        List<String> list = new ArrayList<String>();
        SessionImpl sessionImpl = (SessionImpl) sessionFactory.openSession();
        try {

            Connection connection = sessionImpl.connection();
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            String schemaName = connection.getSchema();
            String catalogName = connection.getCatalog();
            
            ResultSet resultSet = databaseMetaData.getColumns(catalogName, schemaName, null, fieldName);
            while (resultSet.next()) {
                list.add(resultSet.getString("TABLE_NAME"));
            }

            TablesNameDAO.close(resultSet, sessionImpl, connection);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;

    }

    public void generateCsvFile(String tableName, String fieldName, String fieldValue) {
        SessionImpl sessionImpl = (SessionImpl) sessionFactory.openSession();

        List<String[]> list = new ArrayList<>();
        try (Connection conn = sessionImpl.connection()){
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE " + fieldName + " = '" + fieldValue + "' ");
            //ResultSet rset = stmt.executeQuery("select * from GT_COMM_TO_CRIT where BUSINESS_UNIT='DEM'");
            ResultSetMetaData rsmd = rset.getMetaData();
            //mwi 2/6/2019 - the following line does not pass linting, but doesn't appear to be needed. The message is:  Always check the return of one of the navigation method (next,previous,first,last) of a ResultSet.
            //rset.next();
            //TODO mwi 2/6/2019 - we need to come up with an actual location to store the files at the customer site
            //Create File directory path
            File dir = new File("C:\\TipQa\\csvFiles");
            if (!dir.exists()) {
                dir.mkdir();
            }
            // Create File in above diretory with fileName
            String fileName = tableName;
            File tagFile = new File(dir, fileName + ".csv");
            if (!tagFile.exists()) {
                tagFile.createNewFile();
            }

            FileWriter fileWriter = new FileWriter(tagFile);
            //Adding table name in the csv file.
            fileWriter.append(tableName);
            //separte the line 
            fileWriter.append(System.getProperty("line.separator"));
            // System.out.println("No of columns in the table:"+ rsmd.getColumnCount());
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                System.out.print(rsmd.getColumnName(i) + " ");

                fileWriter.append(rsmd.getColumnName(i));
                fileWriter.append(",");
                fileWriter.flush();

            }

            //  arr=new String[rsmd.getColumnCount()];
            //  size = rset.getRow();
            //   System.out.println(size);
            System.out.println();//to change the line
            fileWriter.append(System.getProperty("line.separator"));
            while (rset.next()) {

                String[] arr = new String[rsmd.getColumnCount()];
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    //OpenCsv does not handle Blob fields, so we need to convert them to a string
                    if ("BLOB".equals(rsmd.getColumnTypeName(i + 1))) {
                                  
                        if("BLOB".equals(rsmd.getColumnTypeName(i + 1))){
                            
                        Blob blobDbData = rset.getBlob(i + 1);
                        if (blobDbData != null) {
                            int blobLength = (int) blobDbData.length();
                            byte[] blobByteData = blobDbData.getBytes(1, blobLength);
                            blobDbData.free();
                            Base32 encoder = new Base32();
                            String encodedBlob = encoder.encodeToString(blobByteData);
                            arr[i] = encodedBlob;
                        } else {
                            arr[i] = "";
                            }
                        }
                    } else {
                        if("CLOB".equals(rsmd.getColumnTypeName(i + 1)) || "VARCHAR2".equals(rsmd.getColumnTypeName(i + 1)) || "NVARCHAR".equals(rsmd.getColumnTypeName(i + 1))){
                            String dbData = rset.getString(i + 1);
                            if (dbData != null) {
                                String updatedDbString = dbData.replace("\\", "\\\\");
                                arr[i] = updatedDbString;                                    
                            } else {
                                arr[i] = "";
                            }
                        } else {
                            arr[i] = rset.getString(i + 1);
                        }
                    }
                }

                list.add(arr);
            }
            // create CSVWriter object filewriter object as parameter 
            CSVWriter writer = new CSVWriter(fileWriter);
            writer.writeAll(list);

            // closing  connection 
            writer.close();
            stmt.close();
            fileWriter.close();
        
        } catch (Exception e) {
            System.err.println("Unable to connect to database: " + e);
        }

    }
    
    public MultiKeyMap readCSVSaveToDatabse(File file, String businessUnit, Map<String, String> parentTables, MultiKeyMap childTables, Map<String, String> sequenceOnlyTables, MultiKeyMap oldNewDataMap, Boolean okToProcess) {
        SessionImpl sessionImpl = (SessionImpl) sessionFactory.openSession();
        
        try (Connection connection = sessionImpl.connection()){
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            String schemaName = connection.getSchema();
            String catalogName = connection.getCatalog();
            String parentFieldName = "";
            String childsParentFieldName = "";
            String originalBusinessUnit = "";
            String modLinkTable = "";
            // Change from int to Double to get decimal
            Double oldParentFieldValue = 0.0;
            Double newParentFieldValue = 0.0;
            //Reading csv file
            //File file = new File("C:\\TipQA\\csvFiles\\"+fileName+".csv");
         
            CSVReader reader = new CSVReader(new FileReader(file), ',');
            // read line by line
            String[] record = null;
            //Read table name from CSV file
            record = reader.readNext();
            String tableName = null;
            if(record.length > 0) {
                tableName = record[0];
            }
            String columnWithTrigger = "";
            //get list of primary key fields for the table
            ResultSet primaryKeys = databaseMetaData.getPrimaryKeys(catalogName, schemaName, tableName);
            int numPrimaryKeyFields = 0;
            while (primaryKeys.next()) {
                numPrimaryKeyFields++;
                columnWithTrigger = primaryKeys.getString("COLUMN_NAME");
            }
            //if there is 0 or more than 1 primary key field, there are no auto-incremented fields
            if (numPrimaryKeyFields != 1) {
                columnWithTrigger = "";
            }
            
            parentFieldName = parentTables.get(tableName);
            
            //Read Column name form CSV file  
            record = reader.readNext();
            
            Statement stmt = connection.createStatement();
            ResultSet rset = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE BUSINESS_UNIT = '" + businessUnit + "' ");
            int numRecords = 0;
            while (rset.next()) {
                numRecords++;
            }
            //We want to skip some table for now.  For G_DASHBOARD_VALUE_TO_DRILLDOWN, the linking tables do not have business unit,
            //so we are not currenlty pulling the linked data.  G_AGENT_STATUS_V is a view, so does not need to be udpated.
            //The G_UI tables do not need to be inserted for now.  When we do decide to add these, we need to figure out how to update
            //the ELEMENT_ID or we need to change the index on the G_UI_ELEMENTS table.
            //G_AGENTS does not need to be inserted.  We are not currently exporting enough data to recreate the agents.
            //We process G_SKILL_LEVEL_OVERRIDE after all the other tables which is why we are checking the okToProcess flag for that one.
            if ((numRecords > 0) || ("G_DASHBOARD_VALUE_TO_DRILLDOWN".equals(tableName)) || ("G_AGENT_STATUS_V".equals(tableName)) || 
                    ("G_UI_ELEMENTS".equals(tableName)) || ("G_UI_ELEMENTS_REVISIONS".equals(tableName)) || ("G_AGENTS".equals(tableName)) ||
                    (("G_SKILL_LEVEL_OVERRIDE".equals(tableName)) && !okToProcess)) {
                rset.close();
                stmt.close();
                connection.close();
                reader.close();
                return oldNewDataMap;                
            } 
            
            ResultSetMetaData rsmd = rset.getMetaData();
            int columnCount = rsmd.getColumnCount();

            String query = "";
            if (ConnectionStats.USED_DATABASE.equals(ConnectionStats.SUPPORTED_DATABASES.ORACLE) && (parentFieldName != null)){
                query = " BEGIN INSERT INTO " + tableName + " VALUES (";
            } else {
                query = "INSERT INTO " + tableName + " VALUES (";                
            }
            StringBuilder queryBuilder = new StringBuilder(query);
            for (int i = 0; i < columnCount; i++) {
                queryBuilder.append(" ?");
                if (i != columnCount - 1) {
                    queryBuilder.append(",");
                }
            }
            queryBuilder.append(")");
            if (ConnectionStats.USED_DATABASE.equals(ConnectionStats.SUPPORTED_DATABASES.ORACLE) && (parentFieldName != null)){
                queryBuilder.append(" returning " + parentFieldName + " into :newParentFieldValue; END;");
            }
            query = queryBuilder.toString();
            PreparedStatement Pstmt = connection.prepareStatement(query);
            CallableStatement oraclePstmt = null;
            if (ConnectionStats.USED_DATABASE.equals(ConnectionStats.SUPPORTED_DATABASES.ORACLE) && (parentFieldName != null)){
                oraclePstmt = connection.prepareCall(query);
                oraclePstmt.registerOutParameter(columnCount + 1, OracleTypes.NUMBER);
            }
            System.out.println("Query created Successfully");

            int j = 0;
            //Read Record from CSV file
            while ((record = reader.readNext()) != null) {

                for (int i = 1; i <= columnCount; i++) {
                    //read column type from metadata
                    String columnType = rsmd.getColumnTypeName(i);
                    //read the Column name and match with the primary key column of the table
                    String columnName = rsmd.getColumnName(i);
                    // Read the record 
                    String csvData = record[j];
                    
                    if (("BUSINESS_UNIT".equals(columnName)) && (businessUnit != null)){
                        originalBusinessUnit = csvData;
                        csvData = businessUnit;
                    }
                    
                    if ((("CRIT_BUSINESS_UNIT".equals(columnName)) || ("DMSN_BUSINESS_UNIT".equals(columnName)) || 
                            ("ORIG_BUSINESS_UNIT".equals(columnName)) || ("ORIGINATING_BUSINESS_UNIT".equals(columnName))) &&
                            csvData.equals(originalBusinessUnit)) {
                        csvData = businessUnit;
                    }
                    
                    if (columnName.equals(parentFieldName)) {
                        // Change from int to Double to get decimal
                        Double currentValue = convertToDouble(csvData);
                        oldParentFieldValue = currentValue;
                    }
                    
                    if(columnWithTrigger.equals(columnName)){
                        int primaryKeyValue = 0;
                        if (ConnectionStats.USED_DATABASE.equals(ConnectionStats.SUPPORTED_DATABASES.ORACLE)) {
                            String sequenceName = sequenceOnlyTables.get(tableName);
                            if (sequenceName != null) {
                                Statement sequenceStmt = connection.createStatement();
                                ResultSet sequenceValue = sequenceStmt.executeQuery("SELECT " + sequenceName + ".NEXTVAL from dual");
                                if (sequenceValue.next()) {
                                    primaryKeyValue = sequenceValue.getInt(1);
                                } 
                                sequenceValue.close();
                                sequenceStmt.close();
                            }
                        }
                        if (oraclePstmt != null){
                            oraclePstmt.setInt(i, primaryKeyValue);
                        } else {
                            Pstmt.setInt(i, primaryKeyValue); 
                        }
                    }
                    else{
                    switch (columnType) {
                        case "VARCHAR2": case "NVARCHAR":
                            if (("G_SKILL_LEVEL_OVERRIDE".equals(tableName)) && ("MOD_LINK_FROM".equals(columnName))){
                                modLinkTable = csvData;                                 
                            }
                            if (oraclePstmt != null){
                                oraclePstmt.setString(i, csvData);
                            } else {
                                Pstmt.setString(i, csvData);
                            }
                            break;
                        case "NUMBER": case "NUMERIC":
                            // Change from int to Double to get decimal
                            Double number=convertToDouble(csvData);
                            if(number==null){
                                if (oraclePstmt != null){
                                    oraclePstmt.setObject(i,number);                                    
                                } else {
                                    Pstmt.setObject(i,number);
                                }
                            }else{
                                if (("G_SKILL_LEVEL_OVERRIDE".equals(tableName))) {
                                    childsParentFieldName = modLinkTable;
                                } else {
                                    childsParentFieldName = (String) childTables.get(tableName, columnName);
                                }
                                if (childsParentFieldName != null) {
                                    Double newChildValue = (Double) oldNewDataMap.get(childsParentFieldName, number);
                                    if (newChildValue != null) {
                                        number = newChildValue;
                                    }
                                }
                                if (oraclePstmt != null){
                                    oraclePstmt.setDouble(i, number);                                    
                                } else {
                                    Pstmt.setDouble(i, number);
                                }
                            }
                            //int result = Integer.parseInt(csvData);                            
                            break;
                        case "DATE": case "DATETIME":
                            if(("null".equals(csvData)) || (csvData.isEmpty())){
                              // Date date =null; 
                              if (oraclePstmt != null){
                                  oraclePstmt.setDate(i, null);                                  
                              } else {
                                  Pstmt.setDate(i, null); 
                              }
                            }
                            else{
                                //String strDate=stringDate(csvData);
                                Date date = convertDate(csvData);
                                java.sql.Date dat=new java.sql.Date(date.getTime());
                                if (oraclePstmt != null){
                                    oraclePstmt.setDate(i,dat);
                                } else {
                                    Pstmt.setDate(i,dat);
                                }
                            }
                            break;
                        case "CLOB":
                            
                            Clob clobData = connection.createClob();
                            clobData.setString(1, csvData);
                            if (oraclePstmt != null){
                                oraclePstmt.setClob(i, clobData);
                            } else {
                                Pstmt.setClob(i, clobData);
                            }
                            break;
                        case "BLOB":
                            if(("null".equals(csvData)) || (csvData.isEmpty())){
                                if (oraclePstmt != null){
                                    oraclePstmt.setObject(i, csvData);
                                } else {
                                    Pstmt.setObject(i, csvData); 
                                }
                            } else {
                                Blob blobData = connection.createBlob();  
                                String encodedBlob = csvData;
                                Base32 encoder = new Base32();
                                byte[] blobByteData = encoder.decode(encodedBlob);
                                blobData.setBytes(1, blobByteData);
                                if (oraclePstmt != null){
                                    oraclePstmt.setBlob(i, blobData);
                                } else {
                                    Pstmt.setBlob(i, blobData);  
                                }
                            }
                            break;
                        default:
                            break;
                    }
                }
                    if (j < columnCount - 1) {
                        j++;
                    }
                }//end of for Loop 
                if (oraclePstmt != null){
                    oraclePstmt.execute();
                    newParentFieldValue = oraclePstmt.getDouble(columnCount + 1);  
                    oldNewDataMap.put(parentFieldName, oldParentFieldValue, newParentFieldValue);
                } else {
                    // Add it to the batch
                    Pstmt.addBatch();
                }
                j=0;
                //if (oraclePstmt == null){
                //    System.out.println("record added to the batch");
                //}
            }//end of while
            
            if (oraclePstmt == null){
                System.out.println("Before executing the batch");
                int[] updatedRowCount = Pstmt.executeBatch();
                int len = updatedRowCount.length;
                System.out.println("Batch executed successfully ");
                System.out.println(len + " Row Updated");
            }
            System.out.println(tableName + " Csv Imported Successfully");
            
            rset.close();
            stmt.close();
            Pstmt.close();
            connection.close();
            reader.close();
            return oldNewDataMap;

        } catch (Throwable e) {
            e.printStackTrace();
            return oldNewDataMap;
        }
    }
    
    public static Date convertDate(String date) throws ParseException {
        Date dbDate = null;
        // Date dbDate=new SimpleDateFormat("MM/dd/yyyy").parse(date);
        // Date dbDate=new SimpleDateFormat("MM/dd/yyyy HH:mm").parse(date);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        //SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm", new Locale("es", "ES"));
        //  dbDate=new SimpleDateFormat("yyyy-MM-dd H:mm:ss", new Locale("es", "ES")).parse(date);
        dbDate = formatter.parse(date);
//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd", Locale.US); 
//        Date dbDate = new java.sql.Date(df.parse(date).getTime());

        return dbDate;
    }
    public static String stringDate(String date){
        String[] arr=date.split(" ");
        return arr[0];
    }
    public static Double convertToDouble(String value){
        Double number=0.0;
        if(value != null){
            if(value.isEmpty()){
               return null; 
            }
           number = Double.parseDouble(value);
          //  return number;            
        }
       return number;
    }
     public static String clobToString(Clob clobData) {
        StringBuilder sb = new StringBuilder();
        
        try {
            Reader reader = clobData.getCharacterStream();
            BufferedReader br = new BufferedReader(reader);

            String line;
            while (null != (line = br.readLine())) {
                sb.append(line);
            }
            br.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    private static void close(ResultSet resultSet, SessionImpl session, Connection connection) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (session != null) {
                    try {
                        session.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
    }
}
