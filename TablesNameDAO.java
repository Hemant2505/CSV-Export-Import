// LINT_TAG 3055E9610480E169B414DBAD0A8C6C9AB2DF03D6FE7F2BE12DA96050304F9FA
package com.tiptech.tipqadatamodel.dao;


import com.opencsv.CSVWriter;
import com.tiptech.tipqadatamodel.domain.GtCommToCrit;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.Session;
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
    
    private GtCommToCrit gtCommToCrit;

    public List<String> getAllTablesName() {

        List<String> list = new ArrayList<String>();
        String tableName = null;
        SessionImpl sessionImpl = (SessionImpl) sessionFactory.openSession();
        try {
            //final SessionFactoryImplementor sessionFactoryImplementor = (SessionFactoryImplementor) sessionFactory;
            //  Dialect dialect = sessionFactoryImplementor.getDialect();
            //String catalog = sessionFactoryImplementor.getSettings().getConnectionProvider() 
            //.getConnectionProvider().getConnection().getCatalog();

            Connection connection = sessionImpl.connection();
            
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet resultSet = databaseMetaData.getTables(null, null,null, new String[]{"TABLE"});
            while (resultSet.next()) {
                tableName = resultSet.getString(3);
                // list.add(tableName);
                System.out.println(tableName);
                break;
            }
            ResultSet result = databaseMetaData.getColumns(null, null, tableName, "BUSINESS_UNIT");
            while (result.next()) {
               // String columnName = result.getString(4);
               if(! list.contains(tableName)){
                   list.add(result.getString("TABLE_NAME"));
               }
               //  list.add(result.getString("TABLE_NAME"));
                 System.out.println(result.getString("COLUMN_NAME"));
//                if ("BUSINESS_UNIT".equals (columnName)) {
//                    list.add(tableName);
//                } else {
//                }
//                System.out.println(columnName);
               // tablesNameDAO.writeCSVFile(sessionImpl, tableName);
                break;
            }

            TablesNameDAO.close(resultSet, sessionImpl, connection);
               
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;

    }

    public void writeCSVFile(String tableName) throws IOException {

        //Session session=sessionFactory.getCurrentSession();
        // first create file object for file placed at location 
        // specified by filepath 
        //String filePath = "D:\\pankaj\\Personal\\DEMRecords.csv";
           
    //Create File directory path
            File dir=new File("D:\\pankaj\\Personal");
                if(!dir.exists()){
                    dir.mkdir();}
              // Create File in above diretory with fileName
              String fileName=tableName;
                File tagFile=new File(dir,fileName+".csv");
                        if(!tagFile.exists()){
                            tagFile.createNewFile();
                            }
        
                        
        /*String sql = "SELECT * FROM " + tableName + " WHERE BUSINESS_UNIT='DEM'";
        SQLQuery query = session.createSQLQuery(sql);
        // Create List to storage the records
        List list = query.list();
        
        //GT_COMM_TO_CRIT - GtCommToCrit

        // System.out.println(list.get(0));
        // System.out.println(list.get(1));
      //  File file = new File(filePath);
        try {
            // create FileWriter object with file as parameter 
           // FileWriter fileWriter = new FileWriter(file);
            FileWriter fileWriter = new FileWriter(tagFile);            
            // create CSVWriter object filewriter object as parameter 
            //CSVWriter writer = new CSVWriter(fileWriter);
            //writer.writeAll(list);

            // closing writer connection 
            //writer.close();
            
        } catch (IOException e) {
           
            e.printStackTrace();
        }*/

    }
    
        // public Class<?> getTypeForTableName(String tableName) {
    //    for (EntityType<?> entityType : getEntityManager().getMetamodel().getEntities()) {
    //        if (entityType.getJavaType().getAnnotation(Table.class).name().equals(tableName)) {
    //            return entityType.getJavaType();
   //         }
    //    }
   ///     return null;
  //  }   
//    
//     public void readCSVFile(String tableName) {
//
//        try {
//              String fileName="D:\\pankaj\\Personal\\"+tableName+".csv";
//            // Create an object of filereader 
//            // class with CSV file as a parameter. 
//            FileReader filereader = new FileReader(fileName);
//
//            // create csvReader object passing 
//            // file reader as a parameter 
//            // CSVReader csvReader = new CSVReader(filereader); 
//            //Create CsvReader object with skip first line of csv file because first line is column names
//            CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(1).build();
//            String[] nextRecord;
//
//            // we are going to read data line by line 
//            while ((nextRecord = csvReader.readNext()) != null) {
//
//                // System.out.println(nextRecord.length);
//                //  break;
//                for (String cell : nextRecord) {
//
//                    System.out.print(cell + "\t");
//                }
//                System.out.println();
//            }
//            //To import csv file into database
//            /*
//        Class.forName("oracle.jdbc.driver.OracleDriver");
//            String url = "jdbc:oracle:thin:@ora12c:1521:ORA12C";
//            Connection conn = DriverManager.getConnection(url, "dev_710",
//                    "dev_710");
//            conn.setAutoCommit(false);
//            Statement stmt = conn.createStatement();
//            
//            // Load the data
//        //  String filename = filename;
//          String tablename = "GT_COMM_TO_CRIT";
//
//      stmt.executeUpdate("LOAD DATA INFILE \"" + filename + "\" INTO TABLE IGNORE 1 LINES"+ tablename + " FIELDS TERMINATED BY ','");
//          
//          System.out.println("Data inserted into table");
//             */
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }   
           
     
     public void saveDataToDatabase(String fileName){
         Session session=sessionFactory.openSession();
         List<GtCommToCrit> list= gtCommToCrit.readGtCommToCritFromCSV(fileName);
         for(GtCommToCrit gtComm: list){
             session.save(gtComm);
         }
         
         session.close();
     }
     
     public void generateCsvFile(String tableName,String businessUnit) {
        SessionImpl sessionImpl = (SessionImpl) sessionFactory.openSession();
        
        List<String[]> list = new ArrayList<>();
        try {
            Connection conn = sessionImpl.connection();
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE BUSINESS_UNIT ='"+businessUnit+"' ");
            //ResultSet rset = stmt.executeQuery("select * from GT_COMM_TO_CRIT where BUSINESS_UNIT='DEM'");
            ResultSetMetaData rsmd = rset.getMetaData();
            rset.next();
            //Create File directory path
            File dir=new File("D:\\pankaj\\Personal");
                if(!dir.exists()){
                    dir.mkdir();}
              // Create File in above diretory with fileName
              String fileName="GT_COMM_TO_CRIT";
                File tagFile=new File(dir,fileName+".csv");
                        if(!tagFile.exists()){
                            tagFile.createNewFile();
                            }
                
            FileWriter fileWriter = new FileWriter(tagFile);
            //Adding table name in the csv file.
            fileWriter.append("GT_COMM_TO_CRIT");
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
                    arr[i] = rset.getString(i + 1);
                }

                list.add(arr);
                System.out.println(rset.getString(1) + " " + rset.getString(2)
                        + " " + rset.getString(3) + " " + rset.getString(4) + " " + rset.getString(5));
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

     
     
    private static void close(ResultSet rs, SessionImpl session, Connection con) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (session != null) {
                    try {
                        session.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (con != null) {
                            try {
                                con.close();
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
