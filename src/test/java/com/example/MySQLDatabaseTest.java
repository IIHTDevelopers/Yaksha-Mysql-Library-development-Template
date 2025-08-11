package com.example;

import org.junit.jupiter.api.*;
import java.sql.*;

import static com.example.utils.TestUtils.businessTestFile;
import static com.example.utils.TestUtils.currentTest;
import static com.example.utils.TestUtils.testReport;
import static com.example.utils.TestUtils.yakshaAssert;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MySQLDatabaseTest {

    private static final String URL = "jdbc:mysql://localhost:3306/empdb?serverTimezone=UTC";
    private static final String USER = "root";
    private static final String PASSWORD = "pass@word1";

    private static Connection connection;

    @BeforeAll
    public static void setup() throws SQLException {
        connection = DriverManager.getConnection(URL, USER, PASSWORD);
    }

    @AfterAll
    public static void teardown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    @Order(1)
    public void testDatabaseExists() throws Exception {
        String query = "SHOW DATABASES LIKE 'empdb'";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
                yakshaAssert(currentTest(), rs.next(), businessTestFile);
           // assertTrue(rs.next(), "Database 'empdb' does not exist");
        }catch(Exception ex){
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(2)
    public void testTableExists() throws Exception {
        String query = "SHOW TABLES IN empdb LIKE 'empmaster'";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            yakshaAssert(currentTest(), rs.next(), businessTestFile);
           // assertTrue(rs.next(), "Database 'empdb' does not exist");
        }catch(Exception ex){
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(3)
    public void testTableSchema() throws Exception {
        String query = "DESCRIBE empdb.empmaster";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            String[][] expectedSchema = {
                {"ID", "int"},
                {"empname", "text"},
                {"empemail", "text"},
                {"dept", "text"},
                {"age", "int"}
            };
            int i = 0;
            while (rs.next()) {
              
                 yakshaAssert(currentTest(), (expectedSchema[i][0].equals(rs.getString("Field")) && rs.getString("Type").startsWith(expectedSchema[i][1])), businessTestFile);
               // assertEquals(expectedSchema[i][0], rs.getString("Field"));
                //assertTrue(rs.getString("Type").startsWith(expectedSchema[i][1]),"Mismatch in field type for " + expectedSchema[i][0]);
                i++;
            }
            //assertEquals(expectedSchema.length, i, "Schema field count does not match");
        }catch(Exception ex){
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }


    

    @Test
    @Order(4)
    public void testRecordExists() throws Exception {
        String query = "SELECT * FROM empdb.empmaster WHERE ID = ? AND empname = ? AND empemail = ? AND dept = ? AND age = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            stmt.setString(2, "David");
            stmt.setString(3, "david@gmail.com");
            stmt.setString(4, "Finance");
            stmt.setInt(5, 40);

            try (ResultSet rs = stmt.executeQuery()) {
                yakshaAssert(currentTest(), rs.next(), businessTestFile);
                //assertTrue(rs.next(), "Record with given values does not exist in 'empmaster'");
            }
        }catch(Exception ex){
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    
    @Test
     @Order(5)
    public void testProcedureExists() throws Exception {
        String checkProcedure = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE' AND ROUTINE_SCHEMA='empdb' AND ROUTINE_NAME='GetEmployeesWithBonusAboveAge'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(checkProcedure)) {

            yakshaAssert(currentTest(), rs.next(), businessTestFile);
        }catch(Exception ex){
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
     @Order(6)
    public void testProcedureOutput() throws Exception {
        try{
        CallableStatement stmt = connection.prepareCall("{CALL GetEmployeesWithBonusAboveAge(?)}");
        stmt.setInt(1, 30);

        ResultSet rs = stmt.executeQuery();

        boolean atLeastOne = false;
        while (rs.next()) {
            
            int age = rs.getInt("age");
            int expectedBonus = age * 2500;
            int actualBonus = rs.getInt("bonus");

            if(age > 30 && expectedBonus == actualBonus){
                atLeastOne = true;
                break;
            }
        }
         System.out.println(atLeastOne);
        yakshaAssert(currentTest(), atLeastOne, businessTestFile);
        rs.close();
        stmt.close();
        }catch(Exception ex){
            System.out.println("EX :" + ex);
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }
}
