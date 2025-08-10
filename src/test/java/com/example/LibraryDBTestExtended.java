package com.example;

import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

import static com.example.utils.TestUtils.businessTestFile;
import static com.example.utils.TestUtils.currentTest;
import static com.example.utils.TestUtils.testReport;
import static com.example.utils.TestUtils.yakshaAssert;


public class LibraryDBTestExtended {

    static Connection conn;

    @BeforeAll
    public static void init() throws Exception {
        try {
    	conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/LibraryDB?serverTimezone=UTC", "root", "pass@word1");
    	yakshaAssert("testDBConnection", conn!=null, businessTestFile);
    	//assertNotNull(conn, "Database connection should be established");
        }catch(Exception ex) {
        	yakshaAssert("testDBConnection", false, businessTestFile);
        }
    }

    @Test
    public void testTableExists() throws Exception {
        try
        {
    	DatabaseMetaData meta = conn.getMetaData();
        List<String> expectedTables = Arrays.asList("Members", "Books", "BorrowTransactions");
        boolean tableExists = true;
        for (String table : expectedTables) {
            ResultSet rs = meta.getTables(null, null, table, null);
            if(!rs.next())
            {
            	tableExists = false;
            	break;
            }
            
            //assertTrue(rs.next(), "Table " + table + " should exist");
        }
        yakshaAssert(currentTest(), tableExists, businessTestFile);
     
        }catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void testSeedDataExists() throws Exception {
    	try {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM Members WHERE FullName = 'Alice Johnson'");
        yakshaAssert(currentTest(), rs.next(), businessTestFile);
        //assertTrue(rs.next(), "Alice Johnson should exist");

       
    	}catch(Exception ex) {
        	yakshaAssert(currentTest() , false, businessTestFile);
        }
    }

    @Test
    public void testProcedureExists() throws Exception {
    	try {
        List<String> procedures = List.of(
            "sp_AggregateStats", "sp_StringOperations", "sp_JoinReports",
            "sp_OperatorExamples", "sp_FormattedDates", "sp_WildcardExamples",
            "sp_ClauseExamples", "sp_RunAllReports"
        );

        boolean procedureExists = true;
        for (String proc : procedures) {
            PreparedStatement ps = conn.prepareStatement(
                "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'LibraryDB' AND ROUTINE_NAME = ?");
            ps.setString(1, proc);
            ResultSet rs = ps.executeQuery();
            if(!rs.next()) {
            	procedureExists = false;
            	break;
            }
            //assertTrue(rs.next(), "Procedure '" + proc + "' should exist");
        }
        yakshaAssert(currentTest(), procedureExists, businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    private String getProcedureBody(String procName) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(
            "SELECT ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'LibraryDB' AND ROUTINE_NAME = ?");
        ps.setString(1, procName);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return rs.getString("ROUTINE_DEFINITION").toUpperCase();
        }
        return "";
    }

    @Test
    public void test_sp_AggregateStats_Content() throws Exception {
    	try {
        String body = getProcedureBody("sp_AggregateStats");
        /*assertAll("sp_AggregateStats should contain aggregate functions",
            () -> assertTrue(body.contains("COUNT"), "Should contain COUNT"),
            () -> assertTrue(body.contains("MAX"), "Should contain MAX"),
            () -> assertTrue(body.contains("MIN"), "Should contain MIN")
        );*/
        boolean status = body.contains("COUNT") && body.contains("MAX") && body.contains("MIN");
        yakshaAssert(currentTest(), status, businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void test_sp_StringOperations_Content() throws Exception {
    	try {
    	String body = getProcedureBody("sp_StringOperations");
        /*assertAll("sp_StringOperations should contain string functions",
            () -> assertTrue(body.contains("SUBSTRING"), "Should contain SUBSTRING"),
            () -> assertTrue(body.contains("CONCAT"), "Should contain CONCAT")
        );*/
        boolean status = body.contains("SUBSTRING") && body.contains("CONCAT");
        yakshaAssert(currentTest(), status, businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void test_sp_JoinReports_Content() throws Exception {
    	try {
        String body = getProcedureBody("sp_JoinReports");
        /*assertAll("sp_JoinReports should contain joins",
            () -> assertTrue(body.contains("INNER JOIN"), "Should contain INNER JOIN"),
            () -> assertTrue(body.contains("LEFT JOIN"), "Should contain LEFT JOIN"),
            () -> assertTrue(body.contains("RIGHT JOIN"), "Should contain RIGHT JOIN")
        );*/
        boolean status = body.contains("INNER JOIN") && body.contains("LEFT JOIN")&&body.contains("RIGHT JOIN");
        yakshaAssert(currentTest(), status, businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void test_sp_OperatorExamples_Content() throws Exception {
    	try {
        String body = getProcedureBody("sp_OperatorExamples");
        /*assertAll("sp_OperatorExamples should contain logical operators",
            () -> assertTrue(body.contains("LIKE"), "Should contain LIKE"),
            () -> assertTrue(body.contains("AND"), "Should contain AND"),
            () -> assertTrue(body.contains("OR"), "Should contain OR")
        );*/
        boolean status = body.contains("LIKE") && body.contains("AND")&&body.contains("OR");
        yakshaAssert(currentTest(), status, businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void test_sp_FormattedDates_Content() throws Exception {
    	try {
        String body = getProcedureBody("sp_FormattedDates");
        assertTrue(body.contains("DATE_FORMAT"), "Should contain DATE_FORMAT");
        yakshaAssert(currentTest(), body.contains("DATE_FORMAT"), businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void test_sp_WildcardExamples_Content() throws Exception {
    	try {
        String body = getProcedureBody("sp_WildcardExamples");
       /* assertAll("sp_WildcardExamples should contain wildcards",
            () -> assertTrue(body.contains("LIKE"), "Should contain LIKE"),
            () -> assertTrue(body.contains("%") || body.contains("_"), "Should contain wildcard symbols")
        );*/
        boolean status = body.contains("LIKE") && (body.contains("%") || body.contains("_"));
        yakshaAssert(currentTest(), status, businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void test_sp_ClauseExamples_Content() throws Exception {
    	try {
        String body = getProcedureBody("sp_ClauseExamples");
        assertAll("sp_ClauseExamples should contain SQL clauses",
            () -> assertTrue(body.contains("GROUP BY"), "Should contain GROUP BY"),
            () -> assertTrue(body.contains("ORDER BY"), "Should contain ORDER BY"),
            () -> assertTrue(body.contains("DISTINCT"), "Should contain DISTINCT")
        );
        boolean status = body.contains("GROUP BY") && body.contains("ORDER BY") && body.contains("DISTINCT");
        yakshaAssert(currentTest(), status, businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void test_sp_RunAllReports_Content() throws Exception {
    	try {
        String body = getProcedureBody("sp_RunAllReports");
        /*assertAll("sp_RunAllReports should call all procedures",
            () -> assertTrue(body.contains("CALL SP_AGGREGATESTATS"), "Should call sp_AggregateStats"),
            () -> assertTrue(body.contains("CALL SP_STRINGOPERATIONS"), "Should call sp_StringOperations"),
            () -> assertTrue(body.contains("CALL SP_JOINREPORTS"), "Should call sp_JoinReports"),
            () -> assertTrue(body.contains("CALL SP_OPERATOREXAMPLES"), "Should call sp_OperatorExamples"),
            () -> assertTrue(body.contains("CALL SP_FORMATTEDDATES"), "Should call sp_FormattedDates"),
            () -> assertTrue(body.contains("CALL SP_WILDCARDEXAMPLES"), "Should call sp_WildcardExamples"),
            () -> assertTrue(body.contains("CALL SP_CLAUSEEXAMPLES"), "Should call sp_ClauseExamples")
        );*/
        boolean status = body.contains("CALL SP_AGGREGATESTATS") && body.contains("CALL SP_STRINGOPERATIONS") && body.contains("CALL SP_JOINREPORTS") && body.contains("CALL SP_OPERATOREXAMPLES") && body.contains("CALL SP_FORMATTEDDATES") && body.contains("CALL SP_WILDCARDEXAMPLES") && body.contains("CALL SP_CLAUSEEXAMPLES");
        yakshaAssert(currentTest(), status, businessTestFile);
    	}catch(Exception ex) {
        	yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}
