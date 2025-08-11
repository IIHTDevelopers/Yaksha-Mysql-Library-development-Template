package com.example;

import org.junit.jupiter.api.*;
import java.sql.*;

import java.util.*;

import static com.example.utils.TestUtils.businessTestFile;
import static com.example.utils.TestUtils.currentTest;
import static com.example.utils.TestUtils.testReport;
import static com.example.utils.TestUtils.yakshaAssert;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MySQLDatabaseTest {

   static Connection conn;

    @BeforeAll
    public static void init() throws Exception {
        try {
            conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/LibraryDB?serverTimezone=UTC",
                "root",
                "pass@word1"
            );
            yakshaAssert("testDBConnection", conn != null, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert("testDBConnection", false, businessTestFile);
        }
    }

    // ---------- Helpers ----------
    private boolean schemaExists(String schema) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean tableExists(String table) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, table, null)) {
            return rs.next();
        }
    }

    private boolean rowExists(String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) ps.setObject(i + 1, params[i]);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean callProcedure(String callSyntax) {
        try (CallableStatement cs = conn.prepareCall(callSyntax)) {
            boolean hasResults = cs.execute();
            // Consider success if it yields a result set or an update count, or just executes without error
            if (hasResults) return true;
            int updateCount = cs.getUpdateCount();
            if (updateCount != -1) return true;
            // Consume extras in case proc returns multiple
            while (cs.getMoreResults() || cs.getUpdateCount() != -1) { /* no-op */ }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ---------- Tests ----------

    @Test
    public void testDatabaseExists() throws Exception {
        try {
            boolean exists = schemaExists("LibraryDB");
            yakshaAssert(currentTest(), exists, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void testTableExists() throws Exception {
        try {
            List<String> expectedTables = Arrays.asList("Members", "Books", "BorrowTransactions");
            boolean allExist = true;
            for (String t : expectedTables) {
                if (!tableExists(t)) { allExist = false; break; }
            }
            yakshaAssert(currentTest(), allExist, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    public void testSeedDataExists() throws Exception {
        try {
            boolean ok =
                // Members
                rowExists("SELECT 1 FROM Members WHERE FullName = ? AND Email = ? AND JoinDate = ?", "Alice Johnson", "alice@example.com", java.sql.Date.valueOf("2023-01-10")) &&
                rowExists("SELECT 1 FROM Members WHERE FullName = ? AND Email = ? AND JoinDate = ?", "Bob Smith", "bob@example.com", java.sql.Date.valueOf("2023-02-15")) &&
                // Books
                rowExists("SELECT 1 FROM Books WHERE Title = ? AND Author = ? AND Genre = ? AND PublishedYear = ?", "The Great Gatsby", "F. Scott Fitzgerald", "Fiction", 1925) &&
                rowExists("SELECT 1 FROM Books WHERE Title = ? AND Author = ? AND Genre = ? AND PublishedYear = ?", "Clean Code", "Robert C. Martin", "Programming", 2008) &&
                // BorrowTransactions
                rowExists("SELECT 1 FROM BorrowTransactions WHERE MemberID = ? AND BookID = ? AND BorrowDate = ? AND ReturnDate = ?", 1, 1, java.sql.Date.valueOf("2023-03-01"), java.sql.Date.valueOf("2023-03-15")) &&
                rowExists("SELECT 1 FROM BorrowTransactions WHERE MemberID = ? AND BookID = ? AND BorrowDate = ? AND ReturnDate IS NULL", 2, 2, java.sql.Date.valueOf("2023-03-05"));
            yakshaAssert(currentTest(), ok, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert(currentTest(), false, businessTestFile);
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

            boolean allExist = true;
            for (String proc : procedures) {
                try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES " +
                    "WHERE ROUTINE_SCHEMA = 'LibraryDB' AND ROUTINE_NAME = ?")) {
                    ps.setString(1, proc);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) { allExist = false; break; }
                    }
                }
            }
            yakshaAssert(currentTest(), allExist, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    // ---------- Procedure body/content checks (kept exactly like your style) ----------

    private String getProcedureBody(String procName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'LibraryDB' AND ROUTINE_NAME = ?")) {
            ps.setString(1, procName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("ROUTINE_DEFINITION").toUpperCase();
            }
        }
        return "";
    }
    
    @Test
    public void test_sp_AggregateStats_Content() throws Exception {
        try {
            String body = getProcedureBody("sp_AggregateStats");
            boolean status = body.contains("COUNT") && body.contains("MAX") && body.contains("MIN");
            yakshaAssert(currentTest(), status, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        try {
            if (conn != null && !conn.isClosed()) conn.close();
        } catch (Exception ignored) { }
    }
}
