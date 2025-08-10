package com.example;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit tests for the "LibraryDB" SQL assessment.
 *
 * What it checks:
 * 1) Database exists
 * 2) Tables exist with correct names
 * 3) Sample data present (exact counts and key values)
 * 4) Required stored procedures exist
 * 5) Stored procedures execute successfully
 *
 * Optional deep checks (toward the end) assert specific result values produced by SPs
 * based on the given sample data. If students change column aliases or shapes, you can
 * keep the smoke tests and disable deep checks.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LibraryDBAssessmentTest {

    // ---- Connection config (override with -Ddb.host=... etc.) ----
    private static final String HOST = System.getProperty("db.host", "localhost");
    private static final String PORT = System.getProperty("db.port", "3306");
    private static final String USER = System.getProperty("db.user", "root");
    private static final String PASS = System.getProperty("db.pass", "");
    private static final String DB_NAME = "LibraryDB";

    private static String serverUrl() {
        // Connect to server-level (no default DB) for INFORMATION_SCHEMA checks
        return "jdbc:mysql://" + HOST + ":" + PORT + "/?useSSL=false&allowMultiQueries=true&serverTimezone=UTC";
    }
    private static String dbUrl() {
        return "jdbc:mysql://" + HOST + ":" + PORT + "/" + DB_NAME + "?useSSL=false&allowMultiQueries=true&serverTimezone=UTC";
    }

    private static Connection newServerConnection() throws SQLException {
        return DriverManager.getConnection(serverUrl(), USER, PASS);
    }
    private static Connection newDbConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl(), USER, PASS);
    }

    // ---- Helpers ----
    private static boolean schemaExists(Connection conn, String schema) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static boolean tableExists(Connection conn, String schema, String table) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static int countRows(Connection conn, String table) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static boolean procedureExists(Connection conn, String schema, String procName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES " +
                "WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE='PROCEDURE' AND ROUTINE_NAME = ?")) {
            ps.setString(1, schema);
            ps.setString(2, procName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static List<Map<String,Object>> queryToList(ResultSet rs) throws SQLException {
        List<Map<String,Object>> rows = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        while (rs.next()) {
            Map<String,Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= cols; i++) {
                row.put(md.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    // ---- TESTS ----

    @Test
    @Order(1)
    @DisplayName("1) Database 'LibraryDB' exists")
    void testDatabaseExists() throws Exception {
        try (Connection conn = newServerConnection()) {
            assertTrue(schemaExists(conn, DB_NAME), "Database 'LibraryDB' was not found.");
        }
    }

    @Test
    @Order(2)
    @DisplayName("2) Required tables exist: Members, Books, BorrowTransactions")
    void testTablesExist() throws Exception {
        try (Connection conn = newDbConnection()) {
            assertTrue(tableExists(conn, DB_NAME, "Members"), "Table 'Members' missing.");
            assertTrue(tableExists(conn, DB_NAME, "Books"), "Table 'Books' missing.");
            assertTrue(tableExists(conn, DB_NAME, "BorrowTransactions"), "Table 'BorrowTransactions' missing.");
        }
    }

    @Test
    @Order(3)
    @DisplayName("3) Sample data inserted correctly")
    void testSampleDataInserted() throws Exception {
        try (Connection conn = newDbConnection()) {
            // Counts
            assertEquals(2, countRows(conn, "Members"), "Members should have 2 rows.");
            assertEquals(2, countRows(conn, "Books"), "Books should have 2 rows.");
            assertEquals(2, countRows(conn, "BorrowTransactions"), "BorrowTransactions should have 2 rows.");

            // Key value spot-checks
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT FullName, Email, JoinDate FROM Members ORDER BY MemberID")) {
                try (ResultSet rs = ps.executeQuery()) {
                    List<Map<String,Object>> rows = queryToList(rs);
                    assertEquals("Alice Johnson", rows.get(0).get("FullName"));
                    assertEquals("alice@example.com", rows.get(0).get("Email"));
                    assertEquals(Date.valueOf("2023-01-10"), rows.get(0).get("JoinDate"));
                    assertEquals("Bob Smith", rows.get(1).get("FullName"));
                    assertEquals("bob@example.com", rows.get(1).get("Email"));
                    assertEquals(Date.valueOf("2023-02-15"), rows.get(1).get("JoinDate"));
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT Title, Author, Genre, PublishedYear FROM Books ORDER BY BookID")) {
                try (ResultSet rs = ps.executeQuery()) {
                    List<Map<String,Object>> rows = queryToList(rs);
                    assertEquals("The Great Gatsby", rows.get(0).get("Title"));
                    assertEquals("F. Scott Fitzgerald", rows.get(0).get("Author"));
                    assertEquals("Fiction", rows.get(0).get("Genre"));
                    assertEquals(1925, ((Number) rows.get(0).get("PublishedYear")).intValue());

                    assertEquals("Clean Code", rows.get(1).get("Title"));
                    assertEquals("Robert C. Martin", rows.get(1).get("Author"));
                    assertEquals("Programming", rows.get(1).get("Genre"));
                    assertEquals(2008, ((Number) rows.get(1).get("PublishedYear")).intValue());
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT MemberID, BookID, BorrowDate, ReturnDate FROM BorrowTransactions ORDER BY TransactionID")) {
                try (ResultSet rs = ps.executeQuery()) {
                    List<Map<String,Object>> rows = queryToList(rs);
                    assertEquals(1, ((Number) rows.get(0).get("MemberID")).intValue());
                    assertEquals(1, ((Number) rows.get(0).get("BookID")).intValue());
                    assertEquals(Date.valueOf("2023-03-01"), rows.get(0).get("BorrowDate"));
                    assertEquals(Date.valueOf("2023-03-15"), rows.get(0).get("ReturnDate"));

                    assertEquals(2, ((Number) rows.get(1).get("MemberID")).intValue());
                    assertEquals(2, ((Number) rows.get(1).get("BookID")).intValue());
                    assertEquals(Date.valueOf("2023-03-05"), rows.get(1).get("BorrowDate"));
                    assertNull(rows.get(1).get("ReturnDate"), "ReturnDate for second row should be NULL.");
                }
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("4) Stored procedures exist")
    void testStoredProceduresExist() throws Exception {
        List<String> expected = List.of(
                "sp_AggregateStats",
                "sp_StringOperations",
                "sp_JoinReports",
                "sp_OperatorExamples",
                "sp_FormattedDates",
                "sp_WildcardExamples",
                "sp_ClauseExamples"
                // "sp_RunAllReports"  // optional
        );

        try (Connection conn = newDbConnection()) {
            for (String sp : expected) {
                assertTrue(procedureExists(conn, DB_NAME, sp), "Stored procedure missing: " + sp);
            }
        }
    }

    @Test
    @Order(5)
    @DisplayName("5) Stored procedures execute without errors (smoke test)")
    void testStoredProceduresExecute() throws Exception {
        List<String> toCall = List.of(
                "{CALL sp_AggregateStats()}",
                "{CALL sp_StringOperations()}",
                "{CALL sp_JoinReports()}",
                "{CALL sp_OperatorExamples()}",
                "{CALL sp_FormattedDates()}",
                "{CALL sp_WildcardExamples()}",
                "{CALL sp_ClauseExamples()}"
                // "{CALL sp_RunAllReports()}" // optional
        );

        try (Connection conn = newDbConnection()) {
            for (String call : toCall) {
                try (CallableStatement cs = conn.prepareCall(call)) {
                    boolean hasResults = cs.execute();
                    // At least one result set OR update count should be present
                    assertTrue(hasResults || cs.getUpdateCount() != -1,
                            "Procedure ran but produced neither result sets nor updates: " + call);
                    // Consume any additional result sets safely
                    while (true) {
                        if (hasResults) {
                            try (ResultSet rs = cs.getResultSet()) {
                                // We don't assume column names here—just ensure the result can be read.
                                while (rs.next()) {
                                    // no-op
                                }
                            }
                        } else {
                            int updateCount = cs.getUpdateCount();
                            if (updateCount == -1) break; // no more results
                        }
                        hasResults = cs.getMoreResults();
                    }
                }
            }
        }
    }

    // -------- OPTIONAL: deeper, value-based assertions (enable if your SPs SELECT with these aliases) --------

    @Test
    @Order(6)
    @DisplayName("sp_AggregateStats returns expected counts/dates (optional deep check)")
    void testAggregateStatsValues() throws Exception {
        try (Connection conn = newDbConnection();
             CallableStatement cs = conn.prepareCall("{CALL sp_AggregateStats()}")) {

            assertTrue(cs.execute(), "sp_AggregateStats should return a result set.");
            try (ResultSet rs = cs.getResultSet()) {
                assertTrue(rs.next(), "sp_AggregateStats returned no rows.");
                // Expect aliases: TotalBooks, MostRecentJoinDate, OldestPublishedYear
                // If students used different aliases, rename here or skip this test.
                int totalBooks = rs.getInt("TotalBooks");
                Date mostRecentJoin = rs.getDate("MostRecentJoinDate");
                int oldestYear = rs.getInt("OldestPublishedYear");

                assertEquals(2, totalBooks);
                assertEquals(Date.valueOf("2023-02-15"), mostRecentJoin);
                assertEquals(1925, oldestYear);
            }
        } catch (SQLException e) {
            Assumptions.abort("Skipping deep check because sp_AggregateStats column aliases differ or SP not implemented as expected: " + e.getMessage());
        }
    }

    @Test
    @Order(7)
    @DisplayName("sp_OperatorExamples filters as expected (optional deep check)")
    void testOperatorExamplesValues() throws Exception {
        try (Connection conn = newDbConnection();
             CallableStatement cs = conn.prepareCall("{CALL sp_OperatorExamples()}")) {

            boolean hasResults = cs.execute();
            assertTrue(hasResults, "sp_OperatorExamples should return result sets.");

            // Result set 1: members with email ending 'example.com' (expect 2)
            try (ResultSet rs1 = cs.getResultSet()) {
                int c = 0; while (rs1.next()) c++;
                assertEquals(2, c, "Expected 2 members with email ending 'example.com'.");
            }

            // Move to next result set: books in Fiction/Programming after 2000 (expect 1 -> 'Clean Code')
            assertTrue(cs.getMoreResults(), "sp_OperatorExamples should return a second result set.");
            try (ResultSet rs2 = cs.getResultSet()) {
                assertTrue(rs2.next(), "Expected at least one book after 2000 in chosen genres.");
                assertEquals("Clean Code", rs2.getString("Title"));
                assertFalse(rs2.next(), "Expected exactly one row in this result set.");
            }
        } catch (SQLException e) {
            Assumptions.abort("Skipping deep check because sp_OperatorExamples shape differs: " + e.getMessage());
        }
    }

    @Test
    @Order(8)
    @DisplayName("sp_WildcardExamples behaves as expected (optional deep check)")
    void testWildcardExamplesValues() throws Exception {
        try (Connection conn = newDbConnection();
             CallableStatement cs = conn.prepareCall("{CALL sp_WildcardExamples()}")) {

            boolean hasResults = cs.execute();
            assertTrue(hasResults, "sp_WildcardExamples should return result sets.");

            // RS1: titles starting with 'The' -> 'The Great Gatsby' only
            try (ResultSet rs1 = cs.getResultSet()) {
                assertTrue(rs1.next());
                assertEquals("The Great Gatsby", rs1.getString("Title"));
                assertFalse(rs1.next());
            }

            // RS2: titles where second character is 'l' -> 'Clean Code' only
            assertTrue(cs.getMoreResults(), "Expected a second result set.");
            try (ResultSet rs2 = cs.getResultSet()) {
                assertTrue(rs2.next());
                assertEquals("Clean Code", rs2.getString("Title"));
                assertFalse(rs2.next());
            }
        } catch (SQLException e) {
            Assumptions.abort("Skipping deep check because sp_WildcardExamples shape differs: " + e.getMessage());
        }
    }
}

