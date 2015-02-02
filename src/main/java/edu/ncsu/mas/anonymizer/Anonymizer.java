package edu.ncsu.mas.anonymizer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Ordering;

import edu.ncsu.mas.anonymizer.util.ValueComparableMap;

public class Anonymizer {
  private static final String mysqlDriver = "com.mysql.jdbc.Driver";

  public static void main(String[] args) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, SQLException {
    String dbUrl = args[0];
    String selectQuery = args[1];
    String sensitiveColLabels = args[2];

    // String inOutDir = args[5];

    Map<String, TreeMap<String, Long>> sensitiveColMap = new HashMap<String, TreeMap<String, Long>>();
    for (String sensitiveCol : sensitiveColLabels.split(";")) {
      sensitiveColMap.put(sensitiveCol, new ValueComparableMap<String, Long>(Ordering.natural()));
    }

    Class.forName(mysqlDriver).newInstance();

    try (Connection conn = DriverManager.getConnection(dbUrl)) {
      while (true) {
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
          stmt.setFetchSize(1000);
          try (ResultSet rs = stmt.executeQuery(selectQuery)) {
            ResultSetMetaData metadata = rs.getMetaData();
            int colCount = metadata.getColumnCount();
            String[] colLabels = new String[colCount];
            for (int i = 1; i <= colCount; i++) {
              colLabels[i] = metadata.getColumnLabel(i);
            }

            while (rs.next()) {
              for (int i = 1; i <= colCount; i++) {
                String actualVal = rs.getString(i);
                if (sensitiveColMap.containsKey(colLabels[i])) {
                  TreeMap<String, Long> actualToPseudoValMap = sensitiveColMap.get(colLabels[i]);
                  if (!actualToPseudoValMap.containsKey(actualVal)) {
                    Long lastVal = actualToPseudoValMap.get(actualToPseudoValMap.lastKey());
                    actualToPseudoValMap.put(actualVal, ++lastVal);
                  }
                  Long pseudoVal = actualToPseudoValMap.get(actualVal);
                  System.out.print(pseudoVal);
                } else {
                  System.out.print(actualVal);
                }
              }
              System.out.println();
            }
          }
        }
      }
    }
  }
}
