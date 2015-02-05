package edu.ncsu.mas.anonymizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Anonymizer {
  private static final String mysqlDriver = "com.mysql.jdbc.Driver";

  public static void main(String[] args) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, SQLException, FileNotFoundException, IOException {
    String dbUrl = args[0];
    String selectQuery = args[1];
    
    String[] sensitiveColLabels = args[2].split(";");
    String[] sensitiveColFilenames = args[3].split(";");
    if (sensitiveColLabels.length != sensitiveColFilenames.length) {
      throw new IllegalArgumentException("sensitiveColLabels and sensitiveColFilenames "
          + "arguments must of the same length.");
    }
    
    File inOutDir = new File(args[4]);
    File outFile = new File(inOutDir, args[5]);

    // Initialize
    Map<String, Map<String, Long>> sensitiveColMap = new HashMap<String, Map<String, Long>>();
    for (int i = 0; i < sensitiveColLabels.length; i++) {
      Map<String, Long> actualToPseudoVals = new HashMap<String, Long>();
      if (!sensitiveColFilenames[i].equals("null")) {
        try (BufferedReader br = new BufferedReader(new FileReader(new File(inOutDir,
            sensitiveColFilenames[i])))) {
          String line = null;
          while ((line = br.readLine()) != null) {
            String[] lineParts = line.split(",");
            actualToPseudoVals.put(lineParts[0], Long.parseLong(lineParts[1]));
          }
        }
      }
      sensitiveColMap.put(sensitiveColLabels[i], new HashMap<String, Long>());
    }
    
    // Read data, map actual IDs to pseudo IDs, write to outFile
    Class.forName(mysqlDriver).newInstance();
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PrintWriter writer = new PrintWriter(outFile)) {
      try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
          ResultSet.CONCUR_READ_ONLY)) {
        stmt.setFetchSize(1000);
        try (ResultSet rs = stmt.executeQuery(selectQuery)) {
          ResultSetMetaData metadata = rs.getMetaData();
          int colCount = metadata.getColumnCount();
          String[] colLabels = new String[colCount];
          for (int i = 1; i <= colCount; i++) {
            colLabels[i - 1] = metadata.getColumnLabel(i);
          }

          while (rs.next()) {
            StringBuilder outLine = new StringBuilder();
            for (int i = 1; i <= colCount; i++) {
              String actualVal = rs.getString(i);
              // System.out.print(actualVal + ": ");
              if (sensitiveColMap.containsKey(colLabels[i - 1])) {
                Map<String, Long> actualToPseudoValMap = sensitiveColMap.get(colLabels[i - 1]);
                if (actualToPseudoValMap.isEmpty()) {
                  actualToPseudoValMap.put(actualVal, 0L);
                }
                Long pseudoVal = actualToPseudoValMap.get(actualVal);
                if (pseudoVal == null) {
                  pseudoVal = Collections.max(actualToPseudoValMap.values()) + 1;
                  actualToPseudoValMap.put(actualVal, pseudoVal);
                }
                outLine.append(pseudoVal + ",");
              } else {
                outLine.append(actualVal + ",");
              }
            }
            outLine.deleteCharAt(outLine.length() - 1);
            writer.println(outLine);
          }
        }
      }
    }
    
    // Write mappings to outfile. If file exists overwrite it
    for (String sensitiveCol : sensitiveColMap.keySet()) {
      try (PrintWriter writer = new PrintWriter(new File(inOutDir, sensitiveCol + ".csv"))) {
        Map<String, Long> actualToPseudoValMap = sensitiveColMap.get(sensitiveCol);
        for (String actual : actualToPseudoValMap.keySet()) {
          writer.println(actual + "," + actualToPseudoValMap.get(actual));
        }
      }
    }
  }
}
