/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.spark;

import org.apache.spark.sql.cassandra.*;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.spark.utils.CsqlParserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spark Cassandra SQL interpreter for Zeppelin.
 */
public class SparkCassandraSqlInterpreter extends SparkSqlInterpreter {
  Logger logger = LoggerFactory.getLogger(SparkCassandraSqlInterpreter.class);
  Map<String, String> variableMap;

  static {
    Interpreter.register(
      "csql",
      "spark",
      SparkCassandraSqlInterpreter.class.getName(),
      new InterpreterPropertyBuilder()
        .add("zeppelin.spark.maxResult",
          SparkInterpreter.getSystemDefault("ZEPPELIN_SPARK_MAXRESULT",
            "zeppelin.spark.maxResult", "1000"),
          "Max number of SparkSQL result to display.")
        .add("zeppelin.spark.concurrentSQL",
          SparkInterpreter.getSystemDefault("ZEPPELIN_SPARK_CONCURRENTSQL",
            "zeppelin.spark.concurrentSQL", "false"),
          "Execute multiple SQL concurrently if set true.")
        .build());
  }

  public SparkCassandraSqlInterpreter(Properties property) {
    super(property);
    variableMap = new ConcurrentHashMap<>();
  }

  Pattern extractIntoTableNamePattern =
    Pattern.compile(
      "(.*)(into)([ ]+)([a-zA-Z_]+).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

  /**
   * Given a query like:
   * SELECT * from mytable INTO myothertable
   * <p/>
   * Return "myothertable" or null
   *
   * @param query
   * @return
   */
  private String extractSqlInto(String query) {
    Matcher m = extractIntoTableNamePattern.matcher(query);
    if (m.matches()) {
      return m.group(4);
    } else {
      return null;
    }
  }

  /**
   * Given a query like:
   * SELECT * from mytable INTO myothertable
   * <p/>
   * Return "SELECT * from mytable"
   *
   * @param query
   * @return
   */
  private String removeSqlInto(String query) {
    Matcher m = extractIntoTableNamePattern.matcher(query);
    if (m.matches()) {
      return m.group(1);
    } else {
      return query;
    }
  }

  /**
   * Extract snippets of code that being used to do something
   * like register a UDF but looking for any use of a Cassandra SQL context.
   */
  private Boolean contextSparkSqlContext(String snippet) {
    return snippet.contains("sqlc");
  }

  Pattern extractSetStatement =
    Pattern.compile(
      "^\\s*set\\s+([^\\s]+)\\s*=\\s*('.*')\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

  /**
   * Check to see if snippet is a variable setting statement
   */
  private Boolean isSetStatement(String snippet) {
    return extractSetStatement.matcher(snippet).matches();
  }

  /**
   * Set a local variable. Used in a query: SET some_local = 'value'
   */
  private void setLocalVariable(String setStatement, InterpreterContext context) {
    String noteId = context.getNoteId();
    Matcher m = extractSetStatement.matcher(setStatement);
    if (m.matches()) {
      String variableName = noteId + "#" + m.group(1);
      String variableValue = m.group(2).substring(1, m.group(2).length() - 1);
      variableMap.put(variableName, variableValue);
    }
  }

  /**
   * Retrieve the local variable from the map
   */
  private String getLocalVariable(String variable, InterpreterContext context) {
    String noteId = context.getNoteId();
    String variableName = noteId + "#" + variable;
    return variableMap.get(variableName);
  }

  Pattern extractLocalVariable =
    Pattern.compile("(@\\{[A-z]+\\})", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /**
   * Interpolate your query with local variables: SELECT * FROM @{table} WHERE col='@{value}'
   */
  private String interpolateLocalVariable(String snippet, InterpreterContext context) {
    String injectedString = snippet;
    Matcher m = extractLocalVariable.matcher(injectedString);
    while (m.find()) {
      String group = m.group(1);
      System.out.println(group);
      String variableName = group.substring(2, group.length() - 1);
      String variableValue = getLocalVariable(variableName, context);
      System.out.println(variableName);

      int start = m.start(1);
      int end = m.end(1);
      String prefix = "";
      String postfix;
      if (start > 0) {
        prefix = injectedString.substring(0, start);
      }
      if (end <= injectedString.length() - 1) {
        postfix = injectedString.substring(end);
      } else {
        postfix = "";
      }

      injectedString = prefix + variableValue + postfix;
      m = extractLocalVariable.matcher(injectedString);
    }
    return injectedString;
  }
  /**
   * Initialize the interpreter. Load all necessary tables here
   */
  @Override
  public void open() {
    super.open();
    String source = "org.apache.spark.sql.cassandra";
    SQLContext sqlc = getSparkInterpreter().getSQLContext();

    // Register our Cassandra tables as external tables in the spark sql hive context
    Map<String, String> eventlogOpt = new HashMap<>();
    eventlogOpt.put("keyspace", "analytics");
    eventlogOpt.put("table", "eventlog");
    Map<String, String> experimentOpt = new HashMap<>();
    experimentOpt.put("keyspace", "analytics");
    experimentOpt.put("table", "experiment_assignments");
    sqlc.createExternalTable("eventlog", source, eventlogOpt);
    sqlc.createExternalTable("experiment_assignments", source, experimentOpt);
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    SQLContext sqlc = getSparkInterpreter().getSQLContext();
    SparkContext sc = sqlc.sparkContext();

    if (concurrentSQL()) {
      sc.setLocalProperty("spark.scheduler.pool", "fair");
    } else {
      sc.setLocalProperty("spark.scheduler.pool", null);
    }

    sc.setJobGroup(getJobGroup(context), "Zeppelin", false);

    DataFrame rddResult = null;

    for (String snippet : st.split(";")) {
      // Ignore trailing semicolons
      if (snippet.replaceAll("[\n\r ]", "").equals("")) {
        logger.info("Skipping empty bloc.");
        continue;
      }

      if (contextSparkSqlContext(snippet)) {
        InterpreterResult sparkResults = getSparkInterpreter().interpret(snippet, context);
        if (sparkResults.code() != Code.SUCCESS) return sparkResults;
      }
      else if (isSetStatement(snippet)) {
        setLocalVariable(snippet, context);
      }
      else { // Assume this is SQL
        String interpolated = interpolateLocalVariable(snippet, context);
        System.out.println(interpolated);

        String intervalExpanded = CsqlParserUtils.parseAndExpandInterval(interpolated);
        logger.info("Expanded sql: " + intervalExpanded);

        String cleanedSql = removeSqlInto(intervalExpanded);
        logger.info("Cleaned sql: " + cleanedSql);
        rddResult = sqlc.sql(cleanedSql);

        String intoTable = extractSqlInto(snippet);
        if (intoTable != null) {
          intoTable = intoTable.trim();
          logger.info("Registering results to tempTable: " + intoTable);
          rddResult.registerTempTable(intoTable);
        }
      }
    }

    int maxQueryResults = super.maxResult;
    if (context.getConfig().containsKey("OVERRIDE_MAX_RESULTS")) {
      maxQueryResults = Integer.parseInt((String) context.getConfig().get("OVERRIDE_MAX_RESULTS"));
      logger.info("Increasing max results returned to:" + maxQueryResults);
    }

    String msg = "";
    if (rddResult != null) {
      msg = ZeppelinContext.showDF(sc, context, rddResult, maxQueryResults);
      logger.info("Finished constructing result.");
    }

    sc.clearJobGroup();
    return new InterpreterResult(Code.SUCCESS, msg);
  }
}
