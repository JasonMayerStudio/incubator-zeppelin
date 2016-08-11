package org.apache.zeppelin.spark.utils;

import org.apache.zeppelin.spark.ZeppelinContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Stores local variables across all spark interpreters
 */
public class VariableStore {
  private Map<String, String> variableMap;
  private ZeppelinContext z;

  public String noteId() {
    return z.getInterpreterContext().getNoteId();
  }

  public VariableStore(ZeppelinContext z) {
    variableMap = new ConcurrentHashMap<>();
    this.z = z;
  }
  Pattern extractSetStatement =
    Pattern.compile(
      "^\\s*set\\s+([^\\s]+)\\s*=\\s*('.*')\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

  /**
   * Check to see if snippet is a variable setting statement
   */
  public Boolean isSetStatement(String snippet) {
    return extractSetStatement.matcher(snippet).matches();
  }

  /**
   * Set a local variable. Used in a query: SET some_local = 'value'
   */
  public void setLocalVariable(String setStatement) {
    Matcher m = extractSetStatement.matcher(setStatement);
    if (m.matches()) {
      String variableName = noteId() + "#" + m.group(1);
      String variableValue = m.group(2).substring(1, m.group(2).length() - 1);
      variableMap.put(variableName, variableValue);
    }
  }

  /**
   * Set a local variable by noteID
   */
  public void setLocalVariable(String variableName, String variableValue) {
    String localVariableName =
      z.getInterpreterContext().getNoteId() + "#" + variableName;
    variableMap.put(localVariableName, variableValue);
  }

  /**
   * Retrieve the local variable from the map
   */
  public String getLocalVariable(String variable) {
    String variableName = noteId() + "#" + variable;
    return variableMap.get(variableName);
  }

  Pattern extractLocalVariable =
    Pattern.compile("(@\\{[^\\s]+\\})", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /**
   * Interpolate your query with local variables: SELECT * FROM @{table} WHERE col='@{value}'
   */
  public String interpolateLocalVariable(String snippet) {
    String injectedString = snippet;
    Matcher m = extractLocalVariable.matcher(injectedString);
    while (m.find()) {
      String group = m.group(1);
      System.out.println(group);
      String variableName = group.substring(2, group.length() - 1);
      String variableValue = getLocalVariable(variableName);
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
}
