/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.clickbench;

import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.sql.opensearch.monitor.GCedMemoryUsage;
import org.opensearch.sql.ppl.PPLIntegTestCase;

@FixMethodOrder(MethodSorters.JVM)
public class PPLClickBenchIT extends PPLIntegTestCase {
  private static final MapBuilder<String, Long> summary = MapBuilder.newMapBuilder();
  private static final MapBuilder<Integer, Boolean> results = MapBuilder.newMapBuilder();

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.CLICK_BENCH);
    disableCalcite();
  }

  @AfterClass
  public static void reset() throws IOException {
    long total = 0;
    Map<String, Long> map = summary.immutableMap();
    for (long duration : map.values()) {
      total += duration;
    }
    System.out.println("Summary:");
    map.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(
                    entry ->
                            System.out.printf(Locale.ENGLISH, "%s: %d ms%n", entry.getKey(), entry.getValue()));
    System.out.printf(
            Locale.ENGLISH,
            "Total %d queries succeed. Average duration: %d ms%n",
            map.size(),
            total / map.size());
    System.out.println();

    Map<Integer, Boolean> resultsMap = results.immutableMap();
    if (!resultsMap.isEmpty()) {
      System.out.println("Query Results:");
      System.out.println("+---------+--------+");
      System.out.println("| Query   | Result |");
      System.out.println("+---------+--------+");
      resultsMap.entrySet().stream()
              .sorted(Map.Entry.comparingByKey())
              .forEach(
                      entry ->
                              System.out.printf(
                                      Locale.ENGLISH,
                                      "| %-7s | %-6s |%n",
                                      entry.getKey(),
                                      entry.getValue() ? "PASS" : "FAIL"));
      System.out.println("+---------+--------+");

      long passCount = resultsMap.values().stream().filter(result -> result).count();
      long failCount = resultsMap.size() - passCount;
      System.out.printf(
              Locale.ENGLISH,
              "Total: %d queries | Passed: %d | Failed: %d%n",
              resultsMap.size(),
              passCount,
              failCount);
      System.out.println();
    }
  }

  /** Ignore queries that are not supported. */
  protected Set<Integer> ignored() throws IOException {
    Set ignored = new HashSet();
    if (!isCalciteEnabled()) {
      // regexp_replace() is not supported in v2
      ignored.add(29);
    }
    if (!GCedMemoryUsage.initialized()) {
      // Ignore q30 when use RuntimeMemoryUsage,
      // because of too much script push down, which will cause ResourceMonitor restriction.
      ignored.add(30);
    }
    return ignored;
  }

  @Test
  public void test() throws IOException {
    for (int i = 1; i <= 43; i++) {
      if (ignored().contains(i)) {
        continue;
      }
      String ppl = sanitize(loadFromFile("clickbench/queries/q" + i + ".ppl"));
      System.out.println("RUNNING QUERY NUMBER: " + i + " Query: " + ppl);
      runQuery(summary, results, i, ppl);
      System.out.println("QUERY NUMBER: " + i + " Result: " + results.get(i));
      // V2 gets unstable scripts, ignore them when comparing plan
//      if (isCalciteEnabled()) {
//        String expected = loadExpectedPlan("clickbench/q" + i + ".yaml");
//        assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
//      }
    }
  }
}
