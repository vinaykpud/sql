/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.clickbench;

import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;
import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.sql.opensearch.monitor.GCedMemoryUsage;
import org.opensearch.sql.ppl.PPLIntegTestCase;

@FixMethodOrder(MethodSorters.JVM)
public class PPLClickBenchIT extends PPLIntegTestCase {
  private static final MapBuilder<String, Long> summary = MapBuilder.newMapBuilder();
  private static final MapBuilder<Integer, Boolean> response_200 = MapBuilder.newMapBuilder();
  private static final List<Integer> response_200_failing = new java.util.ArrayList<Integer>();
  private static final List<Integer> non_200 = new java.util.ArrayList<Integer>();
  private static final List<Integer> passing = new java.util.ArrayList<Integer>();

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

    // Display detailed query results
    if (!passing.isEmpty() || !response_200_failing.isEmpty() || !non_200.isEmpty()) {
      System.out.println("Query Results:");
      System.out.println("+---------+------------------+");
      System.out.println("| Query   | Status           |");
      System.out.println("+---------+------------------+");

      // Show passing queries
      passing.stream()
              .sorted()
              .forEach(q -> System.out.printf(Locale.ENGLISH, "| %-7d | %-16s |%n", q, "PASS"));

      // Show queries that returned 200 but failed assertion
      response_200_failing.stream()
              .sorted()
              .forEach(q -> System.out.printf(Locale.ENGLISH, "| %-7d | %-16s |%n", q, "FAIL (200)"));

      // Show queries that didn't return 200
      non_200.stream()
              .sorted()
              .forEach(q -> System.out.printf(Locale.ENGLISH, "| %-7d | %-16s |%n", q, "FAIL (non-200)"));

      System.out.println("+---------+------------------+");
      System.out.printf(
              Locale.ENGLISH,
              "Total: %d | Passed: %d | Failed (200): %d | Failed (non-200): %d%n",
              passing.size() + response_200_failing.size() + non_200.size(),
              passing.size(),
              response_200_failing.size(),
              non_200.size());
      System.out.println();
    }
  }

  /** Ignore queries that are not supported by Calcite. */
  protected Set<Integer> ignored() {
    return Set.of(41); // query currently fails on main
  }

  @Test
  public void test() throws IOException {
    for (int i = 1; i <= 43; i++) {
      if (ignored().contains(i)) {
        continue;
      }
      logger.info("Running Query{}", i);
      String ppl = sanitize(loadFromFile("clickbench/queries/q" + i + ".ppl"));
      // V2 gets unstable scripts, ignore them when comparing plan
      if (isCalciteEnabled()) {
        String expected = loadExpectedPlan("clickbench/q" + i + ".yaml");
        assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
      }
      timing(summary, "q" + i, ppl);
    }
  }

  /** Queries that are returning 200s and response is correct and not empty */
  protected Set<Integer> supported() {
    return Set.of(1, 2, 3, 8, 13, 15);
  }

  @Test
  public void testDataFusion() throws IOException {
    // flip this to run everything and get the full current list of p/f/f200.
    // when false will fail on first f200 occurence and show assert diff.
    boolean runAllQueries = true;
    for (int i = 1; i <= 43; i++) {
      if (ignored().contains(i)) {
        continue;
      }
      String ppl = sanitize(loadFromFile("clickbench/queries/q" + i + ".ppl"));
      System.out.println("RUNNING QUERY NUMBER: " + i + " Query: " + ppl);

      // TODO: Add plan comparisons
      // V2 gets unstable scripts, ignore them when comparing plan
//      if (isCalciteEnabled()) {
//        String expected = loadExpectedPlan("clickbench/q" + i + ".yaml");
//        assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
//      }
      // runs the query and buckets into failing (non200), passing (200 and response matches), failing_200
      String actual = runQuery(summary, response_200, i, ppl);
//      System.out.println("QUERY NUMBER: " + i + " Result: " + response_200.get(i));
      String expected = sanitize(loadFromFile("clickbench/queries/expected/expected-q" + i + ".json"));

      if (response_200.get(i)) {
        // 200 returned and we expect it to pass
        try {
          assertJsonEquals(String.format("query number %d", i), expected, actual);
          passing.add(i);
        } catch (AssertionError e) {
          // comment this out to get a full list of current pass/failed
          if (supported().contains(i) && runAllQueries == false) {
            throw e;
          }
          response_200_failing.add(i);
          // 200 but we haven't marked supported yet, mark it in a separate list
        }
      } else {
        non_200.add(i);
      }
    }
    // display results
    System.out.println("PASSING: " + passing);
    System.out.println("FAILING WITH 200: " + response_200_failing);
    System.out.println("FAILING: " + non_200);

    List<Integer> supportedButNotPassing = supported().stream()
            .filter(q -> !passing.contains(q))
            .sorted()
            .toList();
    assertEquals("Expected all supported queries to be marked passing", Collections.emptyList(), supportedButNotPassing);
  }
}
