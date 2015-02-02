package edu.ncsu.mas.anonymizer.util;

import static org.junit.Assert.*;

import java.util.TreeMap;

import org.junit.Test;

import com.google.common.collect.Ordering;

public class ValueComparableMapTest {

  @Test
  public void test() {
    TreeMap<String, Integer> map = new ValueComparableMap<String, Integer>(Ordering.natural());
    map.put("a", 5);
    map.put("b", 1);
    map.put("c", 3);
    assertEquals("b", map.firstKey());
    assertEquals("a", map.lastKey());
    map.put("d", 0);
    assertEquals("d", map.firstKey());
    // ensure it's still a map (by overwriting a key, but with a new value)
    map.put("d", 2);
    assertEquals("b", map.firstKey());
    // Ensure multiple values do not clobber keys
    map.put("e", 2);
    assertEquals(5, map.size());
    assertEquals(2, (int) map.get("e"));
    assertEquals(2, (int) map.get("d"));

  }

}
