package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;


/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

  int numCols;
  int numRows;
  private TreeMap<Integer, IntArrayList> index;
  private HashMap<Integer, Integer> forwardIndex;
  private ByteBuffer rows;
  private int indexColumn;

  public IndexedRowTable(int indexColumn) {
    this.indexColumn = indexColumn;
    index = new TreeMap<>();
    forwardIndex = new HashMap<>();
  }

  /**
   * Loads data into the table through passed-in data loader. Is not timed.
   *
   * @param loader Loader to load data from.
   * @throws IOException
   */
  @Override
  public void load(DataLoader loader) throws IOException {
    this.numCols = loader.getNumCols();
    List<ByteBuffer> rows = loader.getRows();
    numRows = rows.size();
    this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
    for (int rowId = 0; rowId < numRows; rowId++) {
      ByteBuffer curRow = rows.get(rowId);
      for (int colId = 0; colId < numCols; colId++) {
        //int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        int cellValue = curRow.getInt(ByteFormat.FIELD_LEN * colId);
        HashMap<Integer, Integer> invalidReference = new HashMap<>();
//        this.rows.putInt(offset, cellValue);
//        if (colId == indexColumn) {
//          addToIndex(rowId, cellValue);
//        }
        putIntField(rowId, colId, cellValue, invalidReference);
        cleanUpIndex(invalidReference);
      }
    }
  }

  /**
   * Returns the int field at row `rowId` and column `colId`.
   */
  @Override
  public int getIntField(int rowId, int colId) {
    return this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + colId));
  }

  /**
   * Inserts the passed-in int field at row `rowId` and column `colId`.
   */
  @Override
  public void putIntField(int rowId, int colId, int field) {
    HashMap<Integer, Integer> invalidReference = new HashMap<>();
    putIntField(rowId, colId, field, invalidReference);
    cleanUpIndex(invalidReference);
  }

  private void cleanUpIndex(HashMap<Integer, Integer> invalidReference) {
    invalidReference.forEach((value, row) -> {
      index.get(value).rem(row);
    });
  }

  /**
   * Inserts the passed-in int field at row `rowId` and column `colId`.
   */
  private void putIntField(int rowId, int colId, int field, HashMap<Integer, Integer> invalidReferences) {
    this.rows.putInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + colId), field);
    if (colId == indexColumn) {
      addToIndex(rowId, field, invalidReferences);
    }
  }

  private void addToIndex(int rowId, int field, HashMap<Integer, Integer> invalidReferences) {
    //update the forward index first
    if (forwardIndex.containsKey(rowId)) {
      //Collect the old reference
      invalidReferences.put(forwardIndex.get(rowId), rowId);
    }
    forwardIndex.put(rowId, field);
    if (index.containsKey(field)) {
      index.get(field).add(rowId);
    } else {
      IntArrayList list = new IntArrayList();
      list.add(rowId);
      index.put(field, list);
    }
  }

  /**
   * Implements the query
   *  SELECT SUM(col0) FROM table;
   *
   *  Returns the sum of all elements in the first column of the table.
   */
  @Override
  public long columnSum() {
    long sum = 0;
    for (int rowId = 0; rowId < numRows; rowId++) {
      sum += getIntField(rowId, 0);
    }
    return sum;
  }

  /**
   * Implements the query
   *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
   *
   *  Returns the sum of all elements in the first column of the table,
   *  subject to the passed-in predicates.
   */
  @Override
  public long predicatedColumnSum(int threshold1, int threshold2) {
    long[] sum = {0};
    SortedMap<Integer, IntArrayList> list = null;
    if (indexColumn == 1) {
      list = index.tailMap(threshold1, false);
      Iterator<Map.Entry<Integer, IntArrayList>> iterator = list.entrySet().iterator();
      iterator.forEachRemaining(entry -> {
        entry.getValue().stream().forEach(rowId -> {
          //Fetch the value of col 2 to see if threshold is met
          int col2Value = getIntField(rowId, 2);
          if (col2Value < threshold2) {
            sum[0] += getIntField(rowId, 0);
          }
        });
      });
    } else if (indexColumn == 2) {
      list = index.headMap(threshold2, false);
      Iterator<Map.Entry<Integer, IntArrayList>> iterator = list.entrySet().iterator();
      iterator.forEachRemaining(entry -> {
        entry.getValue().stream().forEach(rowId -> {
          //Fetch the value of col 1 to see if threshold is met
          int col1Value = getIntField(rowId, 1);
          if (col1Value > threshold1) {
            sum[0] += getIntField(rowId, 0);
          }
        });
      });
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        int col1Value = getIntField(rowId, 1);
        int col2Value = getIntField(rowId, 2);
        if (col1Value > threshold1 && col2Value < threshold2) {
          sum[0] += getIntField(rowId, 0);
        }
      }
    }
    return sum[0];
  }

  /**
   * Implements the query
   *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
   *
   *  Returns the sum of all elements in the rows which pass the predicate.
   */
  @Override
  public long predicatedAllColumnsSum(int threshold) {
    long[] sum = {0};
//    if (indexColumn == 0) {
//      SortedMap<Integer, IntArrayList> list = index.tailMap(threshold, false);
//      list.entrySet().forEach(entry -> {
//        entry.getValue().stream().forEach(rowId -> {
//          for (int colId = 0; colId < numCols; colId++) {
//            sum[0] += getIntField(rowId, colId);
//          }
//        });
//      });
////      Iterator<Map.Entry<Integer, IntArrayList>> iterator = list.entrySet().iterator();
////      iterator.forEachRemaining(entry -> {
////        entry.getValue().stream().forEach(rowId -> {
////          for (int colId = 0; colId < numCols; colId++) {
////            sum[0] += getIntField(rowId, colId);
////          }
////        });
////      });
//    } else {
    for (int rowId = 0; rowId < numRows; rowId++) {
      int col0Value = getIntField(rowId, 0);
      if (col0Value > threshold) {
        for (int colId = 0; colId < numCols; colId++) {
          sum[0] += getIntField(rowId, colId);
        }
      }
    }
    //}
    return sum[0];
  }

  /**
   * Implements the query
   *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
   *
   *   Returns the number of rows updated.
   */
  @Override
  public int predicatedUpdate(int threshold) {
    int[] count = {0};
    if (indexColumn == 0) {
      //use the index as we have the values
      NavigableMap<Integer, IntArrayList> view = index.headMap(threshold, false);
      Set<Integer> toUpdateRowId = new HashSet<>();
      view.entrySet().forEach((entry) -> {
        toUpdateRowId.addAll(entry.getValue());
      });
      toUpdateRowId.forEach(rowId -> {
        int col2Value = getIntField(rowId, 2);
        int col3Value = getIntField(rowId, 3);
        putIntField(rowId, 3, col2Value + col3Value);
        count[0]++;
      });
    } else {
      //use the method from the table row
      for (int rowId = 0; rowId < numRows; rowId++) {
        int col0Value = getIntField(rowId, 0);
        if (col0Value < threshold) {
          int col2Value = getIntField(rowId, 2);
          int col3Value = getIntField(rowId, 3);
          putIntField(rowId, 3, col2Value + col3Value);
          count[0]++;
        }
      }
    }
    return count[0];
  }
}
