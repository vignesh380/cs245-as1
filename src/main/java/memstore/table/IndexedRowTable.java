package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  private ByteBuffer rows;
  private int indexColumn;

  public IndexedRowTable(int indexColumn) {
    this.indexColumn = indexColumn;
    index = new TreeMap<>();
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
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        int cellValue = curRow.getInt(ByteFormat.FIELD_LEN * colId);
        this.rows.putInt(offset, cellValue);
        if (colId == indexColumn) {
          insertIntoIndex(cellValue,rowId);
        }
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
    this.rows.putInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + colId), field);
  }

  /**
   * Implements the query
   *  SELECT SUM(col0) FROM table;
   *
   *  Returns the sum of all elements in the first column of the table.
   */
  @Override
  public long columnSum() {
    int sum = 0;
    for (int rowId = 0; rowId < numRows; rowId++) {
      sum += this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 0));
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
    int[] sum = {0};
    SortedMap<Integer, IntArrayList> list = null;
    if (indexColumn == 1) {
      list = index.tailMap(threshold1, false);
      Iterator<Map.Entry<Integer, IntArrayList>> iterator = list.entrySet().iterator();
      iterator.forEachRemaining(entry -> {
        entry.getValue().stream().forEach(rowId -> {
          //Fetch the value of col 2 to see if threshold is met
          int col2Value = this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 2));
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
          int col1Value = this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 1));
          if (col1Value > threshold1) {
            sum[0] += getIntField(rowId, 0);
          }
        });
      });
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        int col1Value = this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 1));
        int col2Value = this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 2));
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
    int[] sum = {0};
    if (indexColumn == 0) {
      SortedMap<Integer, IntArrayList> list = index.tailMap(threshold, false);
      Iterator<Map.Entry<Integer, IntArrayList>> iterator = list.entrySet().iterator();
      iterator.forEachRemaining(entry -> {
        entry.getValue().stream().forEach(rowId -> {
          for (int colId = 0; colId < numCols; colId++) {
            sum[0] += getIntField(rowId, colId);
          }
        });
      });
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        int col0Value = this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 0));
        if (col0Value > threshold) {
          for (int colId = 0; colId < numCols; colId++) {
            sum[0] += getIntField(rowId, colId);
          }
        }
      }
    }
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
    int count = 0;
    for (int rowId = 0; rowId < numRows; rowId++) {
      int col0Value = this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 0));
      if (col0Value < threshold) {
        int col2Value = this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 2));
        int col3Value = this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 3));
        putIntField(rowId, 3, col2Value + col3Value);
        if (indexColumn == 3) {
          index.get(col3Value).rem(rowId);
          insertIntoIndex(col3Value+col2Value,rowId);
        }
        count++;
      }
    }
    return count;
  }

  private void insertIntoIndex(int cellValue, int rowId) {
    if (index.containsKey(cellValue)) {
      index.get(cellValue).add(rowId);
    } else {
      IntArrayList rowIndex = new IntArrayList();
      rowIndex.add(rowId);
      index.put(cellValue, rowIndex);
    }
  }
}
