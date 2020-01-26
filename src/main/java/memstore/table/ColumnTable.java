package memstore.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;


/**
 * ColumnTable, which stores data in column-major format.
 * That is, data is laid out like
 *   col 1 | col 2 | ... | col m.
 */
public class ColumnTable implements Table {
  int numCols;
  int numRows;
  ByteBuffer columns;

  public ColumnTable() {
  }

  /**
   * Loads data into the table through passed-in data loader. Is not timed.
   *
   * @param loader Loader to load data from.
   * @throws IOException
   */
  public void load(DataLoader loader) throws IOException {
    this.numCols = loader.getNumCols();
    List<ByteBuffer> rows = loader.getRows();
    numRows = rows.size();
    this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

    for (int rowId = 0; rowId < numRows; rowId++) {
      ByteBuffer curRow = rows.get(rowId);
      for (int colId = 0; colId < numCols; colId++) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        this.columns.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
      }
    }
  }

  /**
   * Returns the int field at row `rowId` and column `colId`.
   */
  @Override
  public int getIntField(int rowId, int colId) {
    int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
    return columns.getInt(offset);
  }

  /**
   * Inserts the passed-in int field at row `rowId` and column `colId`.
   */
  @Override
  public void putIntField(int rowId, int colId, int field) {
    int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
    columns.putInt(offset, field);
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
      sum += this.columns.getInt(ByteFormat.FIELD_LEN * rowId);
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
    int sum = 0;
    for (int rowId = 0; rowId < numRows; rowId++) {
      int col1Value = this.columns.getInt(ByteFormat.FIELD_LEN * ((1 * numRows) + rowId));
      int col2Value = this.columns.getInt(ByteFormat.FIELD_LEN * ((2 * numRows) + rowId));
      if (col1Value > threshold1 && col2Value < threshold2) {
        sum += getIntField(rowId, 0);
      }
    }
    return sum;
  }

  /**
   * Implements the query
   *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
   *
   *  Returns the sum of all elements in the rows which pass the predicate.
   */
  @Override
  public long predicatedAllColumnsSum(int threshold) {
    int sum = 0;
//    List<Integer> listOfRows = new ArrayList<>();
//    for (int rowId = 0; rowId < numRows; rowId++) {
//      int col0Value = this.columns.getInt(ByteFormat.FIELD_LEN * rowId);
//      if (col0Value > threshold) {
//        listOfRows.add(rowId);
//      }
//    }
//    for (int colId = 0; colId < numCols; colId++) {
//      for (Integer rowId : listOfRows) {
//        sum += this.columns.getInt(ByteFormat.FIELD_LEN * ((colId * numRows) + rowId));
//      }
//    }

    for (int colId = 0; colId < numCols; colId++) {
      for (int rowId = 0; rowId < numRows; rowId++) {
        int col0Value = this.columns.getInt(ByteFormat.FIELD_LEN * rowId);
        if (col0Value > threshold) {
          sum += this.columns.getInt(ByteFormat.FIELD_LEN * ((colId * numRows) + rowId));
        }
      }
    }
    return sum;
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
      int col0Value = this.columns.getInt(ByteFormat.FIELD_LEN * rowId);
      if (col0Value < threshold) {
        int col2Value = this.columns.getInt(ByteFormat.FIELD_LEN * ((2 * numRows) + rowId));
        int col3Value = this.columns.getInt(ByteFormat.FIELD_LEN * ((3 * numRows) + rowId));
        putIntField(rowId, 3, col2Value + col3Value);
        count++;
      }
    }
    return count;
  }
}
