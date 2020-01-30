package memstore.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;


/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {

  public CustomTable() {
  }

  protected int numCols;
  protected int numRows;
  protected ByteBuffer rows;
  //protected ByteBuffer columns;
  protected long[] columnSum;
  protected ByteBuffer columns_1;
  protected ByteBuffer columns_2;

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
    this.numRows = rows.size();
    this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
    //this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
    this.columns_1 = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * 3);
    this.columns_2 = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * (numCols - 3));
    this.columnSum = new long[numRows];
    //this.columnSum = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);

    for (int rowId = 0; rowId < numRows; rowId++) {
      ByteBuffer curRow = rows.get(rowId);
      for (int colId = 0; colId < numCols; colId++) {
        putIntField(rowId, colId, curRow.getInt(ByteFormat.FIELD_LEN * colId));
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
   * Returns the int field at row `rowId` and column `colId`.
   */
  public int getIntFieldFromRow(int rowId, int colId) {
    return this.rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + colId));
  }

  /**
   * Returns the int field at row `rowId` and column `colId`.
   */
  public int getIntFieldFromColumn(int rowId, int colId) {
    if (colId < 4) {
      int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
      return columns_1.getInt(offset);
    } else {
      int offset = ByteFormat.FIELD_LEN * (((colId - 3) * numRows) + rowId);
      return columns_2.getInt(offset);
    }
    //int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
    //return columns.getInt(offset);
  }

  /**
   * Returns the int field at row `rowId` and column `colId`.
   */
  public long getIntFieldFromColumnSum(int rowId) {
    return columnSum[rowId];
    //return columnSum.getInt(ByteFormat.FIELD_LEN * rowId);
  }

  /**
   * Inserts the passed-in int field at row `rowId` and column `colId`.
   */
  @Override
  public void putIntField(int rowId, int colId, int field) {
    //Update the row sum
    putIntFieldSum(rowId, field, getIntField(rowId, colId));

    int rowOffset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
    this.rows.putInt(rowOffset, field);

    if (colId < 3) {
      this.columns_1.putInt(ByteFormat.FIELD_LEN * ((colId * numRows) + rowId), field);
    } else {
      this.columns_2.putInt(ByteFormat.FIELD_LEN * (((colId - 3) * numRows) + rowId), field);
    }
  }

  private void putIntFieldSum(int rowId, int field, int oldValue) {
    //this.columnSum.putInt(ByteFormat.FIELD_LEN * rowId, getIntFieldFromColumnSum(rowId) + field);
    if (oldValue != 0) {
      columnSum[rowId] -= oldValue;
    }
    columnSum[rowId] += field;
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
      sum += getIntFieldFromColumn(rowId, 0);
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
    long sum = 0;
    for (int rowId = 0; rowId < numRows; rowId++) {
      int col1Value = getIntFieldFromRow(rowId, 1);
      int col2Value = getIntFieldFromRow(rowId, 2);
      if (col1Value > threshold1 && col2Value < threshold2) {
        sum += getIntFieldFromColumn(rowId, 0);
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
    long sum = 0;
    for (int rowId = 0; rowId < numRows; rowId++) {
      int col0Value = getIntFieldFromColumn(rowId, 0);
      if (col0Value > threshold) {
        sum += getIntFieldFromColumnSum(rowId);
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
      int col0Value = getIntFieldFromRow(rowId, 0);
      if (col0Value < threshold) {
        int col2Value = getIntFieldFromRow(rowId, 2);
        int col3Value = getIntFieldFromRow(rowId, 3);
        //putIntFieldSum(rowId, -1 * col3Value, 0);
        putIntField(rowId, 3, col2Value + col3Value);
        count++;
      }
    }
    return count;
  }
}
