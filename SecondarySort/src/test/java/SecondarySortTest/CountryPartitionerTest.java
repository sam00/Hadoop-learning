package SecondarySortTest;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import ProjectCounter.Compositekeywrite;
import ProjectCounter.CountryPartitioner;

public class CountryPartitionerTest {

  @Test
  public void getPartition_SameCountries_SamePartition() throws IOException {

    // Arrange
    Compositekeywrite inp0 =
        new Compositekeywrite("Switzerland", 63318.3487591229);
    Compositekeywrite inp1 =
        new Compositekeywrite("Switzerland", 65636.7724056934);
    CountryPartitioner uut = new CountryPartitioner();

    // Act
    int actual0 = uut.getPartition(inp0, NullWritable.get(), 100);
    int actual1 = uut.getPartition(inp1, NullWritable.get(), 100);

    // Assert
    assertEquals(actual0, actual1);

  }

  @Test
  public void getPartition_DiffCountries_DiffPartition() throws IOException {

    // Arrange
    Compositekeywrite inp0 =
        new Compositekeywrite("Switzerland", 63318.3487591229);
    Compositekeywrite inp1 = new Compositekeywrite("Norway", 65636.7724056934);
    CountryPartitioner uut = new CountryPartitioner();

    // Act
    int actual0 = uut.getPartition(inp0, NullWritable.get(), 100);
    int actual1 = uut.getPartition(inp1, NullWritable.get(), 100);

    // Assert
    assertTrue(actual0 != actual1);

  }
}
