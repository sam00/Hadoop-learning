package SecondarySortTest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ProjectCounter.Compositekeywrite;
import ProjectCounter.CountryIncomeReducer;
import ProjectCounter.ImpCounterMap;

public class BasicCompKeySortTest {

  @Test
  public void Comapre_SmallToBig_MinusOne() throws IOException {

    // Arrange
    Compositekeywrite inp0 =
        new Compositekeywrite("Switzerland", 63318.3487591229);
    Compositekeywrite inp1 = new Compositekeywrite("Norway", 65636.7724056934);

    // Act
    int actual = inp0.compareTo(inp1);

    // Assert
    assertEquals(-1, actual);
  }

  @Test
  public void Comapre_BigToSmall_One() throws IOException {

    // Arrange
    Compositekeywrite inp0 =
        new Compositekeywrite("Switzerland", 63318.3487591229);
    Compositekeywrite inp1 = new Compositekeywrite("Norway", 65636.7724056934);

    // Act
    int actual = inp1.compareTo(inp0);

    // Assert
    assertEquals(1, actual);
  }

  @Test
  public void Comapre_Same_Zero() throws IOException {

    // Arrange
    Compositekeywrite inp0 = new Compositekeywrite("Switzerland", 123.123);
    Compositekeywrite inp1 = new Compositekeywrite("Norway", 123.123);

    // Act
    int actual = inp1.compareTo(inp0);

    // Assert
    assertEquals(0, actual);
  }

}
