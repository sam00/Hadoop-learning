package ProjectCounter;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

@SuppressWarnings("rawtypes")
public class Compositekeywrite implements WritableComparable {

  private String countryName;
  private Double income;

  public Compositekeywrite() {
  }

  public Compositekeywrite(String countryName, Double income) {
    this.countryName = countryName;
    this.income = income;
  }

  @Override
  public String toString() {
    return (new StringBuilder().append(countryName).append("\t").append(income))
        .toString();
  }

  public void readFields(DataInput dataInput) throws IOException {
    countryName = WritableUtils.readString(dataInput);
    income = dataInput.readDouble();
  }

  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, countryName);
    dataOutput.writeDouble(income);
  }

  public int compareTo(Object objKeyPair) {

    int result = income.compareTo(((Compositekeywrite) objKeyPair).income);

    return result;
  }

  public String getCountryName() { // getter
    return countryName;
  }



  public Double getIncome() { // getter
    return income;

  }

  public void setIncome(double income) {
    this.income = income;

  }

  public void setCountryname(String countryName) {
    this.countryName = countryName;

  }

  }
