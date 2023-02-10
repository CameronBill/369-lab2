import java.util.Comparator.comparing;

public class YearMonthWritable implements WritableComparable<YearMonthWritable> {

    public IntWritable year;
    public IntWritable month;
    public static final String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    private static final Comparator<YearMonthWritable> YMCMP =
        comparing((YearMonthWritable yearMonth) -> yearMonth.year).thenComparing(yearMonth -> yearMonth.month);

    @Override
    public int compareTo(YearMonthWritable that) {
        return YMCMP.compare(this, that);
    }
}