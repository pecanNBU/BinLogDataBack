import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] args) {
        String str="ahbamysql-bin.0085.taraaa";
        String regex="(^mysql)(.*?)(tar$)";
        Pattern pattern=Pattern.compile(regex);
        Matcher m=pattern.matcher(str);
        System.out.println("hello world!");
        System.out.println(m.matches());
        if(m.find())
        {
            System.out.println(m.group(1));
        }
    }
}
