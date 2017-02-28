/**
 * Created by fujiaqing on 2016/5/15 0015.
 */
import java.lang.*;
import java.util.regex.Pattern;

public class test {
    public static void main(String[] args) throws Exception {
        String s = "qq.com123";
        Pattern pat=Pattern.compile("\\t");
        String[] str=pat.split("adsfsf"+"\t"+"qerwrw");
        System.out.println(str[1]);
        if (s.contains("qq.com")) {
                System.out.println("true");
        }
    }
}
