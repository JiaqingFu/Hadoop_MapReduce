import java.io.IOException;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.regex.Pattern;
//import com.kenai.jaffl.struct.Struct;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//hadoop jar ./target/bbb-1.0-SNAPSHOT.jar  filter  /user/hadooptest/chensha/access.log /user/hadooptest/fujiaqing/access_filter
public class spCount {
    //继承mapper接口，设置map的输入类型为<Object,Text>
    //输出类型为<Text,IntWritable>
    public static class Map extends Mapper<Object,Text,Text,Text>{
        //one表示单词出现一次
        private static IntWritable one = new IntWritable(1);
        //word存储切下的单词
        private Text word1= new Text();
        private Text word2 = new Text();
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            //对输入的行切词
            String str = value.toString();
            String[] res=str.split("\t");
            if (res.length<7){return;}
            String u_s=res[1]+"\t"+res[4];
            String u_d=res[5]+"\t"+res[6]+"\t"+"1";
            word1.set(u_s);
            word2.set(u_d);
            context.write(word1, word2);
            //char[] ch = str.toCharArray();
//            for(int i = 0; i < ch.length ; i++){
//                if(ch[i] <= 126 && ch[i] >= 33){
//                    word.set(String.valueOf(ch[i]));
//                    context.write(word, one);
//                }
//            }
//            StringTokenizer st = new StringTokenizer(value.toString());
//            while(st.hasMoreTokens()){
//                word.set(st.nextToken());//切下的单词存入word
//                context.write(word, one);
//            }
        }
    }
    //继承reducer接口，设置reduce的输入类型<Text,IntWritable>
    //输出类型为<Text,IntWritable>
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        //result记录单词的频数
        private static Text text1 = new Text();
        // private static Text text2 = new Text();
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            int up = 0;
            int down=0;
            int count=0;
            //对获取的<key,value-list>计算value的和
            try{
            for(Text val:values){
                String[] dat=val.toString().split("\t");
                up+=Long.parseLong(dat[0]);
                down+=Long.parseLong(dat[1]);
                count+=Long.parseLong(dat[2]);
                //sum += val.get();
            } }
            catch (Exception e){}
            //将频数设置到result
            text1.set(String.valueOf(count)+"\t"+String.valueOf(up)+"\t"+String.valueOf(down));
            //收集结果
            context.write(key, text1);
        }
    }
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://192.168.83.30:8020");
        //conf.set("hadoop.job.user", "fujiaqing");
        //检查运行命令
        //String[] args={"C:\\access.log","C:\\output"};
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage WordCount <int> <out>");
            System.exit(2);
        }
        //配置作业名
        Job job = new Job(conf,"sp count");
        //配置作业各个类
        job.setJarByClass(spCount.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
