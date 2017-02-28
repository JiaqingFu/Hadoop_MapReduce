import java.io.IOException;
import java.lang.*;
import java.util.StringTokenizer;

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


/**
 * Created by fujiaqing on 2016/5/15 0015.
 */
public class filter {
    //�̳�mapper�ӿڣ�����map����������Ϊ<Object,Text>
    //�������Ϊ<Text,IntWritable>
    public static class Map extends Mapper<Object,Text,Text,Text> {
        //one��ʾ���ʳ���һ��
        private static IntWritable one = new IntWritable(1);
        //word�洢���µĵ���
        private Text word1= new Text();
        private Text word2 = new Text();
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            //����������д�
            String str = value.toString();
            String[] res=str.split("\t");
            if (res.length<7){return;}
            if(res[4].equals("qq")){
            String u_s=res[1]+"\t"+res[4];
            String u_d=res[5]+"\t"+res[6]+"\t"+"1";

            word1.set(u_s);
            word2.set(u_d);
            context.write(word1, word2);}
            //char[] ch = str.toCharArray();
//            for(int i = 0; i < ch.length ; i++){
//                if(ch[i] <= 126 && ch[i] >= 33){
//                    word.set(String.valueOf(ch[i]));
//                    context.write(word, one);
//                }
//            }
//            StringTokenizer st = new StringTokenizer(value.toString());
//            while(st.hasMoreTokens()){
//                word.set(st.nextToken());//���µĵ��ʴ���word
//                context.write(word, one);
//            }
        }
    }
    //�̳�reducer�ӿڣ�����reduce����������<Text,IntWritable>
    //�������Ϊ<Text,IntWritable>
    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        //result��¼���ʵ�Ƶ��
        private static Text text1 = new Text();
        // private static Text text2 = new Text();
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            int up = 0;
            int down=0;
            int count=0;
            //�Ի�ȡ��<key,value-list>����value�ĺ�
            try{
                for(Text val:values){
                    String[] dat=val.toString().split("\t");
                    up+=Long.parseLong(dat[0]);
                    down+=Long.parseLong(dat[1]);
                    count+=Long.parseLong(dat[2]);
                    //sum += val.get();
                } }
            catch (Exception e){}
            //��Ƶ�����õ�result
            text1.set(String.valueOf(count)+"\t"+String.valueOf(up)+"\t"+String.valueOf(down));
            //�ռ����
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
        //�����������
        //String[] args={"C:\\access.log","C:\\output"};
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage WordCount <int> <out>");
            System.exit(2);
        }
        //������ҵ��
        Job job = new Job(conf,"sp count");
        //������ҵ������
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
