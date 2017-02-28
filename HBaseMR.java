import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class HBaseMR {

	private static final String targetTable = "hbasemr_fujiaqing";
	static Configuration config = HBaseConfiguration.create();

	public static void createTable(String tablename, String[] cfs)
			throws IOException {
		// 新建一个 HBaseAdmin 类的对象，其用来管理表元数据的操作，包括建表删表等
		HBaseAdmin admin = new HBaseAdmin(config);
		if (admin.tableExists(tablename)) {
			// 如果表已经存在
			System.out.println("table already exists");
		} else {
			//如果表不存在，执行以下动作新建
			//构建一个 HTableDescriptor 对象，其包含表的详细信息
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			//将列族一一加到 tableDesc 对象中
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			//创建表
			admin.createTable(tableDesc);
			System.out.println("create table successly");
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		/*
		 * 使用 mapreduce 向 HBase 导入数据
		 */
		// 定义表列族为 colfamily
		String[] cfs = { "colfamily" };
//		//设置 zookeeper
	    config.set("hbase.zookeeper.quorum", "192.168.83.33");
		//config.set("hbase.zookeeper.quorum", "10.0.2.15");
		// 创建一个 hbasemr 表，只有一个列族为 colfamily
		createTable(targetTable, cfs);
		
		// 新建作业，用 mapreduce 将 数据导入 hbase
//		final Job job = Job.getInstance(config, HBaseMR.class.getSimpleName());
		Job job = new Job(config, "HBaseMR");
		TableMapReduceUtil.addDependencyJars(job);
		job.setJarByClass(HBaseMR.class);
		
		//设置作业的 map 类
		job.setMapperClass(InputMapper.class);
		//设置作业的 reduce 处理类
		TableMapReduceUtil.initTableReducerJob(targetTable, Reducer.class, job);
		
		//作业输入路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//设置作业的输出类型
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//提交作业
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error");
		}
	}

	public static class InputMapper extends
			Mapper<Object, Text, NullWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// map 中什么也不做，直接将数据写出
			context.write(NullWritable.get(), value);
		}
	}

	public static class Reducer extends
			TableReducer<NullWritable, Text, ImmutableBytesWritable> {
		String time;
		String spName;
		String userID;
		String serverIP;
		String hostName;
		String uploadTraffic;
		String downloadTraffic;

		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text val : values) {
				String line = val.toString();
				String[] fields = line.split("\t");

				time = fields[0];
				userID = fields[1];
				serverIP = fields[2];
				hostName = fields[3];
				spName = fields[4];
				uploadTraffic = fields[5];
				downloadTraffic = fields[6];
				
				//设置 rowkey 为时间值
				Put put = new Put(Bytes.toBytes(time));
				// 列族，列，值
				put.add(Bytes.toBytes("colfamily"), Bytes.toBytes("userID"),
						Bytes.toBytes(userID));
				put.add(Bytes.toBytes("colfamily"), Bytes.toBytes("serverIP"),
						Bytes.toBytes(serverIP));
				put.add(Bytes.toBytes("colfamily"), Bytes.toBytes("hostName"),
						Bytes.toBytes(hostName));
				put.add(Bytes.toBytes("colfamily"), Bytes.toBytes("spName"),
						Bytes.toBytes(spName));
				put.add(Bytes.toBytes("colfamily"),
						Bytes.toBytes("uploadTraffic"),
						Bytes.toBytes(uploadTraffic));
				put.add(Bytes.toBytes("colfamily"),
						Bytes.toBytes("downloadTraffic"),
						Bytes.toBytes(downloadTraffic));
				
				//将数据写出
				context.write(null, put);
			}
		}
	}
}