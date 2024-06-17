package HBase.api.CreateTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 1. 使用HBaseConfiguration创建config对象（读取指定路径下hbase-site.xml和hbase-default.xml的配置信息）
 * 2. 连接HBase
 */
public class createTable {

    private Admin admin;
    private Connection connection;

    @Before
    public void init() throws IOException {

        // 1. 使用HBaseConfiguration创建config对象
        Configuration config = HBaseConfiguration.create();
        // 2. 连接HBase
        Connection connection = ConnectionFactory.createConnection(config);
        // 3. 获取admin对象
        Admin admin = connection.getAdmin();
    }

    @Test
    public void testCreateTable() throws IOException {

        // 定义表名
        TableName tableName = TableName.valueOf("test");
        // 定义表对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        // 构建列族对象
        ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data")).build();
        // 设置列族
        tableDescriptorBuilder.setColumnFamily(family);
        // 创建表
        admin.createTable(tableDescriptorBuilder.build());
    }

    @Test
    public void testInsert() throws IOException {
        /**
         * 给表添加数据
         */

        // 定义表名
        TableName tableName = TableName.valueOf("tb_step2");
        // 定义表对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        // 构建列族对象
        ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data")).build();
        // 设置列族
        tableDescriptorBuilder.setColumnFamily(family);
        // 创建表
        admin.createTable(tableDescriptorBuilder.build());

        // 获取表对象
        Table table = connection.getTable(tableName);
        try {
            // 定义行
            byte[] row = Bytes.toBytes("row1");
            // 创建put对象
            Put put = new Put(row);
            // 列族
            byte[] columnFamily = Bytes.toBytes("data");
            // 列
            byte[] qualifier = Bytes.toBytes(String.valueOf(1));
            // 值
            byte[] value = Bytes.toBytes("张三丰");
            put.addColumn(columnFamily, qualifier, value);
            // 像表中添加数据
            table.put(put);
        } finally {
            // 使用完释放资源
            table.close();
        }
    }

    @Test
    public void testScanTable() throws IOException {
        /**
         * 1. 使用GET对象获取单行数据
         * 2. 使用Scan批量输出表中数据
         */

        // 定义表名
        TableName tableName = TableName.valueOf("tb_step2");
        // 获取表对象
        Table table = connection.getTable(tableName);

        // 1. 使用GET对象获取单行数据
        Get get = new Get(Bytes.toBytes("row1"));   // 定义get对象
        Result result1 = table.get(get);    // 通过table对象获取数据
        System.out.println("Result: "+result1);
        byte[] valueBytes = result1.getValue(Bytes.toBytes("data"), Bytes.toBytes("1"));    // 获取data:1列族的值
        String s = new String(valueBytes, "utf-8");     // 将字节码转换成字符串
        System.out.println("value: "+s);

        // 2. 使用Scan批量输出表中数据
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                System.out.println("Scan: "+result);    // Scan: keyvalues={row1/data:1/1542657887632/Put/vlen=6/seqid=0}
                byte[] row = result.getRow();
                System.out.println("rowName: "+new String(row, "utf-8"));   // rowName:row1
            }
        } finally {
            scanner.close();
        }
    }

    @Test
    public void testDeleteTable() throws IOException {
        /**
         * 删除表前，先禁用
         */

        TableName tableName = TableName.valueOf("test");
        admin.disableTable(tableName);      //禁用表
        admin.deleteTable(tableName);        //删除表
    }
}
