package HBase.api.CreateTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    @Test
    public void testGetBulk() throws IOException {
        /**
         * 使用GET批量获取数据
         */

        // 定义表名
        TableName tableName = TableName.valueOf("step1_student");
        // 获取表对象
        Table table = connection.getTable(tableName);

        ArrayList<Get> gets = new ArrayList<>();    // 创建Get集合
        List<String> rows = new ArrayList<>();      // 定义个rowkey集合
        rows.add("2018"); rows.add("2020");

        for (String row : rows) {                   // 把rowkey转换成get对象，放入gets集合
            Get get = new Get(Bytes.toBytes(row));
            gets.add(get);
        }

        List<String> values = new ArrayList<>();
        Result[] results = table.get(gets);         // 批量获取gets
        for (Result result : results) {
            System.out.println("Row:"+Bytes.toString(result.getRow()));

            for (Cell kv : result.rawCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(kv));
                String qualifire = Bytes.toString(CellUtil.cloneQualifier(kv));
                String value = Bytes.toString(CellUtil.cloneValue(kv));
                values.add(value);
                System.out.println(family+":"+qualifire+"\t"+value);
            }
        }

        /**
         * Row:2018
         * basic_info:birthday	1987-05-23
         * basic_info:gender	male
         * basic_info:name	张飞
         * school_info:class	class 1 grade 2
         * school_info:college	ChengXing
         * school_info:object	Software
         * Row:2020
         * basic_info:birthday	1985-05-23
         * basic_info:gender	male
         * basic_info:name	黄忠
         * school_info:class	class 2 grade 2
         * school_info:college	Harvard
         * school_info:object	Construction
         */
    }

    @Test
    public void testDeleteBulk() throws IOException {
        /**
         * 删除单行数据
         * 删除多行数据
         */

        // 定义表名
        TableName tableName = TableName.valueOf("step1_student");
        // 获取表对象
        Table table = connection.getTable(tableName);

        byte[] row = Bytes.toBytes("row1");

        // 删除单行
        Delete delete = new Delete(row);    // 创建delete对象
        table.delete(delete);

        // 删除多行
        List<Delete> deletes = new ArrayList<>();
        for(int i = 1 ; i < 5;i++){
            byte[] row1 = Bytes.toBytes("row" + i);
            Delete delete1 = new Delete(row1);
            deletes.add(delete1);
        }
        table.delete(deletes);
    }

    @Test
    public void testInsertBulk() throws IOException {
        /**
         * 批量插入数据
         */

        // 定义表名
        TableName tableName = TableName.valueOf("stu");
        // 定义表对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        // 构建列族对象
        ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("basic_info")).build();
        ColumnFamilyDescriptor family1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("school_info")).build();
        // 设置列族
        tableDescriptorBuilder.setColumnFamily(family);
        tableDescriptorBuilder.setColumnFamily(family1);
        // 创建表
        admin.createTable(tableDescriptorBuilder.build());

        List<Put> puts = new ArrayList<>();
        // 循环添加数据
        for (int i = 1; i <= 4; i++) {
            byte[] row = Bytes.toBytes("row" + i);
            Put put = new Put(row);
            byte[] columnFamily = Bytes.toBytes("data");
            byte[] qualifier = Bytes.toBytes(String.valueOf(i));
            byte[] value = Bytes.toBytes("value" + i);
            put.addColumn(columnFamily, qualifier, value);
            puts.add(put);
        }
        Table table = connection.getTable(tableName);
        table.put(puts);

        byte[] row1 = Bytes.toBytes("20181122");
        byte[] row2 = Bytes.toBytes("20181123");
        Put put1 = new Put(row1);
        Put put2 = new Put(row2);
        byte[] columnFamily1 = Bytes.toBytes("basic_info");
        byte[] columnFamily2 = Bytes.toBytes("school_info");
        put1.addColumn(columnFamily1, Bytes.toBytes("name"), Bytes.toBytes("阿克蒙德"));
        put1.addColumn(columnFamily1, Bytes.toBytes("gender"), Bytes.toBytes("male"));
        put1.addColumn(columnFamily1, Bytes.toBytes("birthday"), Bytes.toBytes("1987-05-23"));
        put1.addColumn(columnFamily1, Bytes.toBytes("connect"), Bytes.toBytes("tel:13974036666"));
        put1.addColumn(columnFamily1, Bytes.toBytes("address"), Bytes.toBytes("HuNan-ChangSha"));
        put1.addColumn(columnFamily2, Bytes.toBytes("college"), Bytes.toBytes("ChengXing"));
        put1.addColumn(columnFamily2, Bytes.toBytes("class"), Bytes.toBytes("class 1 grade 2"));
        put1.addColumn(columnFamily2, Bytes.toBytes("object"), Bytes.toBytes("Software"));

        put2.addColumn(columnFamily1, Bytes.toBytes("name"), Bytes.toBytes("萨格拉斯"));
        put2.addColumn(columnFamily1, Bytes.toBytes("gender"), Bytes.toBytes("male"));
        put2.addColumn(columnFamily1, Bytes.toBytes("birthday"), Bytes.toBytes("1986-05-23"));
        put2.addColumn(columnFamily1, Bytes.toBytes("connect"), Bytes.toBytes("tel:18774036666"));
        put2.addColumn(columnFamily1, Bytes.toBytes("address"), Bytes.toBytes("HuNan-ChangSha"));
        put2.addColumn(columnFamily2, Bytes.toBytes("college"), Bytes.toBytes("ChengXing"));
        put2.addColumn(columnFamily2, Bytes.toBytes("class"), Bytes.toBytes("class 2 grade 2"));
        put2.addColumn(columnFamily2, Bytes.toBytes("object"), Bytes.toBytes("Software"));

        puts.add(put1);puts.add(put2);
        Table table2 = connection.getTable(tableName);
        table2.put(puts);
    }

    @Test
    public void testListTable() throws IOException {
        /**
         * 列出Hbase中所有表的名字，输出表是否存在，表是否可用
         */

        // 定义表名
        // TableName tableName = TableName.valueOf("step1_student");

        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();

        for (TableDescriptor tableDescriptor : tableDescriptors) {
            TableName tableName = tableDescriptor.getTableName();   // 获取表名
            System.out.println("Table："+tableName.getNameAsString());
            tableDescriptor.getColumnFamilies();    // 获取所有列族

            boolean b = admin.tableExists(tableName);// 存在返回true
            System.out.println("\texists："+b);
            boolean c = admin.isTableEnabled(tableName);// 可用返回true
            System.out.println("\tenabled："+c);
        }
    }
}
