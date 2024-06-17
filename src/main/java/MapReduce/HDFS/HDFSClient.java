package MapReduce.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 */
public class HDFSClient {

    private FileSystem fs;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {

        //
        URI uri = new URI("hdfs://");
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(uri, conf, "xxx");
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdir() {

    }
}
