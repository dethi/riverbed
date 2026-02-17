import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.util.Bytes;

public class GenerateSeekTestHFile {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: GenerateSeekTestHFile <output-path>");
            System.exit(1);
        }

        String outputPath = args[0];
        Configuration conf = HBaseConfiguration.create();
        FileSystem fs = FileSystem.getLocal(conf);
        Path path = new Path(outputPath);

        byte[] family = Bytes.toBytes("cf");
        byte[] qualifier = Bytes.toBytes("q");
        long timestamp = 1700000000000L;
        int numRows = 100;

        // Use a small block size to force multiple data blocks.
        HFileContext context = new HFileContextBuilder()
            .withCompression(Compression.Algorithm.NONE)
            .withDataBlockEncoding(DataBlockEncoding.NONE)
            .withBlockSize(256)
            .withIncludesTags(false)
            .build();

        StoreFileWriter writer = new StoreFileWriter.Builder(conf, fs)
            .withFilePath(path)
            .withFileContext(context)
            .withBloomType(BloomType.ROW)
            .withMaxKeyCount(numRows)
            .build();

        for (int i = 0; i < numRows; i++) {
            byte[] row = Bytes.toBytes(String.format("row-%03d", i));
            byte[] value = Bytes.toBytes(String.format("val-%03d", i));
            KeyValue kv = new KeyValue(row, family, qualifier, timestamp, value);
            writer.append(kv);
        }

        writer.close();
        System.out.println("Wrote " + outputPath);
    }
}
