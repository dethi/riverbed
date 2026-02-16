import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GenerateTestHFile {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: GenerateTestHFile <output-path>");
            System.exit(1);
        }

        String outputPath = args[0];
        Configuration conf = HBaseConfiguration.create();
        FileSystem fs = FileSystem.getLocal(conf);
        Path path = new Path(outputPath);

        HFileContext context = new HFileContextBuilder()
                .withCompression(Compression.Algorithm.NONE)
                .withDataBlockEncoding(DataBlockEncoding.NONE)
                .withIncludesTags(false)
                .build();

        HFile.Writer writer = HFile.getWriterFactoryNoCache(conf)
                .withPath(fs, path)
                .withFileContext(context)
                .create();

        byte[] family = Bytes.toBytes("cf");
        byte[] qualifier = Bytes.toBytes("q");
        long timestamp = 1700000000000L;

        for (int i = 0; i < 10; i++) {
            byte[] row = Bytes.toBytes(String.format("row-%02d", i));
            byte[] value = Bytes.toBytes(String.format("val-%02d", i));
            KeyValue kv = new KeyValue(row, family, qualifier, timestamp, value);
            writer.append(kv);
        }

        writer.close();
        System.out.println("Wrote " + outputPath);
    }
}
