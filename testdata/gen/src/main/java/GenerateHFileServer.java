import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GenerateHFileServer {

    static class Config {
        String outputPath;
        String compression = "NONE";
        String dataBlockEncoding = "NONE";
        boolean includeTags = false;
        int blockSize = 65536;
        int cellCount = 10;
        String[] families = {"cf"};
        String[] qualifiers = {"q"};
        int valueSize = 6;
        long timestamp = 1700000000000L;
    }

    public static void main(String[] args) throws IOException {
        Gson gson = new Gson();
        Configuration conf = HBaseConfiguration.create();
        // Disable block cache to avoid background threads and shutdown hooks.
        conf.setFloat("hfile.block.cache.size", 0);
        FileSystem fs = FileSystem.getLocal(conf);
        CacheConfig cacheConf = new CacheConfig(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;

            try {
                Config cfg = gson.fromJson(line, Config.class);
                generateHFile(conf, fs, cacheConf, cfg);
                System.out.println(cfg.outputPath);
                System.out.flush();
            } catch (Exception e) {
                System.err.println("ERROR: " + e.getMessage());
                e.printStackTrace(System.err);
                System.exit(1);
            }
        }
    }

    private static void generateHFile(Configuration conf, FileSystem fs, CacheConfig cacheConf, Config cfg) throws IOException {
        Path path = new Path(cfg.outputPath);

        HFileContext context = new HFileContextBuilder()
                .withCompression(Compression.Algorithm.valueOf(cfg.compression))
                .withDataBlockEncoding(DataBlockEncoding.valueOf(cfg.dataBlockEncoding))
                .withIncludesTags(cfg.includeTags)
                .withBlockSize(cfg.blockSize)
                .build();

        HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf)
                .withPath(fs, path)
                .withFileContext(context)
                .create();

        // Sort families and qualifiers to ensure HBase key order.
        String[] families = cfg.families.clone();
        String[] qualifiers = cfg.qualifiers.clone();
        Arrays.sort(families);
        Arrays.sort(qualifiers);

        // Generate cells in HBase sort order: row, family, qualifier.
        // For each row, iterate families then qualifiers.
        for (int i = 0; i < cfg.cellCount; i++) {
            for (String family : families) {
                for (String qualifier : qualifiers) {
                    byte[] row = Bytes.toBytes(String.format("row-%05d", i));
                    byte[] fam = Bytes.toBytes(family);
                    byte[] qual = Bytes.toBytes(qualifier);
                    byte[] value = generateValue(i, cfg.valueSize);

                    KeyValue kv;
                    if (cfg.includeTags) {
                        List<Tag> tags = new ArrayList<>();
                        // Tag type 1 (TTL_TAG_TYPE) with a simple value.
                        tags.add(new ArrayBackedTag((byte) 1, Bytes.toBytes(String.format("tag-%05d", i))));
                        kv = new KeyValue(row, fam, qual, cfg.timestamp, value, tags);
                    } else {
                        kv = new KeyValue(row, fam, qual, cfg.timestamp, value);
                    }
                    writer.append(kv);
                }
            }
        }

        writer.close();
    }

    private static byte[] generateValue(int index, int valueSize) {
        if (valueSize == 0) {
            return new byte[0];
        }
        // Zero-pad the index to fill valueSize bytes.
        String s = String.format("%0" + valueSize + "d", index);
        // If the formatted string is longer than valueSize (shouldn't happen for reasonable sizes),
        // take the last valueSize characters.
        if (s.length() > valueSize) {
            s = s.substring(s.length() - valueSize);
        }
        return Bytes.toBytes(s);
    }
}
