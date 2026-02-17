import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class GenerateHFileServer {

    static class CellTemplate {
        int qualifierSize;
        int valueSize;
        long timestamp;
        int type;
        int tagSize;
    }

    static class RowGroup {
        int rowCount;
        int rowKeySize;
        CellTemplate[] cells;
    }

    static class Config {
        String outputPath;
        String compression = "NONE";
        String dataBlockEncoding = "NONE";
        String bloomType = "NONE";
        boolean includeTags = false;
        int blockSize = 65536;
        int cellCount = 10;
        String[] families = {"cf"};
        String[] qualifiers = {"q"};
        int valueSize = 6;
        long timestamp = 1700000000000L;

        // Recipe mode fields.
        long seed;
        String family;
        RowGroup[] groups;
    }

    public static void main(String[] args) throws IOException {
        Gson gson = new Gson();
        Configuration conf = HBaseConfiguration.create();

        FileSystem fs = FileSystem.getLocal(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;

            try {
                Config cfg = gson.fromJson(line, Config.class);
                if (cfg.groups != null) {
                    generateHFileFromRecipe(conf, fs, cfg);
                } else {
                    generateHFile(conf, fs, cfg);
                }
                System.out.println(cfg.outputPath);
                System.out.flush();
            } catch (Exception e) {
                System.err.println("ERROR: " + e.getMessage());
                e.printStackTrace(System.err);
                System.exit(1);
            }
        }
    }

    private static void generateHFile(Configuration conf, FileSystem fs, Config cfg) throws IOException {
        Path path = new Path(cfg.outputPath);

        // TODO: consider adding the following lines to switch between zstd-jni and hadoop-native
        // conf.set("hbase.io.compress.zstd.codec", "org.apache.hadoop.hbase.io.compress.zstd.ZstdCodec");
        // Compression.Algorithm.ZSTD.reload(conf);

        HFileContext context = new HFileContextBuilder()
                .withCompression(Compression.Algorithm.valueOf(cfg.compression))
                .withDataBlockEncoding(DataBlockEncoding.valueOf(cfg.dataBlockEncoding))
                .withIncludesTags(cfg.includeTags)
                .withBlockSize(cfg.blockSize)
                .build();

        BloomType bloom = BloomType.valueOf(cfg.bloomType);
        long maxKeyCount = (long) cfg.cellCount * cfg.families.length * cfg.qualifiers.length;

        StoreFileWriter writer = new StoreFileWriter.Builder(conf, fs)
                .withFilePath(path)
                .withFileContext(context)
                .withBloomType(bloom)
                .withMaxKeyCount(maxKeyCount)
                .build();

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

    // ---- Recipe mode ----

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] generateRowKey(long seed, int globalIdx, int size) {
        byte[] key = new byte[size];
        // First 4 bytes = big-endian uint32 of globalIdx (ensures ascending order).
        key[0] = (byte) (globalIdx >> 24);
        key[1] = (byte) (globalIdx >> 16);
        key[2] = (byte) (globalIdx >> 8);
        key[3] = (byte) globalIdx;
        if (size > 4) {
            MessageDigest md = sha256();
            md.update(longToLE(seed));
            md.update(longToLE(globalIdx));
            byte[] hash = md.digest();
            System.arraycopy(hash, 0, key, 4, Math.min(hash.length, size - 4));
        }
        return key;
    }

    private static byte[] generateQualifier(long seed, int globalIdx, int cellIdx, int size) {
        if (size == 0) {
            return new byte[0];
        }
        MessageDigest md = sha256();
        md.update(longToLE(seed));
        md.update(longToLE(globalIdx));
        md.update(longToLE(cellIdx));
        md.update(new byte[]{'q'});
        byte[] hash = md.digest();
        return Arrays.copyOf(hash, Math.min(hash.length, size));
    }

    private static byte[] generateRecipeValue(long seed, int globalIdx, int cellIdx, int size) {
        if (size == 0) {
            return new byte[0];
        }
        MessageDigest md = sha256();
        md.update(longToLE(seed));
        md.update(longToLE(globalIdx));
        md.update(longToLE(cellIdx));
        md.update(new byte[]{'v'});
        byte[] block = md.digest();
        byte[] result = new byte[size];
        int off = 0;
        while (off < size) {
            int n = Math.min(block.length, size - off);
            System.arraycopy(block, 0, result, off, n);
            off += n;
        }
        return result;
    }

    private static byte[] generateRecipeTag(long seed, int globalIdx, int cellIdx, int size) {
        if (size == 0) {
            return new byte[0];
        }
        MessageDigest md = sha256();
        md.update(longToLE(seed));
        md.update(longToLE(globalIdx));
        md.update(longToLE(cellIdx));
        md.update(new byte[]{'t'});
        byte[] block = md.digest();
        byte[] result = new byte[size];
        int off = 0;
        while (off < size) {
            int n = Math.min(block.length, size - off);
            System.arraycopy(block, 0, result, off, n);
            off += n;
        }
        return result;
    }

    private static byte[] longToLE(long v) {
        byte[] buf = new byte[8];
        ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putLong(v);
        return buf;
    }

    // Holds an expanded cell for sorting before writing.
    static class ExpandedCell {
        byte[] row;
        byte[] family;
        byte[] qualifier;
        long timestamp;
        byte type;
        byte[] value;
        List<Tag> tags;
    }

    private static void generateHFileFromRecipe(Configuration conf, FileSystem fs, Config cfg) throws IOException {
        Path path = new Path(cfg.outputPath);

        // Detect whether any cell template has tags.
        boolean hasTags = false;
        for (RowGroup group : cfg.groups) {
            for (CellTemplate ct : group.cells) {
                if (ct.tagSize > 0) {
                    hasTags = true;
                    break;
                }
            }
            if (hasTags) break;
        }

        HFileContext context = new HFileContextBuilder()
                .withCompression(Compression.Algorithm.valueOf(cfg.compression))
                .withDataBlockEncoding(DataBlockEncoding.valueOf(cfg.dataBlockEncoding))
                .withIncludesTags(hasTags)
                .withBlockSize(cfg.blockSize)
                .build();

        BloomType bloom = BloomType.valueOf(cfg.bloomType);

        // Estimate max key count for bloom filter sizing.
        long maxKeyCount = 0;
        for (RowGroup group : cfg.groups) {
            maxKeyCount += (long) group.rowCount * group.cells.length;
        }

        StoreFileWriter writer = new StoreFileWriter.Builder(conf, fs)
                .withFilePath(path)
                .withFileContext(context)
                .withBloomType(bloom)
                .withMaxKeyCount(maxKeyCount)
                .build();

        byte[] familyBytes = Bytes.toBytes(cfg.family);

        int globalIdx = 0;
        for (RowGroup group : cfg.groups) {
            for (int r = 0; r < group.rowCount; r++) {
                byte[] rowKey = generateRowKey(cfg.seed, globalIdx, group.rowKeySize);

                // Expand all cells for this row.
                List<ExpandedCell> rowCells = new ArrayList<>();
                for (int c = 0; c < group.cells.length; c++) {
                    CellTemplate ct = group.cells[c];
                    ExpandedCell ec = new ExpandedCell();
                    ec.row = rowKey;
                    ec.family = familyBytes;
                    ec.qualifier = generateQualifier(cfg.seed, globalIdx, c, ct.qualifierSize);
                    ec.timestamp = ct.timestamp;
                    ec.type = (byte) ct.type;
                    ec.value = generateRecipeValue(cfg.seed, globalIdx, c, ct.valueSize);
                    if (ct.tagSize > 0) {
                        ec.tags = new ArrayList<>();
                        ec.tags.add(new ArrayBackedTag((byte) 1, generateRecipeTag(cfg.seed, globalIdx, c, ct.tagSize)));
                    }
                    rowCells.add(ec);
                }

                // Sort cells within a row: qualifier ASC, timestamp DESC, type DESC.
                rowCells.sort(Comparator
                        .<ExpandedCell, byte[]>comparing(e -> e.qualifier, Bytes::compareTo)
                        .thenComparing(e -> e.timestamp, Comparator.reverseOrder())
                        .thenComparing(e -> e.type, Comparator.reverseOrder()));

                // Deduplicate: remove cells with same (qualifier, timestamp, type).
                List<ExpandedCell> deduped = new ArrayList<>();
                for (int i = 0; i < rowCells.size(); i++) {
                    if (i == 0) {
                        deduped.add(rowCells.get(i));
                        continue;
                    }
                    ExpandedCell prev = rowCells.get(i - 1);
                    ExpandedCell cur = rowCells.get(i);
                    if (Bytes.compareTo(prev.qualifier, cur.qualifier) == 0 &&
                            prev.timestamp == cur.timestamp &&
                            prev.type == cur.type) {
                        continue; // duplicate
                    }
                    deduped.add(cur);
                }

                for (ExpandedCell ec : deduped) {
                    KeyValue kv;
                    if (ec.tags != null) {
                        kv = new KeyValue(ec.row, ec.family, ec.qualifier, ec.timestamp,
                                KeyValue.Type.codeToType(ec.type), ec.value, ec.tags);
                    } else {
                        kv = new KeyValue(ec.row, ec.family, ec.qualifier, ec.timestamp,
                                KeyValue.Type.codeToType(ec.type), ec.value);
                    }
                    writer.append(kv);
                }

                globalIdx++;
            }
        }

        writer.close();
    }
}
