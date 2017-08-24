package com.jihf.mr.excel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

public class ExcelInputFormat extends FileInputFormat<LongWritable, Text> {
    public ExcelInputFormat() {
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        return new ExcelRecordReader();
    }

    public class ExcelRecordReader extends RecordReader<LongWritable, Text> {
        private LongWritable key = new LongWritable(-1);
        private Text value = new Text();
        private InputStream inputStream;
        private String[] strArrayOfLines;

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            // 分片
            FileSplit split = (FileSplit) genericSplit;
            // 获取配置
            Configuration job = context.getConfiguration();

            // 分片路径
            Path filePath = split.getPath();

            FileSystem fileSystem = filePath.getFileSystem(job);

            inputStream = fileSystem.open(split.getPath());

            // 调用解析excel方法            
            this.strArrayOfLines = ExcelParser.parseExcelData(inputStream);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            int pos = (int) key.get() + 1;

            if (pos < strArrayOfLines.length) {

                if (strArrayOfLines[pos] != null) {
                    key.set(pos);
                    value.set(strArrayOfLines[pos]);

                    return true;
                }
            }

            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}