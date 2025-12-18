package com.example.batchdemo.web;

/**
 * API 请求体：每个请求绑定一个大文件路径，指定处理模式与输出路径。
 */
public class FileProcessingRequest {
    private String inputPath;
    private String outputPath;
    private Mode mode = Mode.MMAP;

    public enum Mode {
        MMAP, SENDFILE
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }
}
