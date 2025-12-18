# spring-batch-demo

使用 Spring Batch 清洗 500MB 以上的 CSV 大文件的示例项目，重点演示 mmap 与 sendfile 两种零拷贝读取方式，以及文件头尾的随机访问写法。

## 功能概览
- **文件请求化**：每个大文件通过一个 HTTP 请求触发一次 Job，互不干扰。
- **流式读取**：自定义 `ItemReader` 按窗口/分片读取文件，避免一次性载入内存。
- **零拷贝优化**：
  - `MemoryMappedCsvReader`：利用 `FileChannel.map` 做内存映射。
  - `SendfileCsvReader`：利用 `FileChannel.transferTo`（sendfile）将数据推送到管道实现零拷贝路径。
- **写入优化**：`FileChannelLineWriter` 使用预分配的直接内存缓冲区复用，减少分配开销。
- **内存预分配**：应用启动后通过 `PreallocatedBufferPool` 预分配直接内存，重启时清空并复用。
- **随机访问头尾**：`RandomAccessHeaderFooterService` 预留头部空间并直接定位尾部写入，避免全量读写。

## 运行与调用
1. 启动服务：
   ```bash
   mvn spring-boot:run
   ```
2. 触发处理（一个文件一个请求）：
   ```bash
   curl -X POST http://localhost:8080/files/process \
        -H "Content-Type: application/json" \
        -d '{"inputPath":"/data/raw.csv","outputPath":"/data/clean-mmap.csv","mode":"MMAP"}'
   ```
   或者使用 sendfile 模式：`"mode":"SENDFILE"`。
3. 预留并写入文件头：
   ```bash
   curl -X POST "http://localhost:8080/files/header?path=/data/out.csv&header=HEADER"
   ```
4. 追加文件尾：
   ```bash
   curl -X POST "http://localhost:8080/files/footer?path=/data/out.csv&footer=EOF"
   ```

## 代码结构
- `config/BatchJobConfig`：Job/Step 定义，选择 mmap 或 sendfile 读取器。
- `io/MemoryMappedCsvReader`：基于 mmap 的流式行读取。
- `io/SendfileCsvReader`：基于 sendfile/transferTo 的流式行读取。
- `io/FileChannelLineWriter`：预分配直接内存的写入器。
- `service/PreallocatedBufferPool`：应用启动即预分配直接内存，可重复利用。
- `service/RandomAccessHeaderFooterService`：随机访问写入头/尾。
- `service/FileJobLauncherService` & `web/FileProcessingController`：每个请求触发一次处理。
