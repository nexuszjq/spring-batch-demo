# spring-batch-demo

使用 Spring Batch 清洗 500MB 以上的 CSV 大文件的示例项目，重点演示 mmap 与 sendfile 两种零拷贝读取方式，以及文件头尾的随机访问写法。

## 功能概览
- **文件请求化**：通过 Spring Batch 的“一个文件一个 Job 参数”实现请求化，不依赖 REST；每个文件描述串触发一次独立 Job。
- **流式读取**：自定义 `ItemReader` 按窗口/分片读取文件，避免一次性载入内存。
- **零拷贝优化**：
  - `MemoryMappedCsvReader`：利用 `FileChannel.map` 做内存映射。
  - `SendfileCsvReader`：利用 `FileChannel.transferTo`（sendfile）将数据推送到管道实现零拷贝路径。
- **写入优化**：`FileChannelLineWriter` 使用预分配的直接内存缓冲区复用，减少分配开销。
- **内存预分配**：应用启动后通过 `PreallocatedBufferPool` 预分配直接内存，重启时清空并复用。
- **随机访问头尾**：`RandomAccessHeaderFooterService` 预留头部空间并直接定位尾部写入，避免全量读写。
- **XML 配置**：批处理 Job/Step 均在 `src/main/resources/batch/jobs.xml` 中用传统 Spring XML 声明。

## 运行与调用
1. 启动并传入“文件请求化”参数（每个文件一个请求）：
   ```bash
   mvn spring-boot:run -Dspring-boot.run.arguments="--file.requests=MMAP:/data/raw.csv:/data/clean-mmap.csv --file.requests=SENDFILE:/data/raw2.csv:/data/clean-sendfile.csv"
   ```
   - `file.requests` 支持重复指定；格式为 `模式:输入路径:输出路径`，模式取值 `MMAP` 或 `SENDFILE`。
   - 每个描述串会触发一次独立 Job，互不干扰，符合“一个文件一个请求”。
2. 预留并写入文件头/尾可在程序中直接调用 `RandomAccessHeaderFooterService`：
   - 预留并写入头部：
     ```java
     headerFooterService.createFileWithReservedHeader(Path.of("/data/out.csv"));
     headerFooterService.writeHeader(Path.of("/data/out.csv"), "HEADER");
     ```
   - 追加尾部：
     ```java
     headerFooterService.appendFooter(Path.of("/data/out.csv"), "EOF");
     ```

## 代码结构
- `resources/batch/jobs.xml`：传统 Spring XML 定义的 Job/Step，选择 mmap 或 sendfile 读取器。
- `io/MemoryMappedCsvReader`：基于 mmap 的流式行读取。
- `io/SendfileCsvReader`：基于 sendfile/transferTo 的流式行读取。
- `io/FileChannelLineWriter`：预分配直接内存的写入器。
- `service/PreallocatedBufferPool`：应用启动即预分配直接内存，可重复利用。
- `service/RandomAccessHeaderFooterService`：随机访问写入头/尾。
- `service/FileJobLauncherService` & `service/FileRequestCommandLineRunner`：每个文件描述串触发一次处理，体现请求化。
