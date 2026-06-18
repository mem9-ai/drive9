# Drive9 文件接入说明

Drive9 支持两种文件接入方式：

- **Mount 方式**：把 Drive9 挂载成本地目录，应用像读写本地文件一样访问 Drive9。
- **Go SDK 方式**：Go 服务直接调用 SDK API 读写 Drive9 文件。

如果已有程序只接受本地路径，优先使用 Mount；如果 Go 服务需要在代码中控制读写和错误处理，使用 Go SDK。

## 1. 快速开始

### 1.1 配置认证

```bash
export DRIVE9_SERVER="https://your-drive9.example"
export DRIVE9_API_KEY="drive9_xxx"
```

### 1.2 Mount 快速验证

```bash
mkdir -p /mnt/drive9
drive9 mount /mnt/drive9

echo "hello drive9" > /mnt/drive9/hello.txt
cat /mnt/drive9/hello.txt

drive9 umount /mnt/drive9
```

### 1.3 Go SDK 快速验证

```go
package main

import (
    "context"
    "fmt"
    "os"
    "time"

    "github.com/mem9-ai/dat9/pkg/client"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    c := client.New(os.Getenv("DRIVE9_SERVER"), os.Getenv("DRIVE9_API_KEY"))
    c.SetActor("customer-service")

    // 推荐初始化一次，提升后续上传性能。
    c.Warm(ctx)

    if err := c.WriteCtx(ctx, "/sdk-demo/hello.txt", []byte("hello drive9")); err != nil {
        panic(err)
    }

    data, err := c.ReadCtx(ctx, "/sdk-demo/hello.txt")
    if err != nil {
        panic(err)
    }

    fmt.Println(string(data))
}
```

## 2. 选择哪种方式

| 场景 | 推荐方式 |
| --- | --- |
| 已有程序只认本地文件路径，希望最少改代码 | Mount |
| Go 服务需要直接调用 API 读写文件 | Go SDK |
| 需要在代码里处理错误和并发写保护 | Go SDK |
| 需要先写入隔离层，检查后再提交 | Go SDK + Layer |

## 3. Mount：像本地文件系统一样读写

### 3.1 挂载根目录

```bash
mkdir -p /mnt/drive9
drive9 mount /mnt/drive9
```

### 3.2 挂载指定远程目录

```bash
mkdir -p /mnt/drive9
drive9 mount :/workspace /mnt/drive9
```

### 3.3 写文件

```bash
echo "hello drive9" > /mnt/drive9/hello.txt
cp ./local-file.bin /mnt/drive9/local-file.bin
```

### 3.4 读文件

```bash
cat /mnt/drive9/hello.txt
cp /mnt/drive9/local-file.bin ./downloaded-file.bin
```

### 3.5 卸载

```bash
drive9 umount /mnt/drive9
```

## 4. Go SDK：在 Go 服务中直接读写

### 4.1 安装

```bash
go get github.com/mem9-ai/dat9@latest
```

```go
import "github.com/mem9-ai/dat9/pkg/client"
```

### 4.2 初始化客户端

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

c := client.New(os.Getenv("DRIVE9_SERVER"), os.Getenv("DRIVE9_API_KEY"))
c.SetActor("customer-service")

// 推荐初始化一次，提升后续上传性能。
c.Warm(ctx)
```

### 4.3 写入内存数据

```go
err := c.WriteCtx(ctx, "/workspace/hello.txt", []byte("hello drive9"))
if err != nil {
    return err
}
```

### 4.4 上传本地文件或 Reader

业务方不需要判断文件大小；SDK 会自动选择合适的上传方式。

```go
f, err := os.Open("./local-file.bin")
if err != nil {
    return err
}
defer f.Close()

info, err := f.Stat()
if err != nil {
    return err
}

_, err = c.WriteStreamWithSummary(
    ctx,
    "/workspace/local-file.bin",
    f,
    info.Size(),
    nil,
)
if err != nil {
    return err
}
```

### 4.5 可选：并发写保护（CAS）

`expectedRevision=0` 表示只允许创建新文件；如果文件已存在，会返回冲突错误。

```go
rev, err := c.WriteCtxConditionalWithRevision(
    ctx,
    "/workspace/config.json",
    []byte(`{"version":1}`),
    0,
)
if err != nil {
    return err
}

_ = rev
```

### 4.6 读取完整文件

```go
data, err := c.ReadCtx(ctx, "/workspace/hello.txt")
if err != nil {
    return err
}

_ = data
```

### 4.7 范围读取

```go
part, err := c.ReadAtCtx(ctx, "/workspace/local-file.bin", 0, 4096)
if err != nil {
    return err
}

_ = part
```

### 4.8 下载到本地文件

`DownloadToFile` 需要文件大小，可先 `StatCtx`。

```go
st, err := c.StatCtx(ctx, "/workspace/local-file.bin")
if err != nil {
    return err
}

err = c.DownloadToFile(ctx, "/workspace/local-file.bin", "./downloaded-file.bin", st.Size)
if err != nil {
    return err
}
```

## 5. Go SDK 常用文件 API

| 功能 | 方法 |
| --- | --- |
| 初始化客户端性能参数 | `Warm(ctx)` |
| 写入内存数据 | `WriteCtx(ctx, path, data)` |
| 上传本地文件/Reader | `WriteStreamWithSummary(ctx, path, reader, size, progress)` |
| 可选：并发写保护（CAS） | `WriteCtxConditionalWithRevision(ctx, path, data, expectedRevision)` |
| 读取完整文件 | `ReadCtx(ctx, path)` |
| 范围读取 | `ReadAtCtx(ctx, path, offset, length)` |
| 下载到本地文件 | `DownloadToFile(ctx, remotePath, localPath, size)` |
| 获取元信息 | `StatCtx(ctx, path)` |
| 创建目录 | `MkdirCtx(ctx, path, mode)` |
| 列目录 | `ListCtx(ctx, path)` |
| 删除目录/文件 | `RemoveAllCtx(ctx, path)` |
| 复制 | `CopyCtx(ctx, srcPath, dstPath)` |
| 重命名/移动 | `RenameCtx(ctx, oldPath, newPath)` |

## 6. 可选：Layer 隔离写入

Layer 适合“先在隔离层写入，检查后再提交”的工作流。普通文件读写不需要 Layer。

### 6.1 创建 Layer

```go
layer, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
    BaseRootPath: "/workspace",
    Name:         "demo-layer",
})
if err != nil {
    return err
}
```

### 6.2 向 Layer 写文件

```go
reader := strings.NewReader("hello layer")
entry, err := c.UploadFSLayerFile(
    ctx,
    layer.LayerID,
    "/workspace/hello.txt",
    reader,
    int64(reader.Len()),
    0,
    0644,
    true,
)
if err != nil {
    return err
}

_ = entry
```

## 7. 常见问题

### Q: 大文件需要业务方自己分片吗？

不需要。Drive9 会自动处理，业务方使用统一写入接口即可。

### Q: 为什么建议调用 `Warm(ctx)`？

`Warm(ctx)` 用于初始化客户端性能参数。建议服务启动后调用一次。

### Q: 多个客户端可能同时写同一个文件怎么办？

需要并发写保护时，可以使用 `WriteCtxConditionalWithRevision`。如果 revision 不匹配，SDK 会返回冲突错误。

### Q: 如何判断 CAS 冲突？

```go
if errors.Is(err, client.ErrConflict) {
    // revision/CAS 冲突
}
```

## 8. 验证 Demo

仓库中提供了 Go SDK smoke demo：`examples/go-sdk-smoke/`。

本地 mock 测试：

```bash
go test ./examples/go-sdk-smoke
go run ./examples/go-sdk-smoke -mock
```

真实 Drive9 服务测试：

```bash
export DRIVE9_SERVER="https://your-drive9.example"
export DRIVE9_API_KEY="drive9_xxx"

go run ./examples/go-sdk-smoke -root /go-sdk-smoke-$(date -u +%Y%m%dT%H%M%SZ)
```

成功输出包含：

```text
drive9 Go SDK smoke passed
drive9 Go SDK smoke passed
```

看到该输出说明 SDK 基础文件读写和上传流程验证通过。
