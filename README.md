# 91VIP视频爬虫插件

AstrBot插件，支持爬取91porn视频列表和下载视频

## 功能特性

- 🎬 **91porn视频列表爬取**: 支持多种分类和视图类型，可输出CSV和JSONL格式
- 📥 **视频下载**: 支持批量下载视频，自动过滤长视频
- 🔄 **异步任务管理**: 支持任务状态查询、取消等操作
- ⚙️ **配置管理**: 支持自定义爬虫参数和行为
- 📊 **进度反馈**: 实时显示任务进度和状态

## 安装要求

- Python 3.7+
- AstrBot框架
- 依赖包（见requirements.txt）

## 安装步骤

1. 将插件文件复制到AstrBot插件目录
2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```
3. 在AstrBot中启用插件

## 使用方法

### 基本命令

#### 爬取91porn视频列表
```
/91porn [分类] [页数] [格式]
```

参数说明：
- 分类：rf(热门), mv(最新), vd(视频)等，默认为rf
- 页数：要爬取的页数，默认为5
- 格式：jsonl或csv，默认为jsonl

示例：
```
/91porn rf 3 jsonl    # 爬取热门分类前3页，输出JSONL格式
/91porn mv 5 csv      # 爬取最新分类前5页，输出CSV格式
/91porn               # 使用默认参数
```

#### 下载视频
```
/dlvideo [页面范围] [最大时长]
```

参数说明：
- 页面范围：如"1-3"或"5"，默认为"1-3"
- 最大时长：最大视频时长(分钟)，默认为20分钟

示例：
```
/dlvideo 1-3 20       # 下载1-3页的视频，最大20分钟
/dlvideo 5 15         # 下载第5页的视频，最大15分钟
/dlvideo              # 使用默认参数
```

#### 查看任务状态
```
/status [任务ID]
```

示例：
```
/status abc123        # 查看特定任务状态
/status               # 查看所有任务状态
```

#### 取消任务
```
/cancel <任务ID>
```

示例：
```
/cancel abc123        # 取消任务abc123
```

#### 显示帮助
```
/91help
```

## 配置文件

插件配置文件位于 `config/scraper_config.json`，包含以下配置项：

### 91porn爬虫配置
```json
{
  "91porn": {
    "enabled": true,                    // 是否启用
    "default_category": "rf",           // 默认分类
    "default_viewtype": "basic",        // 默认视图类型
    "default_max_pages": 5,             // 默认最大页数
    "default_workers": 3,               // 默认工作线程数
    "default_delay": 1.0,               // 默认请求延迟(秒)
    "default_timeout": 15.0,            // 默认超时时间(秒)
    "user_agent": "..."                 // 用户代理
  }
}
```

### 视频下载配置
```json
{
  "video_download": {
    "enabled": true,                    // 是否启用
    "base_url": "https://zvm.xinhua107.com/",
    "favorite_url": "https://zvm.xinhua107.com/video/category/most-favorite/",
    "cdns": ["cdn2.jiuse3.cloud"],      // CDN列表
    "default_pages": "1-3",             // 默认页面范围
    "max_video_duration": 20,           // 最大视频时长(分钟)
    "download_workers": 4,              // 下载线程数
    "output_dir": "downloads"           // 输出目录
  }
}
```

### 通用配置
```json
{
  "general": {
    "output_dir": "outputs",            // 通用输出目录
    "max_concurrent_tasks": 2,          // 最大并发任务数
    "task_timeout": 300,                // 任务超时时间(秒)
    "enable_notifications": true        // 是否启用通知
  }
}
```

## 输出文件

### 91porn爬虫输出
- JSONL格式：`outputs/91porn_分类_时间戳.jsonl`
- CSV格式：`outputs/91porn_分类_时间戳.csv`

### 视频下载输出
- 下载的视频：`downloads/页面-序号-标题-收藏数-上传者-日期.mp4`

## 文件结构

```
astrbot_plugin_91vip/
├── main.py                    # 主插件文件
├── metadata.yaml              # 插件元数据
├── requirements.txt           # 依赖包列表
├── scraper_91porn.py         # 91porn爬虫模块
├── scraper_video_download.py # 视频下载模块
├── scraper_manager.py        # 爬虫管理器
├── test_scrapers_simple.py   # 测试脚本
├── config/
│   └── scraper_config.json   # 配置文件
├── outputs/                  # 输出目录
└── downloads/                # 下载目录
```

## 测试

运行测试脚本验证功能：
```bash
python test_scrapers_simple.py
```

## 注意事项

1. **合法使用**: 请确保遵守相关法律法规和网站使用条款
2. **频率限制**: 插件已内置请求延迟，避免过于频繁的请求
3. **存储空间**: 视频下载可能需要大量存储空间，请确保有足够空间
4. **网络环境**: 爬虫功能需要稳定的网络连接
5. **权限要求**: 确保插件有读写输出目录的权限

## 故障排除

### 常见问题

1. **导入错误**: 确保已安装所有依赖包
2. **权限错误**: 检查输出目录的读写权限
3. **网络错误**: 检查网络连接和防火墙设置
4. **配置错误**: 检查配置文件格式和内容

### 日志查看

插件运行日志会输出到AstrBot的日志系统中，可以通过查看日志来诊断问题。

## 更新日志

### v1.0.0
- 初始版本发布
- 支持91porn视频列表爬取
- 支持视频下载功能
- 实现异步任务管理
- 添加配置文件支持

## 支持

如有问题或建议，请通过以下方式联系：
- 提交Issue到项目仓库
- 加入讨论群组

## 许可证

本插件遵循相应的开源许可证，详情请查看LICENSE文件。
