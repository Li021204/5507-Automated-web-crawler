COM5507 group22 自动化爬虫

自动化爬虫工具，支持从中关村在线获取手机产品信息，并基于这些关键词在多个社交媒体平台（小红书、微博、豆瓣、知乎、B站）进行数据采集，最终生成汇总Excel文件。

功能特性

Step 1: 中关村在线产品信息爬取
- 爬取中关村在线手机产品信息（型号、特点、价格、评分、链接）
- 支持多页爬取
- 自动导出为格式化Excel文件

Step 2: 多平台社交媒体数据采集
支持以下平台的并行爬取：
1. 小红书 (XHS)
   - 搜索笔记内容
   - 采集标题、点赞数、链接等信息
   - 支持有头/无头模式
2. 微博 (Weibo)
   - 搜索微博内容
   - 采集文本、转发数、评论数、点赞数、链接等信息
   - 支持有头/无头模式
3. 豆瓣 (Douban)
   - 搜索豆瓣小组帖子
   - 采集标题、作者、时间、内容、回复数、浏览量等信息
   - 支持有头/无头模式
4. 知乎 (Zhihu)
   - 搜索知乎回答
   - 采集问题、回答、作者、点赞数、评论数等信息
   - 支持有头/无头模式
5. B站 (Bilibili)
   - 搜索视频内容
   - 采集视频列表、详情、评论、弹幕等信息
   - 每个关键词生成独立Excel，最终生成整合Excel
Step 3: 数据汇总
- 自动合并所有平台的整合Excel文件
- 每个平台一个sheet，便于查看和对比
- 生成统一的汇总Excel文件

环境要求
- Python 3.7+
- Chrome浏览器
- 相关Python依赖包

安装依赖

```bash
pip install requests beautifulsoup4 pandas openpyxl selenium webdriver-manager playwright python-dotenv rich
```

基本使用

方式一：运行完整工作流（推荐）
单行代码：python -c "from 自动化 import run_automated_workflow; run_automated_workflow()"

运行完整流程（可选）：Step1 -> Step2 -> Step3
run_automated_workflow(
    zol_start_page=1,        # 中关村起始页码
    zol_end_page=3,          # 中关村结束页码
    skip_step1=False,        # 是否跳过Step1
    run_xhs=True,            # 是否运行小红书爬虫
    run_weibo=True,          # 是否运行微博爬虫
    run_douban=True,         # 是否运行豆瓣爬虫
    run_zhihu=True,          # 是否运行知乎爬虫
    run_bilibili=True,       # 是否运行B站爬虫
)
```

方式二：只运行Step1和B站爬虫（有头模式）

```bash
python -c "from 自动化 import run_automated_workflow; run_automated_workflow(skip_step1=False, run_bilibili=True, run_xhs=False, run_weibo=False, run_douban=False, run_zhihu=False)"
```

方式三：跳过Step1，使用现有Excel文件

```python
run_automated_workflow(
    excel_file_path="中关村在线产品信息_20231209_120000.xlsx",
    skip_step1=True,
    run_xhs=True,
    run_weibo=True,
    # ... 其他参数
)
```

输出文件结构

```
项目根目录/
├── out/                          # 输出目录
│   ├── xhs/                      # 小红书输出
│   │   ├── xhs_整合_YYYYMMDD.xlsx
│   │   └── xhs_关键词.xlsx
│   ├── weibo/                    # 微博输出
│   │   ├── 微博搜索结果_整合_YYYYMMDD.xlsx
│   │   └── weibo_关键词.xlsx
│   ├── douban/                   # 豆瓣输出
│   │   ├── 豆瓣搜索结果_整合_YYYYMMDD.xlsx
│   │   └── douban_关键词.xlsx
│   ├── zhihu/                    # 知乎输出
│   │   ├── zhihu_all_combined.xlsx
│   │   └── zhihu_关键词.xlsx
│   ├── bilibili/                 # B站输出
│   │   ├── bilibili_all.xlsx     # 整合Excel（多表）
│   │   ├── bilibili_关键词.xlsx  # 单关键词Excel
│   │   └── *.csv                 # 实时写入的CSV文件
│   └── 所有平台汇总_YYYYMMDD_HHMMSS.xlsx  # Step3生成的汇总文件
├── 中关村在线产品信息_YYYYMMDD_HHMMSS.xlsx  # Step1输出
└── 自动化.py                     # 主程序文件
```
详细配置

爬虫参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `zol_start_page` | 中关村爬虫起始页码 | 1 |
| `zol_end_page` | 中关村爬虫结束页码 | 3 |
| `xhs_limit` | 小红书每个关键词采集数量上限 | 200 |
| `xhs_headless` | 小红书是否使用无头模式 | True |
| `weibo_limit` | 微博每个关键词采集数量上限 | 200 |
| `weibo_headless` | 微博是否使用无头模式 | False |
| `douban_headless` | 豆瓣是否使用无头模式 | False |
| `zhihu_headless` | 知乎是否使用无头模式 | False |
| `run_xhs` | 是否运行小红书爬虫 | True |
| `run_weibo` | 是否运行微博爬虫 | True |
| `run_douban` | 是否运行豆瓣爬虫 | True |
| `run_zhihu` | 是否运行知乎爬虫 | True |
| `run_bilibili` | 是否运行B站爬虫 | True |

登录说明

部分平台需要登录才能正常爬取：

- **微博**: 首次运行会弹出浏览器窗口，需要扫码登录
- **豆瓣**: 首次运行会弹出浏览器窗口，需要扫码登录
- **知乎**: 首次运行会弹出浏览器窗口，需要扫码登录
- **B站**: 首次运行会弹出浏览器窗口，需要扫码登录
- **小红书**: 使用Playwright，需要先运行setup模式登录

登录状态会自动保存，后续运行可直接使用。

数据格式说明

中关村在线 (Step1)
-列: model, features, price, score, link

小红书
-列: Keyword, Title, Like Count, Post URL

微博
-列: Key Word, Text Content, Repost Count, Comment Count, Like Count, Post Link

豆瓣
-列: Key Word, Title, Author, Time, Text Content, Reply Count, View Count, Post Link

知乎
-列: 关键词, 回答ID, 问题ID, 问题标题, 回答链接, 回答正文, 回答创建时间, 回答更新时间, 点赞数, 评论数, 作者ID, 作者昵称, 作者性别, 作者头像, 作者主页, 作者粉丝数, 评论, 原始JSON, 抓取时间

B站
-多表结构:
  - `search_list`: 搜索列表（视频基本信息）
  - `video_detail`: 视频详情（统计数据）
  - `comment`: 评论数据
  - `danmu`: 弹幕数据



