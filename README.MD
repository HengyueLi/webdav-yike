一刻相册 WebDav Server

百度出品的[一刻相册](https://photo.baidu.com/)可以无限量的存储照片。借助于百度强大的AI后台，能够实现一些智能分类操作。如果有一个WebDav服务，相信能做更多有趣的事情。

# 安装lib
`pip install -r requirements.txt`


# 快速启动
### Step1: 下载浏览器中的cookies
找一台有UI的机器，通过浏览器登陆一刻相册后，通过如下指令生成cookies文件
```
python webdav-yike.py -c chrome
```
这里是用chrome浏览器做的例子，如果用其他的浏览器则将`chrome`替换。有`firefox`等各种。详细信息可以参考[browser_cookie3](https://github.com/borisbabic/browser_cookie3#contribute)。
之后可以看到自动生成了一个文件`cj.json`。该文件可以被移动去别的机器上用。可以根据`-o`指定文件名。

### Step2: 启动
```
python webdav-yike.py cj.json
```

默认启动到5000端口，可以通过浏览器访问`http://localhost:5000/`测试一下

# 功能
目前处于非常初级的阶段。但是能够将百度智能分类的相册也同步下来。因为数量可能是非常大的，默认文件夹中只读取前999个文件。根据自己的网络可以做一些调整，详细参考下面的`API参数`配置。

# 说明
- 该项目主要依赖[wsgidav](https://github.com/mar10/wsgidav)和[pybaiduphoto](https://github.com/HengyueLi/baiduphoto)。欢迎共同学习。
- 文件目录会缓存在一个NoSQL中。目前用的是一个内存中的假数据库(字典)，后期可以快速方便地接到真的NoSQL上（有需要再改）。如果有多个客户端同时工作，比如同一个账号还有人通过手机上传照片，可能（并不是一定）导致目录不一致。如果出现这种情况清空缓存就可以了。当前的简单版本就是把WebDav服务重启一下。


# API参数
目前有如下参数可以调整
```
"ALBUM_DELETE_WITHITEM": False,
"ALBUM_ITEM_DELETE_WITH_ORIGIN": False,
"ABSALUM_MAX_IN_DIR": 0,
"ITEM_NUM_MAX_IN_DIR": 999,
"ITEM_NUM_MAX_IN_ALBUM": 999,
"ITEM_NUM_MAX_IN_PERSON": 999,
"ITEM_NUM_MAX_IN_LOCATION": 999,
"ITEM_NUM_MAX_IN_THING": 999,
"DELIMITER": "@",
```
修改参数的方法是（例如）
```
python webdav-yike.py cj.json -O ALBUM_DELETE_WITHITEM=True ITEM_NUM_MAX_IN_DIR=2000
```

# 其他功能
什么网络代理啊，账号密码等等都行，详细看` webdav-yike.py -h`吧。还有很多想法，以后有空慢慢一遍学习一边做吧。

# 免责申明
此脚本（API）仅供学习交流，禁止商业使用。使用软件过程中，发生意外造成的损失由使用者承担。您必须在下载后的24小时内从计算机或其他各种设备中完全删除本项目所有内容。您使用或者复制了以上的任何内容，则视为已接受此声明，请仔细阅读。
