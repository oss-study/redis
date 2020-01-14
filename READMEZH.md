##### 参考资料：[redis-3.0-annotated](https://github.com/huangz1990/redis-3.0-annotated)、网络上的相关文章等。

## 文件结构

#### 数据结构

[sds.h](./src/sds.h)：简单动态字符串

[adlist.h](./src/adlist.h)：双向无环链表

[ziplist.c](./src/ziplist.c)：压缩列表

[dict.h](./src/dict.h)：字典

[server.h](./src/server.h)：跳跃表

[intset.h](./src/intset.h)：跳跃表