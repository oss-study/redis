##### 参考资料：[redis-3.0-annotated](https://github.com/huangz1990/redis-3.0-annotated)、网络上的相关文章等。

## 文件结构

#### 数据结构

[sds.c](./src/sds.c)：简单动态字符串

[adlist.c](./src/adlist.c)：双向无环链表

[ziplist.c](./src/ziplist.c)：压缩列表

[quicklist.c](./src/quicklist.c)：快速链表

[server.h](./src/server.h)：跳跃表

[dict.c](./src/dict.c)：字典

[zipmap.c](./src/zipmap.c)：压缩哈希表

[intset.c](./src/intset.c)：整数集合

[rax.c](./src/rax.c)：radix tree