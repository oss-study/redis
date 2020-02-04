/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"

/*-----------------------------------------------------------------------------
 * List API
 *----------------------------------------------------------------------------*/

/* The function pushes an element to the specified list object 'subject',
 * at head or tail position as specified by 'where'.
 *
 * 将给定元素添加到列表的表头或表尾。
 *
 * 参数 where 决定了新元素添加的位置：
 *
 *  - REDIS_HEAD 将新元素添加到表头
 *
 *  - REDIS_TAIL 将新元素添加到表尾
 * 
 * 调用者无须担心 value 的引用计数，因为这个函数会负责这方面的工作。
 * 
 * There is no need for the caller to increment the refcount of 'value' as
 * the function takes care of it if needed. */
void listTypePush(robj *subject, robj *value, int where) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        int pos = (where == LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
        value = getDecodedObject(value);
        size_t len = sdslen(value->ptr);
        quicklistPush(subject->ptr, value->ptr, len, pos);
        decrRefCount(value);
    } else {
        // 现在只使用 OBJ_ENCODING_QUICKLIST 编码
        serverPanic("Unknown list encoding");
    }
}

// pop 操作保护程序  
void *listPopSaver(unsigned char *data, unsigned int sz) {
    return createStringObject((char*)data,sz);
}

// 从列表的表头或表尾中弹出一个元素。
robj *listTypePop(robj *subject, int where) {
    long long vlong;
    robj *value = NULL;

    int ql_where = where == LIST_HEAD ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        if (quicklistPopCustom(subject->ptr, ql_where, (unsigned char **)&value,
                               NULL, &vlong, listPopSaver)) {
            if (!value)
                value = createStringObjectFromLongLong(vlong);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

// 返回列表的节点数量
unsigned long listTypeLength(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistCount(subject->ptr);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Initialize an iterator at the specified index. */
/* 创建并返回一个列表迭代器。
 *
 * 参数 index 决定开始迭代的列表索引。
 *
 * 参数 direction 则决定了迭代的方向。
 *
 * listTypeIterator 于 server.h 文件中定义。
 */
listTypeIterator *listTypeInitIterator(robj *subject, long index,
                                       unsigned char direction) {
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    li->iter = NULL;
    /* LIST_HEAD means start at TAIL and move *towards* head.
     * LIST_TAIL means start at HEAD and move *towards tail. */
    int iter_direction =
        direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        li->iter = quicklistGetIteratorAtIdx(li->subject->ptr,
                                             iter_direction, index);
    } else {
        serverPanic("Unknown list encoding");
    }
    return li;
}

/* Clean up the iterator. */
// 释放迭代器
void listTypeReleaseIterator(listTypeIterator *li) {
    zfree(li->iter);
    zfree(li);
}

/* Stores pointer to current the entry in the provided entry structure
 * and advances the position of the iterator. Returns 1 when the current
 * entry is in fact an entry, 0 otherwise. */
// 使用 listTypeEntry 结构记录迭代器当前指向的节点，并将迭代器的指针移动到下一个元素。
// 如果列表中还有元素可迭代，那么返回 1 ，否则，返回 0 。
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* Protect from converting when iterating */
    serverAssert(li->subject->encoding == li->encoding);

    entry->li = li;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistNext(li->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
    return 0;
}

/* Return entry or NULL at the current position of the iterator. */
// 返回 listTypeEntry 结构当前所保存的列表节点，如果没有记录任何节点，那么返回 NULL。
robj *listTypeGet(listTypeEntry *entry) {
    robj *value = NULL;
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        if (entry->entry.value) {
            value = createStringObject((char *)entry->entry.value,
                                       entry->entry.sz);
        } else {
            value = createStringObjectFromLongLong(entry->entry.longval);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

// 将对象 value 插入到列表节点的之前或之后。
void listTypeInsert(listTypeEntry *entry, robj *value, int where) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        // 对 value 进行编码
        value = getDecodedObject(value);
        sds str = value->ptr;
        size_t len = sdslen(str);
        if (where == LIST_TAIL) {
            quicklistInsertAfter((quicklist *)entry->entry.quicklist,
                                 &entry->entry, str, len);
        } else if (where == LIST_HEAD) {
            quicklistInsertBefore((quicklist *)entry->entry.quicklist,
                                  &entry->entry, str, len);
        }
        decrRefCount(value);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Compare the given object with the entry at the current position. */
// 将当前节点的值和对象 o 进行对比，相等时返回 1，不相等时返回 0。
int listTypeEqual(listTypeEntry *entry, robj *o) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        serverAssertWithInfo(NULL,o,sdsEncodedObject(o));
        return quicklistCompare(entry->entry.zi,o->ptr,sdslen(o->ptr));
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Delete the element pointed to. */
// 删除 entry 所指向的节点
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelEntry(iter->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Create a quicklist from a single ziplist */
// 将一个 ziplist 编码的对象转换成 quicklist
// 在 t_list 中并未使用该转换函数
void listTypeConvert(robj *subject, int enc) {
    serverAssertWithInfo(NULL,subject,subject->type==OBJ_LIST);
    serverAssertWithInfo(NULL,subject,subject->encoding==OBJ_ENCODING_ZIPLIST);

    if (enc == OBJ_ENCODING_QUICKLIST) {
        size_t zlen = server.list_max_ziplist_size;
        int depth = server.list_compress_depth;
        subject->ptr = quicklistCreateFromZiplist(zlen, depth, subject->ptr);
        subject->encoding = OBJ_ENCODING_QUICKLIST;
    } else {
        serverPanic("Unsupported list conversion");
    }
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

void pushGenericCommand(client *c, int where) {
    int j, pushed = 0;
    // 取出列表对象
    robj *lobj = lookupKeyWrite(c->db,c->argv[1]);

    // 类型错误
    if (lobj && lobj->type != OBJ_LIST) {
        addReply(c,shared.wrongtypeerr);
        return;
    }

    // 遍历所有输入值，并将它们添加到列表中
    for (j = 2; j < c->argc; j++) {
        if (!lobj) {
            // 如果列表对象不存在，那么创建一个，并关联到数据库
            lobj = createQuicklistObject();
            quicklistSetOptions(lobj->ptr, server.list_max_ziplist_size,
                                server.list_compress_depth);
            dbAdd(c->db,c->argv[1],lobj);
        }
        // 将值推入到列表
        listTypePush(lobj,c->argv[j],where);
        pushed++;
    }
    // 返回添加的节点数量
    addReplyLongLong(c, (lobj ? listTypeLength(lobj) : 0));
    // 如果至少有一个元素被成功推入，那么执行以下代码
    if (pushed) {
        char *event = (where == LIST_HEAD) ? "lpush" : "rpush";

        // 发送键修改信号
        signalModifiedKey(c->db,c->argv[1]);
        // 发送事件通知
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
    }
    server.dirty += pushed;
}

// LPUSH 命令，将所有指定的值插入到列表的头部
void lpushCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD);
}

// RPUSH 命令，将所有指定的值插入到列表的尾部
void rpushCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL);
}

void pushxGenericCommand(client *c, int where) {
    int j, pushed = 0;
    robj *subject;

    // 取出列表对象
    if ((subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,subject,OBJ_LIST)) return;

    // 遍历所有输入值，并将它们添加到列表中
    for (j = 2; j < c->argc; j++) {
        // 将值推入到列表
        listTypePush(subject,c->argv[j],where);
        pushed++;
    }

    // 返回添加的节点数量
    addReplyLongLong(c,listTypeLength(subject));

    // 如果至少有一个元素被成功推入，那么执行以下代码
    if (pushed) {
        char *event = (where == LIST_HEAD) ? "lpush" : "rpush";
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
    }
    server.dirty += pushed;
}

// LPUSHX 命令，当 key 不存在的时候不会执行 PUSH 操作
void lpushxCommand(client *c) {
    pushxGenericCommand(c,LIST_HEAD);
}

// RPUSHX 命令，当 key 不存在的时候不会执行 PUSH 操作
void rpushxCommand(client *c) {
    pushxGenericCommand(c,LIST_TAIL);
}

// LINSERT 命令，将新值插入在指定值之前或之后
void linsertCommand(client *c) {
    int where;
    robj *subject;
    // 创建迭代器
    listTypeIterator *iter;
    listTypeEntry entry;
    int inserted = 0;

    if (strcasecmp(c->argv[2]->ptr,"after") == 0) {
        where = LIST_TAIL;
    } else if (strcasecmp(c->argv[2]->ptr,"before") == 0) {
        where = LIST_HEAD;
    } else {
        addReply(c,shared.syntaxerr);
        return;
    }

    if ((subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,subject,OBJ_LIST)) return;

    /* Seek pivot from head to tail */
    // 查找指定值
    iter = listTypeInitIterator(subject,0,LIST_TAIL);
    while (listTypeNext(iter,&entry)) {
        if (listTypeEqual(&entry,c->argv[3])) {
            listTypeInsert(&entry,c->argv[4],where);
            inserted = 1;
            break;
        }
    }
    listTypeReleaseIterator(iter);

    if (inserted) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"linsert",
                            c->argv[1],c->db->id);
        server.dirty++;
    } else {
        /* Notify client of a failed insert */
        addReplyLongLong(c,-1);
        return;
    }

    addReplyLongLong(c,listTypeLength(subject));
}

// LLEN 命令，返回 list 的长度
void llenCommand(client *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    addReplyLongLong(c,listTypeLength(o));
}

// LINDEX 命令，根据索引返回值
void lindexCommand(client *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;
    robj *value = NULL;

    // 取出整数值对象 index
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    // 根据索引，遍历 quicklist ，直到指定位置
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistEntry entry;
        if (quicklistIndex(o->ptr, index, &entry)) {
            if (entry.value) {
                value = createStringObject((char*)entry.value,entry.sz);
            } else {
                value = createStringObjectFromLongLong(entry.longval);
            }
            addReplyBulk(c,value);
            decrRefCount(value);
        } else {
            addReplyNull(c);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

// LSET 命令，设置索引位置处的 list 元素值
void lsetCommand(client *c) {
    // 取出列表对象
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;
    // 取出值对象 value
    robj *value = c->argv[3];

    // 取出 index
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = o->ptr;
        // 替换值
        int replaced = quicklistReplaceAtIndex(ql, index,
                                               value->ptr, sdslen(value->ptr));
        // 索引值超出 list 长度
        if (!replaced) {
            addReply(c,shared.outofrangeerr);
        } else {
            addReply(c,shared.ok);
            signalModifiedKey(c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_LIST,"lset",c->argv[1],c->db->id);
            server.dirty++;
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

void popGenericCommand(client *c, int where) {
    // 取出列表对象
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;

    // 弹出列表元素
    robj *value = listTypePop(o,where);
    // 根据弹出元素是否为空，决定后续动作
    if (value == NULL) {
        addReplyNull(c);
    } else {
        char *event = (where == LIST_HEAD) ? "lpop" : "rpop";

        addReplyBulk(c,value);
        decrRefCount(value);
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
        if (listTypeLength(o) == 0) {
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                c->argv[1],c->db->id);
            dbDelete(c->db,c->argv[1]);
        }
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }
}

// LPOP 命令，移除并且返回 list 的头部元素
void lpopCommand(client *c) {
    popGenericCommand(c,LIST_HEAD);
}

// RPOP 命令，移除并且返回 list 的尾部元素
void rpopCommand(client *c) {
    popGenericCommand(c,LIST_TAIL);
}

// LRANGE 命令，返回指定范围内的元素
void lrangeCommand(client *c) {
    robj *o;
    long start, end, llen, rangelen;

    // 取出索引值 start 和 end
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    // 取出列表对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray)) == NULL
         || checkType(c,o,OBJ_LIST)) return;
    // 取出列表长度
    llen = listTypeLength(o);

    /* convert negative indexes */
    // 将负数索引转换成正数索引
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c,shared.emptyarray);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyArrayLen(c,rangelen);
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        listTypeIterator *iter = listTypeInitIterator(o, start, LIST_TAIL);

        // 遍历 quicklist ，并将指定索引上的值添加到回复中
        while(rangelen--) {
            listTypeEntry entry;
            listTypeNext(iter, &entry);
            quicklistEntry *qe = &entry.entry;
            if (qe->value) {
                addReplyBulkCBuffer(c,qe->value,qe->sz);
            } else {
                addReplyBulkLongLong(c,qe->longval);
            }
        }
        listTypeReleaseIterator(iter);
    } else {
        serverPanic("List encoding is not QUICKLIST!");
    }
}

// LTRIM 命令，修剪一个 list，只保留指定范围内的元素，闭区间
void ltrimCommand(client *c) {
    robj *o;
    long start, end, llen, ltrim, rtrim;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok)) == NULL ||
        checkType(c,o,OBJ_LIST)) return;
    llen = listTypeLength(o);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    // 删除列表两端的元素
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        // 删除左端元素
        quicklistDelRange(o->ptr,0,ltrim);
        // 删除右端元素
        quicklistDelRange(o->ptr,-rtrim,rtrim);
    } else {
        serverPanic("Unknown list encoding");
    }

    notifyKeyspaceEvent(NOTIFY_LIST,"ltrim",c->argv[1],c->db->id);
    if (listTypeLength(o) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
    addReply(c,shared.ok);
}

// LREM 命令，从头部或尾部移除前 count 次出现的值为 value 的元素
void lremCommand(client *c) {
    robj *subject, *obj;
    obj = c->argv[3];
    long toremove;
    long removed = 0;

    // 取出指定删除模式的 count 参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &toremove, NULL) != C_OK))
        return;

    // 取出列表对象
    subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero);
    if (subject == NULL || checkType(c,subject,OBJ_LIST)) return;

    listTypeIterator *li;
    // 根据 toremove 参数，决定是从表头还是表尾开始进行删除
    if (toremove < 0) {
        toremove = -toremove;
        li = listTypeInitIterator(subject,-1,LIST_HEAD);
    } else {
        li = listTypeInitIterator(subject,0,LIST_TAIL);
    }

    listTypeEntry entry;
    // 查找比较对象，并进行删除
    while (listTypeNext(li,&entry)) {
        if (listTypeEqual(&entry,obj)) {
            listTypeDelete(li, &entry);
            server.dirty++;
            removed++;
            // 已经满足删除数量，停止
            if (toremove && removed == toremove) break;
        }
    }
    listTypeReleaseIterator(li);

    if (removed) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"lrem",c->argv[1],c->db->id);
    }

    // 删除空 list
    if (listTypeLength(subject) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    addReplyLongLong(c,removed);
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */

void rpoplpushHandlePush(client *c, robj *dstkey, robj *dstobj, robj *value) {
    /* Create the list if the key does not exist */
    // 如果目标列表不存在，那么创建一个
    if (!dstobj) {
        dstobj = createQuicklistObject();
        quicklistSetOptions(dstobj->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);
        dbAdd(c->db,dstkey,dstobj);
    }
    signalModifiedKey(c->db,dstkey);
    // 将值推入目标列表中
    listTypePush(dstobj,value,LIST_HEAD);
    notifyKeyspaceEvent(NOTIFY_LIST,"lpush",dstkey,c->db->id);
    /* Always send the pushed value to the client. */
    addReplyBulk(c,value);
}

// RPOPLPUSH 命令，原子性地将源列表尾部元素转移至目标列表头部
void rpoplpushCommand(client *c) {
    robj *sobj, *value;
    // 来源列表
    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,sobj,OBJ_LIST)) return;

    // 空列表，没有元素可 pop ，直接返回
    if (listTypeLength(sobj) == 0) {
        /* This may only happen after loading very old RDB files. Recent
         * versions of Redis delete keys of empty lists. */
        addReplyNull(c);
    // 源列表非空
    } else {
        // 目标对象
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        robj *touchedkey = c->argv[1];

        // 检查目标对象是否列表
        if (dobj && checkType(c,dobj,OBJ_LIST)) return;
        // 从源列表中弹出值
        value = listTypePop(sobj,LIST_TAIL);
        /* We saved touched key, and protect it, since rpoplpushHandlePush
         * may change the client command argument vector (it does not
         * currently). */
        incrRefCount(touchedkey);
        rpoplpushHandlePush(c,c->argv[2],dobj,value);

        /* listTypePop returns an object with its refcount incremented */
        decrRefCount(value);

        /* Delete the source list when it is empty */
        notifyKeyspaceEvent(NOTIFY_LIST,"rpop",touchedkey,c->db->id);
        // 如果源列表已经为空，那么将它删除
        if (listTypeLength(sobj) == 0) {
            dbDelete(c->db,touchedkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                touchedkey,c->db->id);
        }
        signalModifiedKey(c->db,touchedkey);
        decrRefCount(touchedkey);
        server.dirty++;
        if (c->cmd->proc == brpoplpushCommand) {
            rewriteClientCommandVector(c,3,shared.rpoplpush,c->argv[1],c->argv[2]);
        }
    }
}

/*-----------------------------------------------------------------------------
 * Blocking POP operations
 *----------------------------------------------------------------------------*/

/* This is a helper function for handleClientsBlockedOnKeys(). It's work
 * is to serve a specific client (receiver) that is blocked on 'key'
 * in the context of the specified 'db', doing the following:
 * 
 * 函数对被阻塞的客户端 receiver 、造成阻塞的 key 、 key 所在的数据库 db
 * 以及一个值 value 和一个位置值 where 执行以下操作：
 *
 * 1) Provide the client with the 'value' element.
 * 
 *    将 value 提供给 receiver
 * 
 * 2) If the dstkey is not NULL (we are serving a BRPOPLPUSH) also push the
 *    'value' element on the destination list (the LPUSH side of the command).
 * 
 *    如果 dstkey 不为空（BRPOPLPUSH的情况），那么也将 value 推入到 dstkey 指定的列表中。
 * 
 * 3) Propagate the resulting BRPOP, BLPOP and additional LPUSH if any into
 *    the AOF and replication channel.
 * 
 *    将 BRPOP 、 BLPOP 和可能有的 LPUSH 传播到 AOF 和同步节点
 *
 * The argument 'where' is LIST_TAIL or LIST_HEAD, and indicates if the
 * 'value' element was popped from the head (BLPOP) or tail (BRPOP) so that
 * we can propagate the command properly.
 *
 * where 可能是 REDIS_TAIL 或者 REDIS_HEAD ，用于识别该 value 是从那个地方 POP
 * 出来，依靠这个参数，可以同样传播 BLPOP 或者 BRPOP 。
 * 
 * 如果一切成功，返回 REDIS_OK 。
 * 如果执行失败，那么返回 REDIS_ERR ，让 Redis 撤销对目标节点的 POP 操作。
 * 失败的情况只会出现在 BRPOPLPUSH 命令中，
 * 比如 POP 源列表成功，却发现 PUSH 的目标对象不是列表时，操作就会出现失败。
 * 
 * The function returns C_OK if we are able to serve the client, otherwise
 * C_ERR is returned to signal the caller that the list POP operation
 * should be undone as the client was not served: This only happens for
 * BRPOPLPUSH that fails to push the value to the destination key as it is
 * of the wrong type. */
int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int where)
{
    robj *argv[3];

    // 执行的是 BLPOP 或 BRPOP
    if (dstkey == NULL) {
        /* Propagate the [LR]POP operation. */
        argv[0] = (where == LIST_HEAD) ? shared.lpop :
                                          shared.rpop;
        argv[1] = key;
        propagate((where == LIST_HEAD) ?
            server.lpopCommand : server.rpopCommand,
            db->id,argv,2,PROPAGATE_AOF|PROPAGATE_REPL);

        /* BRPOP/BLPOP */
        addReplyArrayLen(receiver,2);
        addReplyBulk(receiver,key);
        addReplyBulk(receiver,value);

        /* Notify event. */
        char *event = (where == LIST_HEAD) ? "lpop" : "rpop";
        notifyKeyspaceEvent(NOTIFY_LIST,event,key,receiver->db->id);
    // 执行的是 BRPOPLPUSH 
    } else {
        /* BRPOPLPUSH */
        // 取出目标对象
        robj *dstobj =
            lookupKeyWrite(receiver->db,dstkey);
        if (!(dstobj &&
             checkType(receiver,dstobj,OBJ_LIST)))
        {
            // 将值推入到 dstobj 中，如果 dstobj 不存在，
            // 那么新创建一个
            rpoplpushHandlePush(receiver,dstkey,dstobj,
                value);
            /* Propagate the RPOPLPUSH operation. */
            // 传播 RPOPLPUSH 命令
            argv[0] = shared.rpoplpush;
            argv[1] = key;
            argv[2] = dstkey;
            propagate(server.rpoplpushCommand,
                db->id,argv,3,
                PROPAGATE_AOF|
                PROPAGATE_REPL);

            /* Notify event ("lpush" was notified by rpoplpushHandlePush). */
            notifyKeyspaceEvent(NOTIFY_LIST,"rpop",key,receiver->db->id);
        } else {
            /* BRPOPLPUSH failed because of wrong
             * destination type. */
            return C_ERR;
        }
    }
    return C_OK;
}

/* Blocking RPOP/LPOP */
void blockingPopGenericCommand(client *c, int where) {
    robj *o;
    mstime_t timeout;
    int j;

    // 取出 timeout 参数
    if (getTimeoutFromObjectOrReply(c,c->argv[c->argc-1],&timeout,UNIT_SECONDS)
        != C_OK) return;

    // 遍历所有列表键
    for (j = 1; j < c->argc-1; j++) {
        o = lookupKeyWrite(c->db,c->argv[j]);
        if (o != NULL) {
            if (o->type != OBJ_LIST) {
                addReply(c,shared.wrongtypeerr);
                return;
            } else {
                // 非空列表
                if (listTypeLength(o) != 0) {
                    /* Non empty list, this is like a non normal [LR]POP. */
                    char *event = (where == LIST_HEAD) ? "lpop" : "rpop";
                    // 弹出值
                    robj *value = listTypePop(o,where);
                    serverAssert(value != NULL);

                    addReplyArrayLen(c,2);
                    addReplyBulk(c,c->argv[j]);
                    addReplyBulk(c,value);
                    decrRefCount(value);
                    notifyKeyspaceEvent(NOTIFY_LIST,event,
                                        c->argv[j],c->db->id);
                    // 删除空列表
                    if (listTypeLength(o) == 0) {
                        dbDelete(c->db,c->argv[j]);
                        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                            c->argv[j],c->db->id);
                    }
                    signalModifiedKey(c->db,c->argv[j]);
                    server.dirty++;

                    /* Replicate it as an [LR]POP instead of B[LR]POP. */
                    // 传播一个 [LR]POP 而不是 B[LR]POP
                    rewriteClientCommandVector(c,2,
                        (where == LIST_HEAD) ? shared.lpop : shared.rpop,
                        c->argv[j]);
                    return;
                }
            }
        }
    }

    /* If we are inside a MULTI/EXEC and the list is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    // 如果命令在一个事务中执行，那么为了不产生死等待
    // 服务器只能向客户端发送一个空回复
    if (c->flags & CLIENT_MULTI) {
        addReplyNullArray(c);
        return;
    }

    /* If the list is empty or the key does not exists we must block */
    // 所有输入列表键都不存在，只能阻塞
    blockForKeys(c,BLOCKED_LIST,c->argv + 1,c->argc - 2,timeout,NULL,NULL);
}

// BLPOP 命令，是命令 LPOP 的阻塞版本，当给定列表内没有任何元素可供弹出的时候，连接将被阻塞
void blpopCommand(client *c) {
    blockingPopGenericCommand(c,LIST_HEAD);
}

// BRPOP 命令，是命令 RPOP 的阻塞版本
void brpopCommand(client *c) {
    blockingPopGenericCommand(c,LIST_TAIL);
}

// BRPOPLPUSH 命令，是命令 RPOPLPUSH 的阻塞版本
void brpoplpushCommand(client *c) {
    mstime_t timeout;

    // 取出 timeout 参数
    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout,UNIT_SECONDS)
        != C_OK) return;

    // 取出列表键
    robj *key = lookupKeyWrite(c->db, c->argv[1]);

    // 键为空，阻塞
    if (key == NULL) {
        if (c->flags & CLIENT_MULTI) {
            /* Blocking against an empty list in a multi state
             * returns immediately. */
            addReplyNull(c);
        } else {
            /* The list is empty and the client blocks. */
            blockForKeys(c,BLOCKED_LIST,c->argv + 1,1,timeout,c->argv[2],NULL);
        }
    // 键非空，执行 RPOPLPUSH
    } else {
        if (key->type != OBJ_LIST) {
            addReply(c, shared.wrongtypeerr);
        } else {
            /* The list exists and has elements, so
             * the regular rpoplpushCommand is executed. */
            serverAssertWithInfo(c,key,listTypeLength(key) > 0);
            rpoplpushCommand(c);
        }
    }
}
