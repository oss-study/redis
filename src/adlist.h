/* adlist.h - A generic doubly linked list implementation
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __ADLIST_H__
#define __ADLIST_H__

/* Node, List, and Iterator are the only data structures used currently. */

// 双端链表节点
typedef struct listNode {
    struct listNode *prev; // 前置节点指针
    struct listNode *next; // 后置节点指针
    void *value;           // 该节点值指针
} listNode;

// 双端链表迭代器 
typedef struct listIter {
    listNode *next; // 当前迭代到的节点
    int direction; // 迭代方向
} listIter;

// 双端链表结构
typedef struct list {
    listNode *head; // 表头节点
    listNode *tail; // 表尾节点
    void *(*dup)(void *ptr); // 节点值复制函数
    void (*free)(void *ptr); // 节点释放函数
    int (*match)(void *ptr, void *key); // 节点比较函数
    unsigned long len; // 链表所包含的节点数量
} list;

/* Functions implemented as macros */
// 以下：T = O(1)
#define listLength(l) ((l)->len)
#define listFirst(l) ((l)->head)
#define listLast(l) ((l)->tail)
#define listPrevNode(n) ((n)->prev)
#define listNextNode(n) ((n)->next)
#define listNodeValue(n) ((n)->value)

// 将链表 l 的值复制函数设置为 m
#define listSetDupMethod(l,m) ((l)->dup = (m))
// 将链表 l 的值释放函数设置为 m
#define listSetFreeMethod(l,m) ((l)->free = (m))
// 将链表的对比函数设置为 m
#define listSetMatchMethod(l,m) ((l)->match = (m))

// 返回给定链表的值复制函数
#define listGetDupMethod(l) ((l)->dup)
// 返回给定链表的值释放函数
#define listGetFreeMethod(l) ((l)->free)
// 返回给定链表的值对比函数
#define listGetMatchMethod(l) ((l)->match)

/* Prototypes */
list *listCreate(void);
void listRelease(list *list);
void listEmpty(list *list);
list *listAddNodeHead(list *list, void *value);
list *listAddNodeTail(list *list, void *value);
list *listInsertNode(list *list, listNode *old_node, void *value, int after);
void listDelNode(list *list, listNode *node);
listIter *listGetIterator(list *list, int direction);
listNode *listNext(listIter *iter);
void listReleaseIterator(listIter *iter);
list *listDup(list *orig);
listNode *listSearchKey(list *list, void *key);
listNode *listIndex(list *list, long index);
void listRewind(list *list, listIter *li);
void listRewindTail(list *list, listIter *li);
void listRotate(list *list);
void listJoin(list *l, list *o);

/* Directions for iterators */
// 迭代器方向：从表头向表尾进行迭代
#define AL_START_HEAD 0
// 从表尾到表头进行迭代
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */
