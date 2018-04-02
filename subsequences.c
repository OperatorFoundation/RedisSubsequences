/* Helloworld module -- A few examples of the Redis Modules API in the form
 * of commands showing how to accomplish common tasks.
 *
 * This module does not do anything useful, if not for a few commands. The
 * examples are designed in order to show the API.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2016, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "sdk/redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

/* SUBSEQUENCES.ADD sortedsetname sequence
   Increments scores with keys corresponding to subsequenes of sequence by 1
   Returns the number of subsequences found
   subsequences.add testkey testvalue
*/
int SequenceAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
  size_t seqlen;
  const char *seq = RedisModule_StringPtrLen(argv[2], &seqlen);

  size_t count = 0;
  for(size_t sublen=1; sublen<=seqlen; sublen++) {
    for(int index=0; index+sublen<=seqlen; index++) {
      const char *subptr=seq+index;
      RedisModuleString *sub=RedisModule_CreateString(ctx, subptr, sublen);
      RedisModule_ZsetIncrby(key,1.0,sub,NULL,NULL);
      count++;
    }
  }

  RedisModule_CloseKey(key);
  RedisModule_ReplyWithLongLong(ctx,count);
  return REDISMODULE_OK;
}

/* SUBSEQUENCES.ADDOFFSET sortedsetname sequence offset
   Increments scores with keys corresponding to subsequenes of sequence at given offset by 1
   Returns the number of subsequences found
   subsequences.add testkey testvalue 100
*/
int SequenceAddOffset_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
  size_t seqlen;
  const char *seq = RedisModule_StringPtrLen(argv[2], &seqlen);

  long long index;
  if ((RedisModule_StringToLongLong(argv[3],&index) != REDISMODULE_OK) || (index < 0) || (index >= seqlen)) {
      RedisModule_CloseKey(key);
      return RedisModule_ReplyWithError(ctx,"ERR invalid offset");
  }

  size_t count = 0;
  for(size_t sublen=1; sublen<=seqlen-index; sublen++) {
    const char *subptr=seq+index;
    RedisModuleString *sub=RedisModule_CreateString(ctx, subptr, sublen);
    RedisModule_ZsetIncrby(key,1.0,sub,NULL,NULL);
    count++;
  }

  RedisModule_CloseKey(key);
  RedisModule_ReplyWithLongLong(ctx,count);
  return REDISMODULE_OK;
}

/* HELLO.LEFTPAD str len ch
 * This is an implementation of the infamous LEFTPAD function, that
 * was at the center of an issue with the npm modules system in March 2016.
 *
 * LEFTPAD is a good example of using a Redis Modules API called
 * "pool allocator", that was a famous way to allocate memory in yet another
 * open source project, the Apache web server.
 *
 * The concept is very simple: there is memory that is useful to allocate
 * only in the context of serving a request, and must be freed anyway when
 * the callback implementing the command returns. So in that case the module
 * does not need to retain a reference to these allocations, it is just
 * required to free the memory before returning. When this is the case the
 * module can call RedisModule_PoolAlloc() instead, that works like malloc()
 * but will automatically free the memory when the module callback returns.
 *
 * Note that PoolAlloc() does not necessarily require AutoMemory to be
 * active. */
int HelloLeftPad_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    long long padlen;

    if (argc != 4) return RedisModule_WrongArity(ctx);

    if ((RedisModule_StringToLongLong(argv[2],&padlen) != REDISMODULE_OK) ||
        (padlen< 0)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid padding length");
    }
    size_t strlen, chlen;
    const char *str = RedisModule_StringPtrLen(argv[1], &strlen);
    const char *ch = RedisModule_StringPtrLen(argv[3], &chlen);

    /* If the string is already larger than the target len, just return
     * the string itself. */
    if (strlen >= (size_t)padlen)
        return RedisModule_ReplyWithString(ctx,argv[1]);

    /* Padding must be a single character in this simple implementation. */
    if (chlen != 1)
        return RedisModule_ReplyWithError(ctx,
            "ERR padding must be a single char");

    /* Here we use our pool allocator, for our throw-away allocation. */
    padlen -= strlen;
    char *buf = RedisModule_PoolAlloc(ctx,padlen+strlen);
    for (long long j = 0; j < padlen; j++) buf[j] = *ch;
    memcpy(buf+padlen,str,strlen);

    RedisModule_ReplyWithStringBuffer(ctx,buf,padlen+strlen);
    return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"subsequences",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    /* Log the list of parameters passing loading the module. */
    for (int j = 0; j < argc; j++) {
        const char *s = RedisModule_StringPtrLen(argv[j],NULL);
        printf("Module loaded with ARGV[%d] = %s\n", j, s);
    }

    if (RedisModule_CreateCommand(ctx,"subsequences.add",
        SequenceAdd_RedisCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"subsequences.addOffset",
        SequenceAddOffset_RedisCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
