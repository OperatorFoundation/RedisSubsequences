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
int SequenceAddFloating_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  
  if (argc != 3) return RedisModule_WrongArity(ctx);

  RedisModuleString *tempName=RedisModule_CreateString(ctx, (char *)"SequenceAdd:Temp", 16);
  RedisModuleKey *temp = RedisModule_OpenKey(ctx, tempName, REDISMODULE_READ|REDISMODULE_WRITE);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
  size_t seqlen;
  const char *seq = RedisModule_StringPtrLen(argv[2], &seqlen);

  size_t count = 0;
  for(size_t sublen=1; sublen<=seqlen; sublen++) {
    for(int index=0; index+sublen<=seqlen; index++) {
      const char *subptr=seq+index;
      RedisModuleString *sub=RedisModule_CreateString(ctx, subptr, sublen);

      int flags=REDISMODULE_ZADD_NX;
      RedisModule_ZsetAdd(temp, 1.0, sub, &flags);
      if(flags & REDISMODULE_ZADD_ADDED) {
        RedisModule_ZsetIncrby(key, 1.0, sub, NULL, NULL);
        count++;
      }
    }
  }

  RedisModule_DeleteKey(temp);

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
  RedisModule_AutoMemory(ctx);

  if (argc != 4) return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
  size_t seqlen;
  const char *seq = RedisModule_StringPtrLen(argv[2], &seqlen);

  long long index;
  if ((RedisModule_StringToLongLong(argv[3],&index) != REDISMODULE_OK) || (index < 0) || (index >= (long long)seqlen)) {
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

/* SUBSEQUENCES.ADD sortedsetname offsetprefix sequence
   Increments scores with keys corresponding to subsequenes of sequence by 1
   Returns the number of subsequences found
   subsequences.add testkey testvalue
*/
int SequenceAddAll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }

  const char *offsetPrefix=RedisModule_StringPtrLen(argv[2], NULL);

  RedisModuleString *tempName=RedisModule_CreateString(ctx, (char *)"SequenceAdd:Temp", 16);
  RedisModuleKey *temp = RedisModule_OpenKey(ctx, tempName, REDISMODULE_READ|REDISMODULE_WRITE);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
  size_t seqlen;
  const char *seq = RedisModule_StringPtrLen(argv[3], &seqlen);

  size_t count = 0;
  for(size_t sublen=1; sublen<=seqlen; sublen++) {
    for(int index=0; index+sublen<=seqlen; index++) {
      const char *subptr=seq+index;
      RedisModuleString *sub=RedisModule_CreateString(ctx, subptr, sublen);

      int flags=REDISMODULE_ZADD_NX;
      RedisModule_ZsetAdd(temp, 1.0, sub, &flags);
      if(flags & REDISMODULE_ZADD_ADDED) {
        RedisModule_ZsetIncrby(key, 1.0, sub, NULL, NULL);
        count++;
      }

      RedisModuleString *offsetName=RedisModule_CreateStringPrintf(ctx, "%s:%d", offsetPrefix, index);
      RedisModuleKey *offsetKey = RedisModule_OpenKey(ctx, offsetName, REDISMODULE_READ|REDISMODULE_WRITE);
      RedisModule_ZsetIncrby(offsetKey,1.0,sub,NULL,NULL);
    }
  }

  RedisModule_DeleteKey(temp);

  RedisModule_CloseKey(key);
  RedisModule_ReplyWithLongLong(ctx,count);
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

    if (RedisModule_CreateCommand(ctx,"subsequences.addFloating", SequenceAddFloating_RedisCommand,"write",1,1,1) == REDISMODULE_ERR)
    {
      return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,"subsequences.addOffset", SequenceAddOffset_RedisCommand,"write",1,1,1) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,"subsequences.add",SequenceAddAll_RedisCommand,"write",1,1,1) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
