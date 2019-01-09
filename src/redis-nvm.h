#ifndef __REDIS_NVM_H
#define __REDIS_NVM_H

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "sds.h"
#include "dict.h"
#include "server.h"

/* Redis NVM header file. */

struct nvm_dict {
  void*   hast_table_addr;  // mmap addr of hash table file.
  size_t  hast_table_size;
  void*   data_addr;        // mmap addr of data file.
  size_t  data_size;
  off_t   allocated_size;   // Allocate from here.
};

struct nvm_server {
  int     num_dicts;
  struct  nvm_dict *nvm_dicts;
};

/* Simple log-structured NVM allocator. */
static inline void* nvm_alloc_data_buf(struct nvm_dict *nvm_dict,
                                       size_t size) {
  void* p = (void *)((unsigned long)nvm_dict->data_addr +
                     nvm_dict->allocated_size);

  /* TODO: Expand data file. */
  if ((unsigned long)p + size > nvm_dict->data_size)
    return NULL;

  nvm_dict->allocated_size += size;
  return p;
}

/* Copy sds string to NVM */
static inline void* nvm_copy_sds(struct nvm_dict* nvm_dict, void* source) {
  /* For simplicity force to use sdshdr64 */
  struct sdshdr64* sh;
  size_t len = sdslen((sds)source);

  /* Allocate space: sdshdr, buf, \0 */
  sh = (struct sdshdr64*)nvm_alloc_data_buf(nvm_dict,
                                            sizeof(struct sdshdr64) + len + 1);
  if (!sh)
    serverPanic("Not enough space for sds length %llu", len);

  sh->len = len;
  sh->alloc = len;
  sh->flags = SDS_TYPE_64;
  memcpy(sh->buf, source, len);
  sh->buf[len] = '\0';
  return (void *)sh->buf;
}

/* Copy Robj to NVM */
static inline void* nvm_copy_robj(struct nvm_dict* nvm_dict, void* source) {
  robj* src = (robj*)source;
  robj* copy = (robj*)nvm_alloc_data_buf(nvm_dict, sizeof(robj));
  if (!copy)
    serverPanic("Failed to allocate robj metadata");

  copy->type = src->type;
  copy->encoding = src->encoding;
  copy->lru = src->lru;
  copy->refcount = src->refcount;
  copy->ptr = nvm_copy_sds(nvm_dict, src->ptr);
  return (void *)copy;
}

/* Free robj in NVM */
static inline void nvm_free_robj(struct nvm_dict* nvm_dict, void* robj) {
  /* FIXME: Not supported yet. Perhaps use GC to compact and recycle. */
  return;
}

#endif
