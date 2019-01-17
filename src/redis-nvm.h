#ifndef __REDIS_NVM_H
#define __REDIS_NVM_H

#include "pmem.h"
#include "server.h"

/*
 * Redis NVM dictionary.
 * A NVM dictionary has two hashtables and one data file.
 */
struct nvm_dict {
  void*     hashtable0_addr;  // mmap addr of hash table file 0.
  size_t    hashtable0_size;
  int       hashtable0_fd;
  size_t    hashtable0_keys;
  void*     hashtable1_addr;  // mmap addr of hash table file 1.
  size_t    hashtable1_size;
  int       hashtable1_fd;
  size_t    hashtable1_keys;
  void*     data_addr;        // Current mmap addr of data file.
  void*     stored_data_addr; // Last mmap addr of data file.
  size_t    data_size;
  off_t     allocated_size;   // Data file allocated from here.
  int       data_fd;
};

struct nvm_server {
  int     num_dicts;
  struct  nvm_dict *nvm_dicts;
};


#define NVM_DICT_ENTRIES  ((uint64_t)16*1024*1024)
#define NVM_HASHTABLE_INIT_SIZE (NVM_DICT_ENTRIES * sizeof(dictEntry*))
#define NVM_DATA_INIT_SIZE (NVM_DICT_ENTRIES * 64)

/* Simple log-structured NVM allocator. */
static inline void* nvm_alloc_data_buf(struct nvm_dict *nvm_dict,
                                       size_t size) {
  void* p = (void *)((unsigned long)nvm_dict->data_addr +
                     nvm_dict->allocated_size);

  /* TODO: Expand data file. */
  if (nvm_dict->allocated_size + size > nvm_dict->data_size)
    return NULL;

  nvm_dict->allocated_size += size;
  return p;
}

static inline void* nvm_get_ht_addr(struct nvm_dict* nvm_dict,
                                    int hashtable_id,
                                    off_t offset) {
  serverAssert(hashtable_id == 0 || hashtable_id == 1);

  if (hashtable_id == 0) {
    serverAssert((unsigned long)offset < nvm_dict->hashtable0_size);
    return (void*)((unsigned long)nvm_dict->hashtable0_addr + offset);
  } else {
    serverAssert((unsigned long)offset < nvm_dict->hashtable1_size);
    return (void*)((unsigned long)nvm_dict->hashtable1_addr + offset);
  }
}

static inline void* nvm_get_data_addr(struct nvm_dict* nvm_dict,
                                      off_t offset) {
  serverAssert((unsigned long)offset < nvm_dict->data_size);

  return (void*)((unsigned long)nvm_dict->data_addr + offset);
}

/* Data file remmap changes the addr. Update accordingly. */
static inline void* nvm_update_data_addr(struct nvm_dict* nvm_dict,
                                         unsigned long value) {
  return (void *)(value - (unsigned long)(nvm_dict->stored_data_addr)
                  + (unsigned long)(nvm_dict->data_addr));
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
  pmem_memcpy_flush(sh->buf, source, len);
  sh->buf[len] = '\0';
  pmem_flush(sh, sizeof(struct sdshdr64));
  return (void *)sh->buf;
}

/* Copy Robj to NVM */
static inline void* nvm_copy_robj(struct nvm_dict* nvm_dict, void* source) {
  robj* src = (robj*)source;
  robj* copy = (robj*)nvm_alloc_data_buf(nvm_dict, sizeof(robj));
  if (!copy)
    serverPanic("Failed to allocate robj metadata");

//  serverLog(LL_WARNING, "copy %p, source encoding %u, ptr %p\n",
//            (void*)copy, src->encoding, src->ptr);
  copy->type = src->type;
  copy->encoding = src->encoding;
  copy->lru = src->lru;
  copy->refcount = src->refcount;
  copy->ptr = nvm_copy_sds(nvm_dict, src->ptr);
  pmem_flush(copy, sizeof(robj));
  return (void *)copy;
}

/* Free robj in NVM */
static inline void nvm_free_robj(struct nvm_dict* nvm_dict, void* robj) {
  /* FIXME: Not supported yet. Perhaps use GC to compact and recycle. */
  (void)nvm_dict;
  (void)robj;
  return;
}

int nvm_init_server(struct redisServer* server);
void nvm_cleanup_server(struct redisServer* server);

#endif
