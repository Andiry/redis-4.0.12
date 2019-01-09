#ifndef __REDIS_NVM_H
#define __REDIS_NVM_H

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

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

#endif
