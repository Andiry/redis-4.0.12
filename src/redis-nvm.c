#define _GNU_SOURCE
#include <sys/mman.h>
#include <fcntl.h>

#include "redis-nvm.h"

char* prefix = "/mnt/ramdisk/redis_nvm";

/* MMap hashtable file or data file. */
static int nvm_map_dict_file(const char* filename,
                               size_t filesize,
                               void** ret_addr,
                               int* ret_fd) {
  int fd;
  int err;
  void* addr;

  fd = open(filename, O_CREAT | O_RDWR, 0666);
  if (fd < 0) {
    serverLog(LL_WARNING, "Open file %s failed", filename);
    return -EINVAL;
  }

  err = fallocate(fd, 0, 0, filesize);
  if (err != 0) {
    serverLog(LL_WARNING, "Fallocate file %s failed", filename);
    close(fd);
    return err;
  }

  addr = mmap(NULL, filesize, PROT_READ | PROT_WRITE,
              MAP_SHARED, fd, 0);
  if (!addr) {
    serverLog(LL_WARNING, "Mmap file %s failed", filename);
    close(fd);
    return -EINVAL;
  }

  *ret_addr = addr;
  *ret_fd = fd;
  return 0;
}

static void nvm_dict_unmap_files(struct nvm_dict* nvm_dict) {
  munmap(nvm_dict->hashtable0_addr, nvm_dict->hashtable0_size);
  munmap(nvm_dict->hashtable1_addr, nvm_dict->hashtable1_size);
  munmap(nvm_dict->data_addr, nvm_dict->data_size);
  close(nvm_dict->hashtable0_fd);
  close(nvm_dict->hashtable1_fd);
  close(nvm_dict->data_fd);
}

static int nvm_create_dict(dict* redis_dict,
                           struct nvm_dict* nvm_dict,
                           char* name) {
  char filename[256];
  dictht* redis_ht;
  int ret;

  sprintf(filename, "%s-0.ht", name);
  ret = nvm_map_dict_file(filename,
                          nvm_dict->hashtable0_size,
                          &nvm_dict->hashtable0_addr,
                          &nvm_dict->hashtable0_fd);
  if (ret != 0) {
    return -EINVAL;
  }

  redis_ht = &redis_dict->ht[0];
  redis_ht->table = (dictEntry**)(nvm_dict->hashtable0_addr);

  sprintf(filename, "%s-1.ht", name);
  ret = nvm_map_dict_file(filename,
                          nvm_dict->hashtable1_size,
                          &nvm_dict->hashtable1_addr,
                          &nvm_dict->hashtable1_fd);
  if (ret != 0) {
    return -EINVAL;
  }

  redis_ht = &redis_dict->ht[1];
  redis_ht->table = (dictEntry**)(nvm_dict->hashtable1_addr);

  sprintf(filename, "%s.dat", name);
  ret = nvm_map_dict_file(filename,
                          nvm_dict->data_size,
                          &nvm_dict->data_addr,
                          &nvm_dict->data_fd);
  if (ret != 0) {
    return -EINVAL;
  }

  redis_dict->nvm_dict = nvm_dict;
  redis_dict->use_nvm = 1;
  return 0;
}

static void nvm_init_dict(struct nvm_dict* nvm_dict) {
  nvm_dict->hashtable0_size = NVM_HASHTABLE_INIT_SIZE;
  nvm_dict->hashtable1_size = NVM_HASHTABLE_INIT_SIZE;
  nvm_dict->allocated_size = 0;
  nvm_dict->data_size = NVM_DATA_INIT_SIZE;
}

int nvm_init_server(struct redisServer* server) {
  struct nvm_server* nvm_server;
  int fd;
  char name[256];
  int ret;

  nvm_server = zmalloc(sizeof(struct nvm_server));
  if (!nvm_server) {
    serverPanic("Failed to allocate nvm server");
    return -ENOMEM;
  }

  nvm_server->num_dicts = server->dbnum;
  nvm_server->nvm_dicts = zmalloc(sizeof(struct nvm_dict) *
                                  nvm_server->num_dicts);
  memset(nvm_server->nvm_dicts, 0, sizeof(struct nvm_dict) *
         nvm_server->num_dicts);

  if (!nvm_server->nvm_dicts) {
    serverPanic("Failed to allocate nvm dicts");
    zfree(nvm_server);
    return -ENOMEM;
  }

  server->nvm_server = (void*)nvm_server;

  for (int i = 0; i < nvm_server->num_dicts; i++) {
    struct nvm_dict* nvm_dict = &nvm_server->nvm_dicts[i];
    dict* redis_dict = server->db[i].dict;

    sprintf(name, "%s%d.dict", prefix, i);
    serverLog(LL_WARNING, "Create NVM dict %d", i);
    if ((fd = open(name, O_RDWR, 0666)) < 0) {
      /* Create new NVM dict */
      if ((fd = open(name, O_CREAT | O_RDWR, 0666)) < 0) {
        serverLog(LL_WARNING, "Create NVM dict file %s failed", name);
        ret = -EINVAL;
        goto out;
      }
      nvm_init_dict(nvm_dict);
    } else {
      /* Read existing NVM dict */
      ssize_t size = read(fd, nvm_dict, sizeof(struct nvm_dict));
      if (size != sizeof(struct nvm_dict)) {
        serverLog(LL_WARNING, "Read NVM dict %s failed, create new one", name);
        nvm_init_dict(nvm_dict);
      }
    }

    sprintf(name, "%s%d", prefix, i);
    ret = nvm_create_dict(redis_dict, nvm_dict, name);
    if (ret) {
      serverLog(LL_WARNING, "Create NVM dict %s failed", name);
      goto out;
    }
  }

  serverLog(LL_WARNING, "Initialize NVM server finished.");
  /* NVM mode. Disable appendfsync. */
  serverLog(LL_WARNING, "Running Redis in NVM mode. Disable appenfsync.");
  server->aof_fsync = AOF_FSYNC_NO;
  server->aof_state = AOF_OFF;

out:
  if (ret)
    nvm_cleanup_server(server);

  return ret;
}

static void nvm_dump_dict_info(struct nvm_dict* nvm_dict,
                               int index) {
  serverLog(LL_WARNING, "NVM dict %d:", index);
  serverLog(LL_WARNING, "Hashtable0 size %lu, keys %lu",
            nvm_dict->hashtable0_size, nvm_dict->hashtable0_keys);
  serverLog(LL_WARNING, "Hashtable1 size %lu, keys %lu",
            nvm_dict->hashtable1_size, nvm_dict->hashtable1_keys);
  serverLog(LL_WARNING, "Data size %lu, used %lu",
            nvm_dict->data_size, nvm_dict->allocated_size);
}

void nvm_cleanup_server(struct redisServer* server) {
  struct nvm_server* nvm_server = (struct nvm_server*)server->nvm_server;
  char name[256];
  int fd;

  if (server->nvm_mode == 0)
    return;

  for (int i = 0; i < nvm_server->num_dicts; i++) {
    struct nvm_dict* nvm_dict = &nvm_server->nvm_dicts[i];

    nvm_dump_dict_info(nvm_dict, i);
    nvm_dict_unmap_files(nvm_dict);

    sprintf(name, "%s%d.dict", prefix, i);
    if ((fd = open(name, O_CREAT | O_RDWR, 0666)) < 0) {
      serverLog(LL_WARNING, "Create NVM dict file %s failed", name);
      continue;
    }

    /* Save dict information */
    write(fd, nvm_dict, sizeof(struct nvm_dict));
  }

  zfree(nvm_server->nvm_dicts);
  zfree(nvm_server);

  server->nvm_server = NULL;
  server->nvm_mode = 0;

  serverLog(LL_WARNING, "Cleanup NVM server finished.");
}

