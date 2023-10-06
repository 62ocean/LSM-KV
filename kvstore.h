#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "cache.h"

class KVStore : public KVStoreAPI {
private:
	string dir;

	SkipList *skiplist;
	Cache *cache;
	uint64_t time = 0;

	void init_cache();

	void set_hash(char *bloomfilter, uint64_t key);
	bool find_hash(char *bloomfilter, uint64_t key);

	void prepare_buf(char **buf);
	void createSSTable(char *buf, int size, int lv);
	void createSSTable_memTable();
	void createSSTable_compaction(uint64_t time, uint64_t num, uint64_t minkey, uint64_t maxkey,
									char *bloomfilter, char *index, int indexPos, char *data, int dataPos, int lv);

	void deleteSStable(int lv, int fl);

	int get_datasize(File_info *file, ifstream &File, int pos);
	std::string get_value(int lv, int fl, int no);
	std::string get_value_openfile(File_info *file, int no, ifstream &File);

	void copy_file(int lv, int fl, uint64_t time);
	void compaction_one_sst(int lv, int fl);
	void compaction();

public:
	KVStore(const std::string &dir);
	
	~KVStore();
	
	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
