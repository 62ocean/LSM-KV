#include "kvstore.h"
#include "skiplist.h"
#include "utils.h"
#include "MurmurHash3.h"
#include <cstring>
#include <fstream>
#include <vector>
#include <iostream>
#include <algorithm>
#include <map>
using namespace std;


KVStore::KVStore(const std::string &dir0): KVStoreAPI(dir0)
{
	dir = dir0;
	skiplist = new SkipList;
	cache = new Cache;
	init_cache();
	// cache->print();
}

KVStore::~KVStore()
{
	if (skiplist->elementNum) {
		createSSTable_memTable();
	}

	delete skiplist;
	delete cache;
}

void KVStore::init_cache()
{
	vector<string> level_list;
	/* find all levels */
	cache->level_num = utils::scanDir(dir, level_list);
	for (int i = 0; i < cache->level_num; ++i) {
		/* create a level in cache */
		cache->level.push_back(new Level);

		/* find all files in this level */
		string level_path = dir + "/" + level_list[i];
		vector<string> file_list;
		Level *lv = cache->level[i];
		lv->file_num = utils::scanDir(level_path, file_list);

		for (int j = 0; j < lv->file_num; ++j) {
			/* create a new file_info */
			File_info *file = new File_info;

			/* open the file */
			string file_path = level_path + "/" + file_list[j];
			ifstream inFile;
			inFile.open(file_path.c_str(), ios::in | ios::binary);

			/* read the header */
			
			inFile.read((char *)&file->time, sizeof(uint64_t));
			inFile.read((char *)&file->key_num, sizeof(uint64_t));
			inFile.read((char *)&file->min_key, sizeof(uint64_t));
			inFile.read((char *)&file->max_key, sizeof(uint64_t));
			time = std::max(time, file->time); /* update time */

			for (int k = 0; k < 10240; ++k) {
				inFile.read((char *)&file->bloomfilter[k], sizeof(char));
			}

			/* read the index*/
			file->key = new uint64_t [file->key_num];
			file->offset = new uint32_t [file->key_num];
			
			for (int k = 0; k < file->key_num; ++k) {
				inFile.read((char *)&file->key[k], sizeof(uint64_t));
				inFile.read((char *)&file->offset[k], sizeof(uint32_t));
			}
			lv->file_info.push_back(file);

			inFile.close();
		}

		cache->sort_file(lv);
	}
	time ++;
}

void KVStore::prepare_buf(char **buf)
{
	/* prepare buf */
	*buf = new char[skiplist->size];
	char *indexPos = *buf + 10272;
	char *dataPos = indexPos + skiplist->elementNum * 12;
	char bloomfilter[10240] = {0};

	for (SKNode *pos = skiplist->head->forwards[1]; pos->type != SKNodeType::NIL; pos = pos->forwards[1]) {
		/* put index, offset, value in buf*/
		*(uint64_t *)(indexPos) = pos->key;
		*(uint32_t *)(indexPos + 8) = dataPos - *buf;
		indexPos += 12;
		memcpy(dataPos, (pos->val).c_str(), (pos->val).length()+1);
		dataPos += (pos->val).length()+1;

		/* prepare bloomfilter */
		unsigned int hash[4] = {0};
		MurmurHash3_x64_128(&(pos->key), sizeof(pos->key), 1, hash);
		for (int i = 0; i < 4; ++i) {
			hash[i] %= 10240 * 8;
			int index = hash[i] / 8;
			int offset = hash[i] % 8;
			bloomfilter[index] |= (1 << (7 - offset));
		}
	}
	/* put header in *buf */
	*(uint64_t *)(*buf) = time;
	*(uint64_t *)(*buf + 8) = skiplist->elementNum;
	*(uint64_t *)(*buf + 16) = skiplist->minKey;
	*(uint64_t *)(*buf + 24) = skiplist->maxKey;
	memcpy(*buf+32, bloomfilter, 10240);
}

/* 将buf中的内容写入到文件中 + 更新缓存 */
void KVStore::createSSTable(char *buf, int size, int lv)
{
	string file_name = cache->add_file_info(buf, lv);

	string path = dir + "/level-" + to_string(lv);
	if (!(utils::dirExists(path))) {
		utils::mkdir(path.c_str());
	}
	path += "/" + file_name;

	/* create file */
	ofstream outFile;
	outFile.open(path.c_str(), ios::out | ios::binary);

	/* write file */
	outFile.write(buf, size);
	outFile.close();
}

void KVStore::createSSTable_memTable()
{
	char *buf;
	prepare_buf(&buf);
	createSSTable(buf, skiplist->size, 0);

	delete [] buf;
	++time;
}

void KVStore::createSSTable_compaction(uint64_t time, uint64_t num, uint64_t minkey, uint64_t maxkey,
									char *bloomfilter, char *index, int indexPos, char *data, int dataPos, int lv)
{
	char *buf = new char[2*1024*1024];

	*(uint64_t *)buf = time;
	*(uint64_t *)(buf + 8) = num;
	*(uint64_t *)(buf + 16) = minkey;
	*(uint64_t *)(buf + 24) = maxkey;
	memcpy(buf + 32, bloomfilter, 10240);

	for (int i = 0; i < num; ++i) {
		*(uint32_t *)(index + 12 * i + 8) += 10272 + indexPos;
	}
	memcpy(buf + 10272, index, indexPos);
	memcpy(buf + 10272 + indexPos, data, dataPos);

	createSSTable(buf, 10272 + indexPos + dataPos, lv);

	// cache->print();

	delete [] buf;
}

/* 删掉特定文件 + 更新缓存 */
void KVStore::deleteSStable(int lv, int fl)
{
	string file_name = cache->delete_file_info(lv, fl);
	string path = dir + "/level-" + to_string(lv) + "/" + file_name;
	// cout << path << endl;
	utils::rmfile(path.c_str());
}

void KVStore::compaction()
{
	uint64_t maxtime = 0;
	int fl_num = cache->level[0]->file_num;

	
	cache->sort_time(cache->level[0]);
	// cache->print();
	for (int i = 0; i < fl_num; ++i) {
		compaction_one_sst(0, 0);
	}

	for (int i = 1; i < cache->level_num; ++i) {
		int compaction_num = cache->level[i]->file_num - (1 << (i + 1));
		if (compaction_num <= 0) break;

		cache->sort_time(cache->level[i]);
		for (int j = 0; j < compaction_num; ++j) {
			compaction_one_sst(i, 0);
		}
		cache->sort_file(cache->level[i]);
	}
}

void KVStore::set_hash(char *bloomfilter, uint64_t key)
{
	unsigned int hash[4] = {0};
	MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
	for (int i = 0; i < 4; ++i) {
		hash[i] %= 10240 * 8;
		int index = hash[i] / 8;
		int offset = hash[i] % 8;
		bloomfilter[index] |= (1 << (7 - offset));
	}
}
bool KVStore::find_hash(char *bloomfilter, uint64_t key)
{
	unsigned int hash[4] = {0};
	MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
	for (int i = 0; i < 4; ++i) {
		hash[i] %= 10240 * 8;
		int index = hash[i] / 8;
		int offset = hash[i] % 8;
		if (!(bloomfilter[index] & (1 << (7 - offset)))) return false;
	}
	return true;
}

void KVStore::compaction_one_sst(int lv, int fl)
{
	char bloomfilter[10240];
	char *index = new char[1024*1024*2];
	char *data = new char[1024*1024*2];
	uint64_t minkey = ~0, maxkey = 0;
	uint64_t num = 0;
	int pos = 0;
	int size = 10272;
	int indexPos = 0, dataPos = 0;
	

	File_info *file1 = cache->level[lv]->file_info[fl];
	string file1_path = dir + "/level-" + to_string(lv) + "/" + cache->file_name(file1);
	ifstream File1(file1_path.c_str(), ios::in | ios::binary);

	int fl1, fl2;
	int flag = cache->find_scan(lv+1, file1->min_key, file1->max_key, fl1, fl2);
	if (flag == 1) {
		File1.close();
		copy_file(lv, fl, file1->time);
		cache->sort_file(cache->level[lv + 1]);
		delete [] index;
		delete [] data;
		return;
	}
	uint64_t maxtime = file1->time;
	for (int i = fl1; i <= fl2; ++i) {
		maxtime = max(maxtime, cache->level[lv+1]->file_info[i]->time);
	}

	for (int i = fl1; i <= fl2; ++i) {
		File_info *file2 = cache->level[lv+1]->file_info[i];
		string file2_path = dir + "/level-" + to_string(lv+1) + "/" + cache->file_name(file2);
		ifstream File2(file2_path.c_str(), ios::in | ios::binary);

		for (int j = 0; j < file2->key_num; ++j) {
			//默认file2中的key插入
			bool file2_flag = true;
			while (pos < file1->key_num && file1->key[pos] <= file2->key[j]) {
				if (file1->key[pos] == file2->key[j] && file1->time < file2->time) {
					//如果file1中的时间戳更小，file1中的key不插入，指针后移+break
					++pos;
					break;
				} else if (file1->key[pos] == file2->key[j]) {
					//如果file1中的时间戳更大，插入这个key，并将file2_flag置为false，即不插入file2的key
					file2_flag = false;
				}

				//file1的key放入

				string val = get_value_openfile(file1, pos, File1);
				if (val == "~DELETED~" && lv + 1 == cache->level_num - 1) {
					++pos;
					continue;
				}

				int dataSize = get_datasize(file1, File1, pos);

				if (size + 12 + dataSize > 2 * 1024 * 1024) {
					createSSTable_compaction(maxtime, num, minkey, maxkey, bloomfilter,
												index, indexPos, data, dataPos, lv+1);
					num = 0;
					minkey = ~0;
					maxkey = 0;
					indexPos = dataPos = 0;
					size = 10272;
					memset(bloomfilter, 0, sizeof(bloomfilter));
					memset(index, 0, sizeof(index));
					memset(data, 0, sizeof(data));
				}
				
				minkey = min(minkey, file1->key[pos]);
				maxkey = max(maxkey, file1->key[pos]);
				++num;
				set_hash(bloomfilter, file1->key[pos]);

				*(uint64_t *)(index+indexPos) = file1->key[pos];
				*(uint32_t *)(index+indexPos+8) = dataPos;//不是最终的offset
				indexPos += 12;

				memcpy(data + dataPos, val.c_str(), val.length()+1);
				dataPos += dataSize;
				size += 12 + dataSize;

				++pos;
			}
				
			//file2的key放入
			if (!file2_flag) {
				continue;
			}
			
			string val = get_value_openfile(file2, j, File2);
			if (val == "~DELETED~" && lv + 1 == cache->level_num - 1) {
				continue;
			}
			

			int dataSize = get_datasize(file2, File2, j);

			if (size + 12 + dataSize > 2 * 1024 * 1024) {
				createSSTable_compaction(maxtime, num, minkey, maxkey, bloomfilter,
											index, indexPos, data, dataPos, lv+1);
				num = 0;
				minkey = ~0;
				maxkey = 0;
				indexPos = dataPos = 0;
				size = 10272;
				memset(bloomfilter, 0, sizeof(bloomfilter));
				memset(index, 0, sizeof(index));
				memset(data, 0, sizeof(data));
			}

			minkey = min(minkey, file2->key[j]);
			maxkey = max(maxkey, file2->key[j]);
			++num;
			set_hash(bloomfilter, file2->key[j]);

			*(uint64_t *)(index+indexPos) = file2->key[j];
			*(uint32_t *)(index+indexPos+8) = dataPos;//不是最终的offset
			indexPos += 12;

			memcpy(data + dataPos, val.c_str(), val.length()+1);
			dataPos += dataSize;

			size += 12 + dataSize;
		}
		File2.close();
	}
	while (pos < file1->key_num) {
		//file1放入
		string val = get_value_openfile(file1, pos, File1);
		if (val == "~DELETED~" && lv + 1 == cache->level_num - 1) {
			++pos;
			continue;
		}

		int dataSize = get_datasize(file1, File1, pos);

		if (size + 12 + dataSize > 2 * 1024 * 1024) {
			createSSTable_compaction(maxtime, num, minkey, maxkey, bloomfilter,
										index, indexPos, data, dataPos, lv+1);
			num = 0;
			minkey = ~0;
			maxkey = 0;
			indexPos = dataPos = 0;
			size = 10272;
			memset(bloomfilter, 0, sizeof(bloomfilter));
			memset(index, 0, sizeof(index));
			memset(data, 0, sizeof(data));
		}

		
		minkey = min(minkey, file1->key[pos]);
		maxkey = max(maxkey, file1->key[pos]);
		++num;
		set_hash(bloomfilter, file1->key[pos]);

		*(uint64_t *)(index+indexPos) = file1->key[pos];
		*(uint32_t *)(index+indexPos+8) = dataPos;//不是最终的offset
		indexPos += 12;

		memcpy(data + dataPos, val.c_str(), val.length()+1);
		dataPos += dataSize;
		size += 12 + dataSize;

		++pos;
	}
	File1.close();
	deleteSStable(lv, fl);
	for (int i = fl1; i <= fl2; ++i) deleteSStable(lv+1, fl1);

	createSSTable_compaction(maxtime, num, minkey, maxkey, bloomfilter,
								index, indexPos, data, dataPos, lv+1);
								
	cache->sort_file(cache->level[lv + 1]);

	delete [] data;
	delete [] index;

	// cache->print();
}

void KVStore::copy_file(int lv, int fl, uint64_t time)
{
	char *buf = new char[2*1024*1024];
	int size = 0;

	File_info *file = cache->level[lv]->file_info[fl];
	string inFile_path = dir + "/level-" + to_string(lv) + "/" + cache->file_name(file);
	ifstream inFile(inFile_path.c_str(), ios::in | ios::binary);

	while (!inFile.eof()) {
		inFile.read((char *)&buf[size++], sizeof(char));
	}
	*(uint64_t *)(buf) = time;
	inFile.close();

	deleteSStable(lv, fl);
	createSSTable(buf, size - 1, lv + 1);

	// cache->print();

	delete [] buf;
}

int KVStore::get_datasize(File_info *file, ifstream &File, int pos)
{
	int dataSize;
	if (pos == file->key_num - 1) {
		File.seekg( 0, ios::end );
		int file_size = File.tellg();
		dataSize = file_size - file->offset[pos];
	} else {
		dataSize = file->offset[pos+1] - file->offset[pos];
	}
	return dataSize;
}


std::string KVStore::get_value(int lv, int fl, int no)
{
	File_info *file = cache->level[lv]->file_info[fl];
	string file_path = dir + "/level-" + to_string(lv) + "/" + cache->file_name(file);
	ifstream File(file_path.c_str(), ios::in | ios::binary);

	int dataSize = get_datasize(file, File, no);
	char *data = new char [dataSize];

	File.seekg(file->offset[no], ios::beg);
	for (int k = 0; k < dataSize; ++k) {
		File.read((char *)&data[k], sizeof(char));
	}
	File.close();

	string str(data);
	delete [] data;

	return str;
}
std::string KVStore::get_value_openfile(File_info *file, int no, ifstream &File)
{
	int dataSize = get_datasize(file, File, no);
	char *data = new char [dataSize];

	File.seekg(file->offset[no], ios::beg);
	for (int k = 0; k < dataSize; ++k) {
		File.read((char *)&data[k], sizeof(char));
	}

	string str(data);
	delete [] data;

	return str;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	// cout << "put " << key << " " << s << endl;
	if (skiplist->size + s.length() + 13 > 2 * 1024 * 1024) {
		// cout << key << endl;
		createSSTable_memTable();
		if (key == 2607) cout << 111 << endl;
		if (cache->level[0]->file_num > 2) compaction();
		if (key == 2607) cout << 111 << endl;

		delete skiplist;
		skiplist = new SkipList;
	}
	
	skiplist->Insert(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	// cout << "GET " << key << endl;
	string mem_search = skiplist->Search(key);
	// cout << mem_search << endl;
	if (mem_search == "~DELETED~") return "";
	else if (mem_search != "") {
		return mem_search;
	} else {
		for (int i = 0; i < cache->level_num; ++i) {
			if (i == 0) {
				cache->sort_time_big(cache->level[0]);
			}
			for (int j = 0; j < cache->level[i]->file_num; ++j) {
				File_info *file = cache->level[i]->file_info[j];
				if (key < file->min_key || key > file->max_key) continue;
				if (!find_hash(file->bloomfilter, key)) continue;
				int pos = lower_bound(file->key, file->key + file->key_num, key) - file->key;
				if (key == file->key[pos]) {
					string val = get_value(i, j, pos);
					// cout << val << endl;
					// cout << (val == "~DELETED~") << endl;
					if (val == "~DELETED~") return "";
					else return val;
				}
			}
		}

		return "";
	}
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	// cout << "DELETE " << key << endl;
	if (get(key) == "") return false;
	put(key, "~DELETED~");
	return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	//not delete SSTable yet...
	for (int i = 0; i < cache->level_num; ++i) {
		string path = dir + "/level-" + to_string(i);
		for (int j = 0; j < cache->level[i]->file_num; ++j) {
			string file_path = path + "/" + cache->file_name(cache->level[i]->file_info[j]);
			utils::rmfile(file_path.c_str());
		}
		utils::rmdir(path.c_str());
	}

	delete skiplist;
	delete cache;
	skiplist = new SkipList;
	cache = new Cache;
	time = 1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
	map<uint64_t, string> scan_map;

	SKNode *mem_pos = skiplist->LowerBound(key1);

	while (mem_pos->key <= key2) {
		if (!scan_map.count(mem_pos->key)) //最后再处理deleted，否则被删除的可能会出现在map中
			scan_map.insert({mem_pos->key, mem_pos->val});
		mem_pos = mem_pos->forwards[1];
	}
	
	for (int i = 0; i < cache->level_num; ++i) {
		if (i == 0) {
			cache->sort_time_big(cache->level[0]);
		}
		for (int j = 0; j < cache->level[i]->file_num; ++j) {
			File_info *file = cache->level[i]->file_info[j];
			if (key1 > file->max_key || key2 < file->min_key) continue;
			int pos;
			if (key1 > file->min_key) {
				pos = lower_bound(file->key, file->key + file->key_num, key1) - file->key;
			} else {
				pos = 0;
			}

			while (file->key[pos] <= key2 && pos < file->key_num) {
				if (!scan_map.count(file->key[pos]))
					scan_map.insert({file->key[pos], get_value(i, j, pos)});
				++pos;
			}
		}
	}

	map<uint64_t, string>::iterator it;
	for (it = scan_map.begin(); it != scan_map.end(); ++it) {
		if (it->second != "~DELETED~") {
			list.push_back(std::pair<uint64_t, std::string>(it->first, it->second));
		}
	}
}