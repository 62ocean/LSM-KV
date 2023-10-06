#include <iostream>
#include <cstdio>
#include <algorithm>
#include "cache.h"
using namespace std;

Cache::~Cache()
{
    for (int i = 0; i < level_num; ++i) {
        for (int j = 0; j < level[i]->file_num; ++j) {
            File_info *file = level[i]->file_info[j];
            delete [] file->key;
            delete [] file->offset;
        }
    }
}

void Cache::print()
{
    for (int i = 0; i < level_num; ++i) {
        cout << "level:" << i << " file_num:" << level[i]->file_num << endl;
        for (int j = 0; j < level[i]->file_num; ++j) {
            cout << " time:" << level[i]->file_info[j]->time << " key_num:" << level[i]->file_info[j]->key_num
                << " min_key:" <<  level[i]->file_info[j]->min_key << " max_key:" << level[i]->file_info[j]->max_key << endl;
            File_info *fl = level[i]->file_info[j];
            for (int k = 0; k < 3; ++k) {
                cout << "   " << fl->key[k] << ' ' << fl->offset[k] << endl;
            }
            for (int k = fl->key_num-3; k < fl->key_num; ++k) {
                cout << "   " << fl->key[k] << ' ' << fl->offset[k] << endl;
            }
        }
    }
}

bool cmp_key(File_info *a, File_info *b) {
    return a->min_key < b->min_key;
}
bool cmp_time(File_info *a, File_info *b) {
    if (a->time == b->time) return a->min_key < b->min_key;
    return a->time < b->time;
}
bool cmp_time_big(File_info *a, File_info *b) {
    return a->time > b->time;
}

void Cache::sort_file(Level *lv)
{
    sort((lv->file_info).begin(), (lv->file_info).end(), cmp_key);
}
void Cache::sort_time(Level *lv)
{
    sort((lv->file_info).begin(), (lv->file_info).end(), cmp_time);
}
void Cache::sort_time_big(Level *lv)
{
    sort((lv->file_info).begin(), (lv->file_info).end(), cmp_time_big);
}

string Cache::file_name(File_info *file)
{
    return to_string(file->time) + "-" + to_string(file->key_num) + "-"
        + to_string(file->min_key) + "-" + to_string(file->max_key) + ".sst";
}

string Cache::add_file_info(char *buf, int lv)
{
    if (lv == level_num) {
        ++level_num;
        Level *lv = new Level;
        lv->file_num = 0;
		level.push_back(lv);
    }
    /* prepare file info */
	File_info *file = new File_info;
	file->time = *(uint64_t *)buf;
	file->key_num = *(uint64_t *)(buf + 8);
	file->min_key = *(uint64_t *)(buf + 16);
	file->max_key = *(uint64_t *)(buf + 24);

    for (int i = 0; i < 10240; ++i) {
        file->bloomfilter[i] = (*(char *)(buf+ 32 + i));
    }
    
    //cout << file->key_num << endl;
    file->key = new uint64_t[file->key_num];
    //cout << file->key << endl;
	file->offset = new uint32_t[file->key_num];
    //cout << file->offset << endl;
    
	for (uint64_t i = 0; i < file->key_num; ++i) {
		file->key[i] = (*(uint64_t *)(buf+ 10272 + 12 * i));
		file->offset[i] = (*(uint32_t *)(buf + 10272 + 8 + 12 * i));
	}
   
	level[lv]->file_num++;
	level[lv]->file_info.push_back(file);
    //don't push_back and sort. insert!
    //print();

    return file_name(file);
}

string Cache::delete_file_info(int lv,int fl)
{
    File_info *file = level[lv]->file_info[fl];
    string ret = file_name(file);

    delete file->key;
    delete file->offset;
    level[lv]->file_info.erase(level[lv]->file_info.begin() + fl);
    level[lv]->file_num--;

    return ret;
}

int Cache::find_scan(int lv, uint64_t key1, uint64_t key2, int &fl1, int &fl2)
{
    // cout << "level_num " << level_num << endl;
    if (level_num == lv) return 1;
    int file_num = level[lv]->file_num;
    if (key1 < level[lv]->file_info[0]->min_key && key2 < level[lv]->file_info[0]->min_key)
        return 1;
    if (key1 > level[lv]->file_info[file_num - 1]->max_key && key2 > level[lv]->file_info[file_num - 1]->max_key)
        return 1;

    if (key1 < level[lv]->file_info[0]->min_key) fl1 = 0;
    if (key2 > level[lv]->file_info[file_num - 1]->min_key) fl2 = file_num - 1;

    for (int i = 0; i < file_num; ++i) {
        // cout << i << endl;
        if (key1 >= level[lv]->file_info[i]->min_key && key1 <= level[lv]->file_info[i]->max_key) fl1 = i;
        if (i < file_num - 1 && key1 > level[lv]->file_info[i]->max_key && key1 < level[lv]->file_info[i+1]->min_key) fl1 = i + 1;
        if (key2 >= level[lv]->file_info[i]->min_key && key2 <= level[lv]->file_info[i]->max_key) fl2 = i;
        if (i < file_num - 1 && key2 > level[lv]->file_info[i]->max_key && key2 < level[lv]->file_info[i+1]->min_key) fl2 = i + 1;
    }
    return 0;
}