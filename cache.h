#include <iostream>
#include <cstdio>
#include <vector>
using namespace std;

struct File_info {
    uint64_t time, key_num, min_key, max_key;
    char bloomfilter[10240] = {0};
    uint64_t *key;
    uint32_t *offset;
};

struct Level {
    uint64_t file_num = 0;
    vector<File_info *> file_info;
};

class Cache
{
private:

public:
    Cache() {};
    ~Cache();

    uint64_t level_num = 0;
    vector<Level *> level;

    void print();
    string file_name(File_info *file);

    string add_file_info(char *buf, int lv);
    string delete_file_info(int lv, int fl);
    void sort_file(Level *level);
    void sort_time(Level *level);
    void sort_time_big(Level *level);

    int find_scan(int lv, uint64_t key1, uint64_t key2, int &fl1, int &fl2);

};