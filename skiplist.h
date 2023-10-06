#pragma once

#include <vector>
#include <climits>
#include <time.h>
#include <cmath>
#include <cstring>
using namespace std;


enum SKNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct SKNode
{
    uint64_t key;
    string val;
    SKNodeType type;
    std::vector<SKNode *> forwards;
    SKNode(uint64_t _key, string _val, SKNodeType _type)
        : key(_key), val(_val), type(_type)
    {
        for (int i = 0; i < 10; ++i)
        {
            forwards.push_back(nullptr);
        }
    }
};

class SkipList
{
private:
    const double p = 0.25;
    const int MAXN = 233017;
    int MAX_LEVEL = 10;
    int level = 1;

    int randomLevel();
    

public:
    uint64_t elementNum = 0;
    uint64_t minKey = 0xffffffffffffffff, maxKey = 0;

    SKNode *head;
    SKNode *NIL;

    int size = 10240 + 32;
    const int MAX_SIZE = 2 * 1024 * 1024;

    SkipList()
    {
        //std::cout << MAX_LEVEL << std::endl;
        head = new SKNode(0, "", SKNodeType::HEAD);
        NIL = new SKNode(0xffffffffffffffff, "", SKNodeType::NIL);
        for (int i = 1; i <= MAX_LEVEL; ++i)
        {
            head->forwards[i] = NIL;
        }
        srand(time(NULL));
    }

    void Insert(uint64_t key, string value);
    string Search(uint64_t key);
    SKNode *LowerBound(uint64_t key);
    bool Delete(uint64_t key);
    void TimeSearch(int key_start, int key_end);
    void Display();
    ~SkipList()
    {
        SKNode *n1 = head;
        SKNode *n2;
        while (n1)
        {
            n2 = n1->forwards[1];
            delete n1;
            n1 = n2;
        }
    }
};
