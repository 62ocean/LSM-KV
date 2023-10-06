#include <iostream>
#include <stdlib.h>
#include <ctime>
#include <cmath>

#include "skiplist.h"

using namespace std;


int SkipList::randomLevel()
{
    int result = 1;
    
    double randomNum = (rand() % 1000 / (double)(1000));
    while (result < MAX_LEVEL && randomNum < p)
    {
        //std::cout << randomNum << std::endl;
        ++result;
        randomNum = (rand() % 1000 / (double)(1000));
    }
    //std::cout << result << std::endl;
    return result;
}

void SkipList::Insert(uint64_t key, string value)
{
    SKNode* update[MAX_LEVEL+1];
    SKNode* x = head;
    for (int i = level; i >= 1; --i) {
        while (x->forwards[i]->key < key) x = x->forwards[i];
        update[i] = x;
        //std::cout << update[i]->key << std::endl;
    }
    x = x->forwards[1];
    if (x->key == key) {
        size -= (12 + (x->val).length() + 1);
        size += (12 + value.length() + 1);
        x->val = value;
    }
    else {
        int nodeLevel = randomLevel();
        //std::cout << nodeLevel << std::endl;
        if (nodeLevel > level) {
            for (int i = level+1; i <= nodeLevel; ++i)
                update[i] = head;
            level = nodeLevel;
        }
        x = new SKNode(key, value, NORMAL);
        for (int i = 1; i <= nodeLevel; ++i) {
            x->forwards[i] = update[i]->forwards[i];
            update[i]->forwards[i] = x;
        }

        size += (12 + value.length() + 1);
        //std::cout << size << std::endl;
        ++elementNum;
    }
   // Display();
   minKey = std::min(minKey, key);
   maxKey = std::max(maxKey, key);
}

void NodeOutput(int layer, SKNode *node)
{
    if (node->type == NORMAL) std::cout << layer+1 << "," << node->key << " ";
    else if (node->type == HEAD) std::cout << layer+1 << ",h ";
    else if (node->type == NIL) std::cout << layer+1 << ",N ";
}

string SkipList::Search(uint64_t key)
{
    SKNode *x = head;
    for (int i = level; i >= 1; --i) {
        while (x->forwards[i]->key < key) {
            x = x->forwards[i];
        }
    }
    x = x->forwards[1];
    //NodeOutput(0, x);
    if (x->key == key) return x->val;
    else return "";
}

SKNode * SkipList::LowerBound(uint64_t key)
{
    //return;
    SKNode *x = head;
    for (int i = level; i >= 1; --i) {
        while (x->forwards[i]->key < key) {
            x = x->forwards[i];
        }
    }
    x = x->forwards[1];
    return x;
    // while (x->key <= key+len) {
    //     x = x->forwards[0]; 
    // }
    //NodeOutput(0, x);
    //if (x->key == key) std::cout << x->val << std::endl;
    //else std::cout << "Not Found" << std::endl;
}

bool SkipList::Delete(uint64_t key)
{
    
    SKNode* update[MAX_LEVEL+1];
    SKNode* x = head;
    for (int i = level; i >= 1; --i) {
        while (x->forwards[i]->key < key) x = x->forwards[i];
        update[i] = x;
    }
    x = x->forwards[1];
    if (x->key == key) {
        size -= (12 + x->val.length() + 1);
        --elementNum;
        for (int i = 1; i <= level; ++i) {
            if (update[i]->forwards[i] != x) break;
            update[i]->forwards[i] = x->forwards[i];
        }
        delete x;
        while (level > 1 && head->forwards[level] == NIL)
            --level;  
        return true;  
    } else {
        return false;
    }
}

void SkipList::Display()
{
    for (int i = level; i >= 1; --i)
    {
        std::cout << "Level " << i << ":h";
        SKNode *node = head->forwards[i];
        while (node->type != SKNodeType::NIL)
        {
            std::cout << "-->(" << node->key << "," << node->val << ")";
            node = node->forwards[i];
        }

        std::cout << "-->N" << std::endl;
    }
}