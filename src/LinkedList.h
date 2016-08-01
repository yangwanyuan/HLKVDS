#ifndef _KV_DB_LINKEDLIST_H_
#define _KV_DB_LINKEDLIST_H_

#include <string.h>
#include <unistd.h>
#include <list>
#include <vector>
#include "Db_Structure.h"

using namespace std;
namespace kvdb{

    class EntryNode{
    public:
        HashEntry data;
        EntryNode* next;

        EntryNode(HashEntry value, EntryNode* nd): data(value), next(nd){};
    };

    class LinkedList{
    public:
        LinkedList();
        LinkedList(const LinkedList &);
        ~LinkedList();
        LinkedList& operator= (const LinkedList &);
        bool insert(HashEntry);
        bool remove(HashEntry);
        bool search(HashEntry);
        vector<HashEntry> get();
        int get_size() {return m_size;}

    private:
        EntryNode* m_head;
        int m_size;

        void copyHelper(const LinkedList &);
        void removeAll();
    };
}

#endif
