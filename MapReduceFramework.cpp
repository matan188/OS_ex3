#include "MapReduceFramework.h"
#include "search.h"
#include <pthread.h>
#include <iostream>
#include <map>
#include <vector>

#define CHUNK_SIZE 2

IN_ITEMS_LIST inContainer;
pthread_mutex_t listIndexMut = PTHREAD_MUTEX_INITIALIZER;
int listIndex = 0;
//std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>>*> midContainers;

void * mapExec(void * p) {
    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    //std::vector<std::pair<k2Base*, v2Base*>> * container = new std::vector<std::pair<k2Base*, v2Base*>>();
    //midContainers[pthread_self()] = container;
    while(true) {
        pthread_mutex_lock(&listIndexMut);
        if(listIndex >= inContainer.size()) {
            pthread_exit(NULL);
        }
        currentChunk = listIndex;
        listIndex += CHUNK_SIZE;
        std::cout << "Thread " << pthread_self() << " listIndex: " << listIndex << std::endl;
        pthread_mutex_unlock(&listIndexMut);
        
        auto it = inContainer.begin();
        std::advance(it, currentChunk);
        for(int i = currentChunk; i < inContainer.size() && i < currentChunk + CHUNK_SIZE; ++i) {
            auto query = it->first;
            auto dir = static_cast<Directory*>(it->second);
            mapReduce->Map(query, dir);
            std::advance(it, 1);
        }
    }
}

OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce,
                                     IN_ITEMS_LIST& itemsList,
                                     int multiThreadLevel) {
    OUT_ITEMS_LIST outItemsList;
    inContainer = itemsList;
    std::list<pthread_t*> threads;
    
    /*******/
    /* MAP */
    /*******/
    
    // Create threads
    for(int i = 0; i < multiThreadLevel; ++i) {
        //std::cout << i << std::endl;
        // create ExecMap thread
        pthread_t p;
        pthread_create(&p, NULL, mapExec, &mapReduce);
        threads.push_back(&p);
    }
    
    // Join all threads
    for(pthread_t * p : threads) {
        void * x;
        pthread_join(*p, &x);
    }
    
    
    
    /***********/
    /* SHUFFLE */
    /***********/
    
    
    return outItemsList;
}


void Emit2 (k2Base* k2, v2Base* v2) {
    //midContainers[pthread_self()]->push_back({k2, v2});
    std::cout << static_cast<FileName1*>(k2)->getVal() << std::endl;
}

void Emit3 (k3Base*, v3Base*) {
    
}