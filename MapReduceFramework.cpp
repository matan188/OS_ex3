#include "MapReduceFramework.h"
#include "search.h"
#include <pthread.h>
#include <iostream>
#include <stdio.h>
#include <map>
#include <vector>

#define CHUNK_SIZE 2

IN_ITEMS_LIST inContainer;
pthread_mutex_t listIndexMut = PTHREAD_MUTEX_INITIALIZER;
int listIndex = 0;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>>*> midContainers;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>>*> midBufferedContainers;
std::map<pthread_t, pthread_mutex_t> midContainersMut;

std::map<k2Base, std::pair<k2Base*, V2_LIST>> postShuffleContainer;

pthread_cond_t cond;
int rc = pthread_cond_init(&cond, NULL);

void clearBuffer() {
    auto selfId = pthread_self();
    pthread_mutex_lock(&midContainersMut[selfId]);
    auto m = midBufferedContainers[selfId];
    for(auto it = m->begin(); it != m->end(); ++it) {
        midContainers[selfId]->push_back(*it);
    }
    m->clear();
    pthread_mutex_unlock(&midContainersMut[selfId]);
}

void * mapExec(void * p) {
    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    std::vector<std::pair<k2Base*, v2Base*>> * container = new std::vector<std::pair<k2Base*, v2Base*>>();
    std::vector<std::pair<k2Base*, v2Base*>> * bufferedContainer = new std::vector<std::pair<k2Base*, v2Base*>>();
    midContainers[pthread_self()] = container;
    midBufferedContainers[pthread_self()] = bufferedContainer;
    midContainersMut[pthread_self()] = PTHREAD_MUTEX_INITIALIZER;
    
    while(true) {
        pthread_mutex_lock(&listIndexMut);
        if(listIndex >= inContainer.size()) {
            clearBuffer();
            pthread_mutex_unlock(&listIndexMut);
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
            ++it;
        }
        clearBuffer();
    }
}

void * shuffle(void * p) {
    timespec t;
    t.tv_nsec = 1000000;
    pthread_mutex_t tempMut = PTHREAD_MUTEX_INITIALIZER;
    while(true) {
        // search for non empty list
        for(auto it = midContainers.begin(); it != midContainers.end(); ++it) {
            auto key = it->first;
            auto list = it->second;
            if(!list->empty()) {
                auto mut = midContainersMut[key];
                pthread_mutex_lock(&mut);
                postShuffleContainer
                pthread_mutex_unlock(&mut);
            }
        }
        pthread_cond_timedwait(&cond, &tempMut, &t);
    }
    return nullptr;
}

OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce,
                                     IN_ITEMS_LIST& itemsList,
                                     int multiThreadLevel) {
    OUT_ITEMS_LIST outItemsList;
    inContainer = itemsList;
    std::list<pthread_t*> threads;
   

    
    /***********/
    /* SHUFFLE */
    /***********/
    
    pthread_t shuffleThread;
    pthread_create(&shuffleThread, NULL, shuffle, nullptr);
    
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
    void * x;
    for(pthread_t * p : threads) {
        pthread_join(*p, &x);
    }
    pthread_join(shuffleThread, &x);
    
    // destroy cond
    pthread_cond_destroy(&cond);
    
    sleep(1);

    
    
    return outItemsList;
}

void Emit2 (k2Base* k2, v2Base* v2) {
    /*
    auto v = midBufferedContainers[pthread_self()];
    v->push_back({k2, v2});
     */
    midBufferedContainers[pthread_self()]->push_back({k2, v2});
    std::cout << static_cast<FileName1*>(k2)->getVal() << std::endl;
}

void Emit3 (k3Base*, v3Base*) {
    
}