#include "MapReduceFramework.h"
#include "Search.h"
#include <pthread.h>
#include <iostream>
#include <stdio.h>
#include <map>
#include <algorithm>
#include <vector>
#include <unistd.h>

#define CHUNK_SIZE 1

IN_ITEMS_LIST inContainer;

pthread_mutex_t debugMut = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t listIndexMut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mapInitMut = PTHREAD_MUTEX_INITIALIZER;
int listIndex = 0;
int postShuffleContainerIndex = 0;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>*>*> mapContainers;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>*>*> mapBufferedContainers;
std::map<pthread_t, pthread_mutex_t> mapContainersMut;
std::map<pthread_t, std::vector<std::pair<k3Base*, v3Base*>*>*> reduceContainers;

int activeThreads;

struct cmpK2Base {
    bool operator()(const k2Base * a, const k2Base * b) const {
        return  *a < *b;
    }
};
std::map<k2Base *, V2_LIST, cmpK2Base> postShuffleContainer;

bool cmpK3Base (std::pair<k3Base*, v3Base*> a, std::pair<k3Base*, v3Base*> b) {
    return  a.first < b.first;
}


pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

void clearBuffer() {
    auto selfId = pthread_self();
    pthread_mutex_lock(&mapContainersMut[selfId]);
    auto m = mapBufferedContainers[selfId];
    for(auto it = m->begin(); it != m->end(); ++it) {
        mapContainers[selfId]->push_back(*it);
    }
    m->clear();
    pthread_mutex_unlock(&mapContainersMut[selfId]);
    pthread_cond_signal(&cond);
}

void * mapExec(void * p) {

    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    std::vector<std::pair<k2Base*, v2Base*>*> * container = new std::vector<std::pair<k2Base*, v2Base*>*>();
    std::vector<std::pair<k2Base*, v2Base*>*> * bufferedContainer = new std::vector<std::pair<k2Base*, v2Base*>*>();

    pthread_mutex_lock(&mapInitMut);
    mapContainers[pthread_self()] = container;
    mapBufferedContainers[pthread_self()] = bufferedContainer;
    mapContainersMut[pthread_self()] = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_unlock(&mapInitMut);
    
    while(true) {
        if(listIndex >= inContainer.size()) {
            clearBuffer();
            pthread_exit(NULL);
        }
        pthread_mutex_lock(&listIndexMut);
        currentChunk = listIndex;
        listIndex += CHUNK_SIZE;
        pthread_mutex_unlock(&listIndexMut);
        
        auto it = inContainer.begin();
        std::advance(it, currentChunk);
        for(int i = currentChunk; i < inContainer.size() && i < currentChunk + CHUNK_SIZE; ++i) {
            auto query = it->first;
            auto dir = it->second;
            mapReduce->Map(query, dir);
            ++it;
        }
        if(mapBufferedContainers[pthread_self()]->size() >= CHUNK_SIZE) {
            clearBuffer();
        }
    }
}

void * reduceExec(void * p) {
    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    std::vector<std::pair<k3Base*, v3Base*>*> * container = new std::vector<std::pair<k3Base*, v3Base*>*>();

    pthread_mutex_lock(&mapInitMut);
    reduceContainers[pthread_self()] = container;
    pthread_mutex_unlock(&mapInitMut);

    while(true) {
        if(postShuffleContainerIndex >= postShuffleContainer.size()) {
            pthread_exit(NULL);
        }
        pthread_mutex_lock(&listIndexMut);
        currentChunk = postShuffleContainerIndex;
        postShuffleContainerIndex += CHUNK_SIZE;
        pthread_mutex_unlock(&listIndexMut);

        auto it = postShuffleContainer.begin();
        std::advance(it, currentChunk);
        for(int i = currentChunk; i < postShuffleContainer.size() && i < currentChunk + CHUNK_SIZE; ++i) {
            auto filename2 = it->first;
            auto counter = it->second;
            mapReduce->Reduce(filename2, counter);
            ++it;
        }
    }

}

void * shuffle(void * p) {
    timespec ts;
    pthread_mutex_t tempMut = PTHREAD_MUTEX_INITIALIZER;
    while(1) {

        // search for non empty list
        for(auto it = mapContainers.begin(); it != mapContainers.end(); ++it) {

            pthread_t key = it->first;
            auto list = it->second;

            if(!list->empty()) {
                // shuffle list
                pthread_mutex_lock(&mapContainersMut[key]);
                for(auto it2 = list->begin(); it2 != list->end(); ++it2) {
                    auto k2 = (*it2)->first;
                    auto v2 = (*it2)->second;
                    if(postShuffleContainer.count(k2)){
                        postShuffleContainer[k2].push_back(v2);
                    } else {
                        postShuffleContainer[k2] = {v2};
                    }
                }
                list->clear();
                pthread_mutex_unlock(&mapContainersMut[key]);
            }
        }
        if(activeThreads == 0) {
            //break;
            activeThreads--;
        } else if(activeThreads == -1) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 10000; // wait only 0.01 sec at most
        pthread_cond_timedwait(&cond, &tempMut, &ts);
    }
    pthread_exit(NULL);
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
    pthread_t * threadsArray = new pthread_t[multiThreadLevel]();

    activeThreads = multiThreadLevel;
    // Create threads
    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&(threadsArray[i]), NULL, mapExec, &mapReduce);
    }

    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threadsArray[i], NULL);
    }

    activeThreads = 0;
    pthread_join(shuffleThread, NULL);

    // destroy cond
    pthread_cond_destroy(&cond);

    /**********/
    /* REDUCE */
    /**********/
    // Create threads
    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&(threadsArray[i]), NULL, reduceExec, &mapReduce);
    }
    // join to main thread
    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threadsArray[i], NULL);
    }
    delete[] threadsArray;

    /************/
    /* FINALIZE */
    /************/

    for(auto it = reduceContainers.begin(); it != reduceContainers.end(); ++it) {
        auto list = it->second;
        if(!list->empty()) {
            // shuffle list
            for(auto it2 = list->begin(); it2 != list->end(); ++it2) {
                auto k3 = (*it2)->first;
                auto v3 = (*it2)->second;
                outItemsList.push_back({k3, v3});
            }
            list->clear();
        }
    }

    //////////
    std::cout << postShuffleContainer.size() << std::endl;

    outItemsList.sort(cmpK3Base);

    return outItemsList;
}

void Emit2 (k2Base* k2, v2Base* v2) {
    auto p = new std::pair<k2Base*, v2Base*>(k2, v2);
    mapBufferedContainers[pthread_self()]->push_back(p);
}

void Emit3 (k3Base* k3, v3Base* v3) {
    auto p = new std::pair<k3Base*, v3Base*>(k3, v3);
    reduceContainers[pthread_self()]->push_back(p);
}