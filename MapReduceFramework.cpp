#include "MapReduceFramework.h"
#include "Search.h"
#include <pthread.h>
#include <iostream>
#include <stdio.h>
#include <map>
#include <vector>
#include <unistd.h>

#define CHUNK_SIZE 1

IN_ITEMS_LIST inContainer;
pthread_mutex_t listIndexMut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mapInitMut = PTHREAD_MUTEX_INITIALIZER;
int listIndex = 0;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>>*> midContainers;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>>*> midBufferedContainers;
std::map<pthread_t, pthread_mutex_t> midContainersMut;

int activeThreads;

struct cmpK2Base {
    bool operator()(const k2Base * a, const k2Base * b) const {
        return  *a < *b;
    }
};

std::map<k2Base *, V2_LIST, cmpK2Base> postShuffleContainer;

pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
//int rc = pthread_cond_init(&cond, NULL);

void clearBuffer() {
    auto selfId = pthread_self();
    pthread_mutex_lock(&midContainersMut[selfId]);
    auto m = midBufferedContainers[selfId];
    for(auto it = m->begin(); it != m->end(); ++it) {
        midContainers[selfId]->push_back(*it);

        //std::cout << "<clearBuffer>" << static_cast<FileName1*>(it->first)->getVal() << std::endl;
    }
    m->clear();
    pthread_mutex_unlock(&midContainersMut[selfId]);
    pthread_cond_signal(&cond);
}

void * mapExec(void * p) {
    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    std::vector<std::pair<k2Base*, v2Base*>> * container = new std::vector<std::pair<k2Base*, v2Base*>>();
    std::vector<std::pair<k2Base*, v2Base*>> * bufferedContainer = new std::vector<std::pair<k2Base*, v2Base*>>();

    pthread_mutex_lock(&mapInitMut);
    midContainers[pthread_self()] = container;
    midBufferedContainers[pthread_self()] = bufferedContainer;
    midContainersMut[pthread_self()] = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_unlock(&mapInitMut);
    
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
        if(midBufferedContainers[pthread_self()]->size() >= CHUNK_SIZE) {
            clearBuffer();
        }
    }
}

void * shuffle(void * p) {
    timespec ts;
    pthread_mutex_t tempMut = PTHREAD_MUTEX_INITIALIZER;
    while(1) {
        // search for non empty list
        for(auto it = midContainers.begin(); it != midContainers.end(); ++it) {
            pthread_t key = it->first;
            auto list = it->second;


            for (auto it2 = list->begin(); it2 != list->end(); ++it2) {
                std::cout << static_cast<FileName1*>(it2->first)->getVal() << std::endl;
            }

            if(!list->empty()) {
                // shuffle list
                auto mut = midContainersMut[key];
                pthread_mutex_lock(&mut);
                for(auto it2 = list->begin(); it2 != list->end(); ++it2) {
                    auto k2 = it2->first;
                    auto v2 = it2->second;
                    if(postShuffleContainer.count(k2)){
                        postShuffleContainer[k2].push_back(v2);
                    } else {
                        postShuffleContainer[k2] = {v2};
                    }
                }
                list->clear();
                pthread_mutex_unlock(&mut);
            }
        }
        if(activeThreads <= 0) {
            //break;
            activeThreads--;
        }
        if(activeThreads == -2) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 10000;
        pthread_cond_timedwait(&cond, &tempMut, &ts);
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

    activeThreads = multiThreadLevel;
    // Create threads
    for(int i = 0; i < multiThreadLevel; ++i) {
        //std::cout << i << std::endl;
        // create ExecMap thread
        pthread_t p;
        pthread_create(&p, NULL, mapExec, &mapReduce);
        std::cout << " $ " << std::endl;
        threads.push_back(&p);
    }
    
    // Join all threads
    void * x;
    for(pthread_t * p : threads) {
        pthread_join(*p, &x);
    }
    activeThreads = 0;
    pthread_join(shuffleThread, &x);
    
    // destroy cond
    pthread_cond_destroy(&cond);



    sleep(1);
    std::cout << postShuffleContainer.size() << std::endl;

    return outItemsList;
}

void Emit2 (k2Base* k2, v2Base* v2) {
    /*
    auto v = midBufferedContainers[pthread_self()];
    v->push_back({k2, v2});
     */
    midBufferedContainers[pthread_self()]->push_back({k2, v2});
    //std::cout << static_cast<FileName1*>(k2)->getVal() << std::endl;
}

void Emit3 (k3Base*, v3Base*) {
    
}