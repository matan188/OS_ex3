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

pthread_mutex_t debugMut = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t listIndexMut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mapInitMut = PTHREAD_MUTEX_INITIALIZER;
int listIndex = 0;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>*>*> midContainers;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>*>*> midBufferedContainers;
std::map<pthread_t, pthread_mutex_t> midContainersMut;

int activeThreads;

pthread_mutex_t tMut = PTHREAD_MUTEX_INITIALIZER;
int tIndex = 0;
std::vector<int> th = {0,0,0,0,0};

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

    pthread_mutex_lock(&debugMut);
    //std::cout << "LOCK " << selfId << std::endl;
    pthread_mutex_unlock(&debugMut);

    std::cout << "LOCK |" << " self: " << selfId << " mutex: " << &midContainersMut[selfId] << std::endl;
    pthread_mutex_lock(&midContainersMut[selfId]);
    auto m = midBufferedContainers[selfId];
    for(auto it = m->begin(); it != m->end(); ++it) {
        midContainers[selfId]->push_back(*it);

        /*
        pthread_mutex_lock(&debugMut);
        std::cout << "clearBuffer" << std::endl;
        pthread_mutex_unlock(&debugMut);
        */
        std::cout << "1" << std::endl;

        //std::cout << "<buf>" << static_cast<FileName1*>((*it)->first)->getVal() << std::endl;
        //std::cout << "<mid>" << midContainers[selfId]->size() << std::endl;
    }
    auto v = midContainers[selfId];
    m->clear();
    pthread_mutex_unlock(&midContainersMut[selfId]);


    pthread_mutex_lock(&debugMut);
    std::cout << "UNLOCK |" << " self: " << selfId << " mutex: " << &midContainersMut[selfId] << std::endl;
    pthread_mutex_unlock(&debugMut);

    pthread_cond_signal(&cond);
}

void * mapExec(void * p) {

    pthread_mutex_lock(&debugMut);
    std::cout << "MAPEXEC START "  << pthread_self() << std::endl;
    pthread_mutex_unlock(&debugMut);

    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    std::vector<std::pair<k2Base*, v2Base*>*> * container = new std::vector<std::pair<k2Base*, v2Base*>*>();
    std::vector<std::pair<k2Base*, v2Base*>*> * bufferedContainer = new std::vector<std::pair<k2Base*, v2Base*>*>();

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

            pthread_mutex_lock(&tMut);
            th.at(tIndex) = 1;
            ++tIndex;
            pthread_mutex_unlock(&tMut);

            pthread_exit(NULL);
        }
        currentChunk = listIndex;
        listIndex += CHUNK_SIZE;
        //std::cout << "Thread " << pthread_self() << " listIndex: " << listIndex << std::endl;
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

    pthread_mutex_lock(&debugMut);
    std::cout << "SHUFFLE START" << std::endl;
    pthread_mutex_unlock(&debugMut);

    timespec ts;
    pthread_mutex_t tempMut = PTHREAD_MUTEX_INITIALIZER;
    while(1) {

        // search for non empty list
        for(auto it = midContainers.begin(); it != midContainers.end(); ++it) {

            pthread_t key = it->first;
            auto list = it->second;

            if(!list->empty()) {
                // shuffle list
                //std::cout << "error?: " << pthread_mutex_trylock(&mut) << std::endl;
                if(pthread_mutex_trylock(&midContainersMut[key]) != 0) {
                    std::cout << "*************** self of criminal: " << key << std::endl;
                    int u = 0;
                    while(pthread_mutex_trylock(&midContainersMut[key]) != 0 ) {
                        if(u % 100000000 == 0) {
                            std::cout << "self: " << key << " mutex: " << &(midContainersMut[key]) << std::endl;
                        }
                        u++;
                    }
                }

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
                pthread_mutex_unlock(&midContainersMut[key]);
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
        //std::cout << i << std::endl;
        // create ExecMap thread
        pthread_create(&(threadsArray[i]), NULL, mapExec, &mapReduce);
        std::cout << " $ " << std::endl;
        //threads.push_back(&p);
    }
    

    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threadsArray[i], NULL);
    }


    pthread_mutex_lock(&tMut);
    for(int i = 0; i < th.size(); ++i) {
        th.at(i) = 2;
    }
    pthread_mutex_unlock(&tMut);

    pthread_mutex_lock(&debugMut);
    std::cout << "JOIN END" << std::endl;
    pthread_mutex_unlock(&debugMut);

    activeThreads = 0;


    pthread_join(shuffleThread, NULL);

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
    auto p = new std::pair<k2Base*, v2Base*>(k2, v2);
    midBufferedContainers[pthread_self()]->push_back(p);
    //std::cout << static_cast<FileName1*>(k2)->getVal() << std::endl;
}

void Emit3 (k3Base*, v3Base*) {
    
}